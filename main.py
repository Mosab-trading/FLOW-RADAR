import asyncio,json,os,time,csv,urllib.request
from collections import defaultdict,deque
from pathlib import Path
import websockets
import aiohttp

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_ENABLED = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)

SYMBOLS=[x.strip().upper() for x in os.getenv("SYMBOLS","BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,APTUSDT,ATOMUSDT,ARBUSDT,ALGOUSDT,OPUSDT,SUIUSDT,SEIUSDT,NEARUSDT,INJUSDT,STXUSDT,TIAUSDT").split(",")]
ALT_CORE=[x.strip().upper() for x in os.getenv("ALT_CORE","APTUSDT,ATOMUSDT,ARBUSDT,ALGOUSDT,OPUSDT,SUIUSDT,SEIUSDT,NEARUSDT,INJUSDT,STXUSDT,TIAUSDT").split(",") if x.strip()]
ALT_MIN_AVG_USD=float(os.getenv("ALT_MIN_AVG_USD","1000")); ALT_MIN_AVG_FEEDS=float(os.getenv("ALT_MIN_AVG_FEEDS","2"))
W=int(os.getenv("FLOW_WINDOW","5")); MIN=float(os.getenv("EVENT_MIN_USD","50000")); IMB=float(os.getenv("EVENT_IMBALANCE","70")); COOL=int(os.getenv("EVENT_COOLDOWN","10"))
D=Path("data");D.mkdir(exist_ok=True); RAW=D/"trades.jsonl"; EVENTS=D/"events.csv"; SCORES=D/"flow_scores.csv"; CALIB=D/"flow_score_regime_calibration.csv"
buf=defaultdict(lambda:deque(maxlen=500000)); pending=[]; last={}; multipliers={}
# V3.5 diagnostic confirmation layer; original EVENT/RESULT logic remains unchanged.
flow_history=defaultdict(lambda:deque(maxlen=120))
setup_last={}
score_pending=[]; score_last={}

def add(ex,s,p,base_qty,side,ts):
 usd=p*base_qty
 t={"ts":ts,"ex":ex,"s":s,"p":p,"qty":base_qty,"usd":usd,"side":side};buf[s].append(t)
 with RAW.open("a") as f:f.write(json.dumps(t,separators=(",",":"))+"\n")

def http_json(url):
 req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0 FlowRadar/3.9","Accept":"application/json"})
 with urllib.request.urlopen(req,timeout=15) as r:return json.loads(r.read())

async def load_meta():
 # V3.9: OKX REST may return HTTP 403 from cloud IPs. Try REST first, then the public
 # WebSocket instruments snapshot (same OKX public WS already used successfully for trades).
 targets={s.replace("USDT","-USDT-SWAP"):s for s in SYMBOLS}
 okx_rows={}
 try:
  x=await asyncio.to_thread(http_json,"https://www.okx.com/api/v5/public/instruments?instType=SWAP")
  for z in x.get("data",[]):
   if z.get("instId") in targets: okx_rows[z["instId"]]=z
  print("OKX META REST",len(okx_rows),"/",len(targets))
 except Exception as e:
  print("OKX META REST FAILED",repr(e),"-> WS fallback")

 if len(okx_rows)<len(targets):
  try:
   u="wss://ws.okx.com:8443/ws/v5/public"
   async with websockets.connect(u,ping_interval=20,ping_timeout=20,max_queue=30000) as w:
    await w.send(json.dumps({"op":"subscribe","args":[{"channel":"instruments","instType":"SWAP"}]}))
    deadline=time.time()+12
    while time.time()<deadline and len(okx_rows)<len(targets):
     try: r=await asyncio.wait_for(w.recv(),timeout=max(0.2,deadline-time.time()))
     except asyncio.TimeoutError: break
     x=json.loads(r)
     if x.get("event")=="error":
      print("OKX META WS ERROR",json.dumps(x,separators=(",",":"))[:500]);break
     for z in x.get("data",[]):
      if z.get("instId") in targets: okx_rows[z["instId"]]=z
   print("OKX META WS",len(okx_rows),"/",len(targets))
  except Exception as e: print("OKX META WS FAILED",repr(e))

 okx_ok=0; okx_skip=[]
 for inst,sym in targets.items():
  try:
   z=okx_rows.get(inst)
   if not z: raise ValueError("instrument metadata not returned")
   cv=float(z.get("ctVal") or 0); cm=float(z.get("ctMult") or 1)
   ccy=(z.get("ctValCcy") or "").upper(); base=sym[:-4]
   if cv<=0: raise ValueError("missing ctVal")
   if ccy and ccy not in (base,"USD"):
    raise ValueError(f"unexpected ctValCcy={ccy}")
   multipliers[("OKX",inst)]=cv*cm; okx_ok+=1
   print("OKX META OK",sym,inst,"ctVal=",cv,"ctMult=",cm,"mult=",cv*cm,"ccy=",ccy)
  except Exception as e:
   okx_skip.append(sym); print("OKX META SKIP",sym,repr(e))
 print("OKX CONTRACT META V3.9",okx_ok,"/",len(SYMBOLS),"loaded","skipped="+",".join(okx_skip) if okx_skip else "all-ok")

 # Safety fallback for the four original contracts only, preserving the original known values.
 fallback={"BTC-USDT-SWAP":0.01,"ETH-USDT-SWAP":0.1,"SOL-USDT-SWAP":1.0,"XRP-USDT-SWAP":100.0}
 for inst,m in fallback.items():
  if ("OKX",inst) not in multipliers:
   multipliers[("OKX",inst)]=m; print("OKX META FALLBACK",inst,"mult=",m)

 # Gate metadata unchanged.
 for s in SYMBOLS:
  try:
   c=s.replace("USDT","_USDT")
   x=await asyncio.to_thread(http_json,f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{c}")
   multipliers[("GATE",c)]=float(x["quanto_multiplier"])
  except Exception as e: print("GATE META SKIP",s,repr(e))

async def binance(s,spot):
 ex="BINANCE_SPOT" if spot else "BINANCE_FUTURES"
 u=(f"wss://stream.binance.com:9443/ws/{s.lower()}@aggTrade" if spot else f"wss://fstream.binance.com/market/ws/{s.lower()}@aggTrade")
 while 1:
  try:
   async with websockets.connect(u,ping_interval=20,ping_timeout=20,max_queue=30000) as w:
    print(ex,"CONNECTED",s); first=True
    async for r in w:
     x=json.loads(r)
     if first:print(ex,"DATA OK",s);first=False
     p=float(x["p"]);q=float(x["q"]);add(ex,s,p,q,"SELL" if x.get("m") else "BUY",x.get("T",x.get("E",time.time()*1000))/1000)
  except Exception as e:print(ex,"reconnect",s,repr(e));await asyncio.sleep(3)

async def bybit(s,spot):
 ex="BYBIT_SPOT" if spot else "BYBIT_FUTURES"; u="wss://stream.bybit.com/v5/public/"+("spot" if spot else "linear")
 while 1:
  try:
   async with websockets.connect(u,ping_interval=20,ping_timeout=20,max_queue=30000) as w:
    await w.send(json.dumps({"op":"subscribe","args":[f"publicTrade.{s}"]}));print(ex,"CONNECTED",s);first=True
    async for r in w:
     x=json.loads(r)
     for z in x.get("data",[]):
      if first:print(ex,"DATA OK",s);first=False
      add(ex,s,float(z["p"]),float(z["v"]),"BUY" if z["S"]=="Buy" else "SELL",int(z["T"])/1000)
  except Exception as e:print(ex,"reconnect",s,repr(e));await asyncio.sleep(3)

async def okx(s,spot):
 ex="OKX_SPOT" if spot else "OKX_FUTURES"; inst=s.replace("USDT","-USDT")+("" if spot else "-SWAP")
 u="wss://ws.okx.com:8443/ws/v5/public"
 channel="trades"
 while 1:
  try:
   async with websockets.connect(u,ping_interval=20,ping_timeout=20,max_queue=30000) as w:
    arg={"channel":channel,"instId":inst}
    await w.send(json.dumps({"op":"subscribe","args":[arg]}))
    print(ex,"CONNECTED",s,"channel="+channel,"instId="+inst)
    first=True; diag=0
    async for r in w:
     x=json.loads(r)
     if x.get("event") in ("subscribe","error"):
      print(ex,"OKX RESPONSE",s,json.dumps(x,separators=(",",":"))[:1000])
      continue
     rows=x.get("data",[])
     if not rows: continue
     if diag<2:
      print(ex,"RAW DATA",s,json.dumps(rows[0],separators=(",",":"))[:1000]);diag+=1
     for z in rows:
      p=float(z["px"]); q=float(z["sz"])
      if not spot:
       m=multipliers.get(("OKX",inst))
       if not m:
        print(ex,"SKIP NO ctVal",s,inst,"raw_sz="+str(z.get("sz")))
        continue
       q*=m
      if first:print(ex,"DATA OK",s,"ctVal="+str(multipliers.get(("OKX",inst),"SPOT")));first=False
      add(ex,s,p,q,z["side"].upper(),int(z["ts"])/1000)
  except Exception as e:
   print(ex,"reconnect",s,repr(e));await asyncio.sleep(3)

async def gate(s,spot):
 ex="GATE_SPOT" if spot else "GATE_FUTURES"; c=s.replace("USDT","_USDT")
 u="wss://api.gateio.ws/ws/v4/" if spot else "wss://fx-ws.gateio.ws/v4/ws/usdt"
 ch="spot.trades" if spot else "futures.trades"
 while 1:
  try:
   async with websockets.connect(u,ping_interval=20,ping_timeout=20,max_queue=30000) as w:
    await w.send(json.dumps({"time":int(time.time()),"channel":ch,"event":"subscribe","payload":[c]}));print(ex,"CONNECTED",s);first=True
    async for r in w:
     x=json.loads(r)
     if x.get("event")!="update":continue
     rows=x.get("result",[]); rows=rows if isinstance(rows,list) else [rows]
     for z in rows:
      if first:print(ex,"DATA OK",s);first=False
      p=float(z["price"])
      if spot:q=float(z["amount"]);side=z["side"].upper();ts=float(z.get("create_time_ms",time.time()*1000))/1000
      else:
       size=float(z["size"]);m=multipliers.get(("GATE",c))
       if not m:continue
       q=abs(size)*m;side="BUY" if size>0 else "SELL";ts=float(z.get("create_time_ms",time.time()*1000))/1000
      add(ex,s,p,q,side,ts)
  except Exception as e:print(ex,"reconnect",s,repr(e));await asyncio.sleep(3)

def flow(s,seconds=W,ex=None):
 n=time.time();b=se=0.;c=0
 for t in reversed(buf[s]):
  if n-t["ts"]>seconds:break
  if ex and t["ex"]!=ex:continue
  c+=1
  if t["side"]=="BUY":b+=t["usd"]
  else:se+=t["usd"]
 tot=b+se;d=b-se;im=d/tot*100 if tot else 0
 return b,se,d,im,c

def group(s,names):
 vals=[flow(s,W,x) for x in names];b=sum(x[0] for x in vals);se=sum(x[1] for x in vals);c=sum(x[4] for x in vals);tot=b+se;d=b-se;im=d/tot*100 if tot else 0
 return b,se,d,im,c

def price(s):return buf[s][-1]["p"] if buf[s] else None

def detect(s):
 b,se,d,im,c=flow(s);tot=b+se;n=time.time();direction="BUY" if d>0 else "SELL"
 if tot<MIN or abs(im)<IMB or n-last.get((s,direction),0)<COOL:return
 p=price(s)
 if not p:return
 last[(s,direction)]=n;pending.append({"ts":n,"s":s,"dir":direction,"entry":p,"b":b,"sell":se,"d":d,"im":im,"r":{}})
 print(f"EVENT {s} {direction} entry={p} delta=${d:,.0f} imbalance={im:+.1f}%")

def outcomes():
 n=time.time()
 for e in pending[:]:
  for h in (5,30,60,300):
   if str(h) not in e["r"] and n>=e["ts"]+h and price(e["s"]):e["r"][str(h)]=(price(e["s"])/e["entry"]-1)*100
  if "300" in e["r"]:
   new=not EVENTS.exists();r=e["r"]
   with EVENTS.open("a",newline="") as f:
    w=csv.writer(f)
    if new:w.writerow(["time","symbol","direction","entry","buy_usd","sell_usd","delta","imbalance","ret5","ret30","ret60","ret300"])
    w.writerow([e["ts"],e["s"],e["dir"],e["entry"],e["b"],e["sell"],e["d"],e["im"],r["5"],r["30"],r["60"],r["300"]])
   print(f'RESULT {e["s"]} {e["dir"]} 5s={r["5"]:+.3f}% 30s={r["30"]:+.3f}% 60s={r["60"]:+.3f}% 300s={r["300"]:+.3f}%');pending.remove(e)

def remember_confirm(s,p,sd,fd,ad,ai,gbuy,gsell,vbuy,vsell):
 h=flow_history[s]; n=time.time()
 h.append({"ts":n,"p":p,"sd":sd,"fd":fd,"d":ad,"im":ai,
           "gbuy":gbuy,"gsell":gsell,"vbuy":vbuy,"vsell":vsell})
 while h and n-h[0]["ts"]>70:h.popleft()

def confirm(s,secs):
 n=time.time(); r=[x for x in flow_history[s] if n-x["ts"]<=secs]
 if len(r)<3:return f"FLOW CONFIRM {secs}s WARMING | samples={len(r)}"

 pos=sum(x["d"]>0 for x in r); neg=sum(x["d"]<0 for x in r)
 side="BUY" if pos>neg else "SELL" if neg>pos else "NEUTRAL"
 persist=max(pos,neg)/len(r)

 cum=sum(x["d"] for x in r)
 total_buy=sum(x["gbuy"] for x in r); total_sell=sum(x["gsell"] for x in r)
 total=total_buy+total_sell
 wimb=cum/total*100 if total else 0

 vbuy=sum(x["vbuy"] for x in r); vsell=sum(x["vsell"] for x in r)
 vtot=vbuy+vsell
 agreement=max(vbuy,vsell)/vtot if vtot else 0
 vote="BUY" if vbuy>vsell else "SELL" if vsell>vbuy else "NEUTRAL"

 spot=sum(x["sd"] for x in r); fut=sum(x["fd"] for x in r)
 aligned=(side=="BUY" and spot>0 and fut>0) or (side=="SELL" and spot<0 and fut<0)

 move=(r[-1]["p"]/r[0]["p"]-1)*100 if r[0]["p"] else 0
 priceok=(side=="BUY" and move>0) or (side=="SELL" and move<0)

 strong=side!="NEUTRAL" and persist>=.67 and abs(wimb)>=45 and agreement>=.67 and vote==side and aligned and priceok
 normal=side!="NEUTRAL" and persist>=.60 and abs(wimb)>=30 and agreement>=.60 and vote==side and priceok

 if strong: label="STRONG "+side
 elif normal: label=side
 elif side!="NEUTRAL" and persist>=.67 and abs(wimb)>=45 and vote==side and not priceok:
  label=side+" ABSORPTION"
 else: label="NEUTRAL"

 return (f"FLOW CONFIRM {secs}s {label} | persistence={persist*100:.0f}% "
         f"| cumDelta=${cum:,.0f} | weightedIMB={wimb:+.1f}% "
         f"| agreement={agreement*100:.0f}% | spotFut={'YES' if aligned else 'NO'} "
         f"| priceMove={move:+.3f}% | priceConfirm={'YES' if priceok else 'NO'}")


def alt_breadth(secs):
 n=time.time(); rows=[]
 for s in ALT_CORE:
  r=[x for x in flow_history[s] if n-x["ts"]<=secs]
  if len(r)<3 or not r[0]["p"]: continue
  cum=sum(x["d"] for x in r); tb=sum(x["gbuy"] for x in r); ts=sum(x["gsell"] for x in r); tot=tb+ts
  avg_usd=tot/len(r) if r else 0
  avg_feeds=sum(x["vbuy"]+x["vsell"] for x in r)/len(r) if r else 0
  # Exclude thin/incomplete symbols so a few tiny trades cannot distort breadth.
  if avg_usd<ALT_MIN_AVG_USD or avg_feeds<ALT_MIN_AVG_FEEDS: continue
  wimb=cum/tot*100 if tot else 0
  ret=(r[-1]["p"]/r[0]["p"]-1)*100
  side="BUY" if wimb>=15 else "SELL" if wimb<=-15 else "NEUTRAL"
  rows.append((s,side,ret,wimb))
 br=[x for x in flow_history["BTCUSDT"] if n-x["ts"]<=secs]
 btc_ret=(br[-1]["p"]/br[0]["p"]-1)*100 if len(br)>=3 and br[0]["p"] else 0.0
 if not rows:return f"ALT BREADTH {secs}s WARMING | valid=0/{len(ALT_CORE)}"
 buys=sum(x[1]=="BUY" for x in rows); sells=sum(x[1]=="SELL" for x in rows); neuts=len(rows)-buys-sells
 rets=sorted(x[2] for x in rows); median=rets[len(rets)//2] if len(rets)%2 else (rets[len(rets)//2-1]+rets[len(rets)//2])/2
 out=sum(x[2]>btc_ret for x in rows); pos=sum(x[2]>0 for x in rows); opp=sum((x[2]>0 and btc_ret<0) or (x[2]<0 and btc_ret>0) for x in rows)
 bp=buys/len(rows)*100; sp=sells/len(rows)*100
 regime="BROAD ALT BUYING" if bp>=65 and sp<=25 else "BROAD ALT SELLING" if sp>=65 and bp<=25 else "MIXED/ROTATION"
 rs=median-btc_ret
 return (f"ALT BREADTH {secs}s {regime} | valid={len(rows)}/{len(ALT_CORE)} "
         f"| BUY={buys}({bp:.0f}%) SELL={sells}({sp:.0f}%) NEUTRAL={neuts} "
         f"| altMedian={median:+.3f}% BTC={btc_ret:+.3f}% RS={rs:+.3f}% "
         f"| outperformBTC={out}/{len(rows)} positive={pos}/{len(rows)} oppositeBTC={opp}/{len(rows)}")



def window_metrics(s,secs):
 n=time.time(); r=[x for x in flow_history[s] if n-x["ts"]<=secs]
 if len(r)<3 or not r[0]["p"]: return None
 pos=sum(x["d"]>0 for x in r); neg=sum(x["d"]<0 for x in r)
 side="BUY" if pos>neg else "SELL" if neg>pos else "NEUTRAL"
 persist=max(pos,neg)/len(r)
 cum=sum(x["d"] for x in r); tb=sum(x["gbuy"] for x in r); ts=sum(x["gsell"] for x in r); tot=tb+ts
 wimb=cum/tot*100 if tot else 0
 vb=sum(x["vbuy"] for x in r); vs=sum(x["vsell"] for x in r); vt=vb+vs
 agreement=max(vb,vs)/vt if vt else 0
 vote="BUY" if vb>vs else "SELL" if vs>vb else "NEUTRAL"
 spot=sum(x["sd"] for x in r); fut=sum(x["fd"] for x in r)
 aligned=(side=="BUY" and spot>0 and fut>0) or (side=="SELL" and spot<0 and fut<0)
 move=(r[-1]["p"]/r[0]["p"]-1)*100
 priceok=(side=="BUY" and move>0) or (side=="SELL" and move<0)
 sign=1 if side=="BUY" else -1 if side=="SELL" else 0
 return {"side":side,"sign":sign,"persist":persist,"cum":cum,"wimb":wimb,"agreement":agreement,
         "vote":vote,"aligned":aligned,"move":move,"priceok":priceok,"spot":spot,"fut":fut}

def breadth_metrics(secs=60):
 n=time.time(); rows=[]
 for s in ALT_CORE:
  r=[x for x in flow_history[s] if n-x["ts"]<=secs]
  if len(r)<3 or not r[0]["p"]: continue
  cum=sum(x["d"] for x in r); tb=sum(x["gbuy"] for x in r); ts=sum(x["gsell"] for x in r); tot=tb+ts
  avg_usd=tot/len(r); avg_feeds=sum(x["vbuy"]+x["vsell"] for x in r)/len(r)
  if avg_usd<ALT_MIN_AVG_USD or avg_feeds<ALT_MIN_AVG_FEEDS: continue
  wimb=cum/tot*100 if tot else 0; ret=(r[-1]["p"]/r[0]["p"]-1)*100
  rows.append((s,1 if wimb>=15 else -1 if wimb<=-15 else 0,ret))
 br=[x for x in flow_history["BTCUSDT"] if n-x["ts"]<=secs]
 btc=(br[-1]["p"]/br[0]["p"]-1)*100 if len(br)>=3 and br[0]["p"] else 0
 if not rows:return {"valid":0,"net":0,"rs":0,"positive":0}
 rets=sorted(x[2] for x in rows); med=rets[len(rets)//2] if len(rets)%2 else (rets[len(rets)//2-1]+rets[len(rets)//2])/2
 return {"valid":len(rows),"net":sum(x[1] for x in rows)/len(rows),"rs":med-btc,"positive":sum(x[2]>0 for x in rows)/len(rows)}

def flow_score(s):
 """V4.0 report-only score. Range -100..+100; no order placement."""
 m15,m30,m60=window_metrics(s,15),window_metrics(s,30),window_metrics(s,60)
 if not all((m15,m30,m60)): return None
 # 35 pts: multi-window flow momentum. 60s gets the largest weight.
 flowpart=0
 for m,w in ((m15,7),(m30,12),(m60,16)):
  strength=min(1.0,abs(m["wimb"])/45.0)*min(1.0,m["persist"]/.67)
  flowpart += m["sign"]*w*strength
 # 20 pts: spot/futures + venue agreement, concentrated on 30/60s.
 conf=0
 for m,w in ((m30,8),(m60,12)):
  if m["sign"]:
   q=min(1.0,m["agreement"]/.67)*(1.0 if m["aligned"] else .35)
   conf += m["sign"]*w*q
 # 15 pts: price confirmation; opposite price action is treated as absorption/divergence.
 pricepart=0
 for m,w in ((m30,6),(m60,9)):
  if m["sign"]: pricepart += m["sign"]*w*(1 if m["priceok"] else -.45)
 # 15 pts: BTC regime modifier for alts only. BTC itself gets its own flow as the market context.
 btcpart=0
 if s!="BTCUSDT":
  b30,b60=window_metrics("BTCUSDT",30),window_metrics("BTCUSDT",60)
  if b30 and b60:
   btcpart=7*b30["sign"]*min(1,abs(b30["wimb"])/45)+8*b60["sign"]*min(1,abs(b60["wimb"])/45)
 # 15 pts: alt breadth/relative strength modifier for alts.
 breadthpart=0; bm=breadth_metrics(60)
 if s!="BTCUSDT" and bm["valid"]>=3:
  breadthpart=9*max(-1,min(1,bm["net"])) + 6*max(-1,min(1,bm["rs"]/.20))
 raw=flowpart+conf+pricepart+btcpart+breadthpart
 score=max(-100,min(100,round(raw)))
 if score>=65: regime="STRONG LONG"
 elif score>=35: regime="LONG BIAS"
 elif score<=-65: regime="STRONG SHORT"
 elif score<=-35: regime="SHORT BIAS"
 else: regime="NEUTRAL"
 return {"score":score,"regime":regime,"flow":round(flowpart,1),"confirm":round(conf,1),"price":round(pricepart,1),
         "btc":round(btcpart,1),"breadth":round(breadthpart,1),"bvalid":bm["valid"],"rs":bm["rs"],
         "m30":m30["side"],"m60":m60["side"]}

def market_regime():
 """Calibration-only regime; FLOW SCORE itself is unchanged."""
 b30,b60=window_metrics("BTCUSDT",30),window_metrics("BTCUSDT",60); bm=breadth_metrics(60)
 if not b30 or not b60:return {"name":"WARMING","btc30":0.0,"btc60":0.0,"net":bm["net"],"rs":bm["rs"]}
 bull=b30["move"]>0 and b60["move"]>0 and b60["sign"]>=0 and bm["valid"]>=3 and bm["net"]>=0 and bm["rs"]>=0
 bear=b30["move"]<0 and b60["move"]<0 and b60["sign"]<=0 and bm["valid"]>=3 and bm["net"]<=0 and bm["rs"]<=0
 return {"name":"BULL" if bull else "BEAR" if bear else "SIDEWAYS","btc30":b30["move"],"btc60":b60["move"],"net":bm["net"],"rs":bm["rs"]}

async def fetch_market_context():
    """Fetch BTC price, dominance, 24h market change from CoinGecko with Binance fallback. Returns None on both failures."""
    try:
        async with aiohttp.ClientSession() as session:
            url = "https://api.coingecko.com/api/v3/global"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return {
                        "btc_price": data.get("data", {}).get("bitcoin", {}).get("usd"),
                        "btc_dominance": data.get("data", {}).get("btc_market_cap_percentage"),
                        "market_24h_change": data.get("data", {}).get("market_cap_change_percentage_24h_usd"),
                        "source": "coingecko"
                    }
    except Exception as e:
        print(f"MARKET CONTEXT: CoinGecko failed: {repr(e)}")
    
    # Fallback to Binance
    try:
        async with aiohttp.ClientSession() as session:
            url = "https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return {
                        "btc_price": float(data.get("lastPrice")),
                        "btc_dominance": None,
                        "market_24h_change": float(data.get("priceChangePercent")),
                        "source": "binance"
                    }
    except Exception as e:
        print(f"MARKET CONTEXT: Binance fallback failed: {repr(e)}")
    
    return None

async def send_telegram(text):
    """Send text message to Telegram. Failures are logged and do not crash Flow Radar."""
    if not TELEGRAM_ENABLED:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                ok = resp.status == 200
                if ok:
                    print(f"TELEGRAM: message sent")
                else:
                    print(f"TELEGRAM: send failed, status={resp.status}")
                return ok
    except Exception as e:
        print(f"TELEGRAM: send error: {repr(e)}")
        return False

def classify_regime(btc30, btc60, breadth_net, breadth_rs, breadth_valid, score_btc):
    """Classify market regime conservatively. RED only when sustained downside + negative RS; GREEN only when sustained upside + positive RS."""
    # RED: sustained downside (30s + 60s negative), breadth selling, negative RS, sufficient data
    if btc30 < 0 and btc60 < 0 and breadth_net < -0.3 and breadth_rs < -1.0 and breadth_valid >= 4:
        return {"color": "RED", "reason": "sustained downside + selling breadth + negative RS"}
    
    # GREEN: sustained upside (30s + 60s positive), breadth buying, positive RS, sufficient data
    if btc30 > 0 and btc60 > 0 and breadth_net > 0.3 and breadth_rs > 1.0 and breadth_valid >= 4:
        return {"color": "GREEN", "reason": "sustained upside + buying breadth + positive RS"}
    
    # YELLOW: weak conviction or transition (low valid count, mixed signals, weak RS)
    if breadth_valid < 3 or (abs(breadth_net) < 0.2 and abs(breadth_rs) < 1.0):
        return {"color": "YELLOW", "reason": "insufficient data or weak signals"}
    
    # ORANGE: everything else (sideways, unclear)
    return {"color": "ORANGE", "reason": "sideways/unclear"}

telegram_regime_state = {"last_color": None, "last_change_time": 0, "last_hourly_time": 0, "startup_sent": False}

async def telegram_market_regime_reporter():
    """Report market regime changes to Telegram. Runs every 60s; sends only on change or hourly summary."""
    if not TELEGRAM_ENABLED:
        return
    
    now = time.time()
    state = telegram_regime_state
    
    # Check if enough time has passed to avoid spam (min 60s between checks)
    if now - state.get("last_change_time", 0) < 60:
        return
    
    try:
        # Gather metrics
        b30 = window_metrics("BTCUSDT", 30)
        b60 = window_metrics("BTCUSDT", 60)
        bm = breadth_metrics(60)
        mr = market_regime()
        fs = flow_score("BTCUSDT")
        ctx = await fetch_market_context()
        
        if not b30 or not b60 or not mr:
            return
        
        # Classify regime
        regime = classify_regime(
            btc30=b30["move"], btc60=b60["move"],
            breadth_net=bm["net"], breadth_rs=bm["rs"],
            breadth_valid=bm["valid"],
            score_btc=(fs["btc"] if fs else 0)
        )
        color = regime["color"]
        
        # Check if regime changed or hourly summary due
        regime_changed = color != state.get("last_color")
        hourly_due = now - state.get("last_hourly_time", 0) > 3600
        
        # Prepare message only if sending
        if regime_changed or hourly_due:
            # Format breadth counts
            if bm["valid"] >= 3:
                net_count = int(bm["net"] * bm["valid"])
                breadth_str = f"net={net_count:+d} (out of {bm['valid']})"
            else:
                breadth_str = f"[warming, {bm['valid']} valid]"
            
            # BTC prices
            btc_p = price("BTCUSDT")
            dom_str = f"{ctx['btc_dominance']:.1f}%" if ctx and ctx.get("btc_dominance") else "N/A"
            change_str = f"{ctx['market_24h_change']:+.1f}%" if ctx and ctx.get("market_24h_change") is not None else "N/A"
            
            # Construct message
            emoji = {"RED": "🔴", "YELLOW": "🟡", "ORANGE": "🟠", "GREEN": "🟢"}[color]
            prev_color = state.get("last_color", "NONE")
            
            msg = f"{emoji} <b>REGIME: {color}</b> (from {prev_color})\n"
            msg += f"📊 BTC ${btc_p:,.0f} | 30s: {b30['move']:+.2f}% | 60s: {b60['move']:+.2f}%\n"
            msg += f"📈 Breadth: {breadth_str} | RS: {bm['rs']:+.2f}%\n"
            msg += f"🌐 DOM: {dom_str} | Market 24h: {change_str}"
            
            if bm["valid"] < 5 or abs(bm["rs"]) < 1.0:
                msg += "\n⚠️ Direction is NOT CERTAIN (low sample or weak RS)"
            
            # Send and update state
            await send_telegram(msg)
            state["last_change_time"] = now
            
            if regime_changed:
                state["last_color"] = color
            if hourly_due:
                state["last_hourly_time"] = now
        
        # Startup message
        if not state.get("startup_sent"):
            startup_msg = "✅ Flow Radar Telegram reporter started. Market context: live."
            await send_telegram(startup_msg)
            state["startup_sent"] = True
    
    except Exception as e:
        print(f"TELEGRAM REPORTER ERROR: {repr(e)}")
        # Do NOT re-raise; failures must not crash Flow Radar

def calibration_baseline(regime,horizon):
 if not CALIB.exists():return None
 try:
  vals=[]
  with CALIB.open(newline="") as f:
   for z in csv.DictReader(f):
    if z.get("market_regime")==regime and z.get("ret"+str(horizon)) not in (None,""):vals.append(float(z["ret"+str(horizon)]))
  if not vals:return None
  return {"n":len(vals),"up":100*sum(v>0 for v in vals)/len(vals),"down":100*sum(v<0 for v in vals)/len(vals),"avg":sum(vals)/len(vals)}
 except Exception:return None

def record_score(s,fs):
 n=time.time()
 if n-score_last.get(s,0)<15:return
 p=price(s)
 if not p:return
 mr=market_regime(); score_last[s]=n
 score_pending.append({"ts":n,"s":s,"p":p,"score":fs["score"],"regime":fs["regime"],"flow":fs["flow"],"confirm":fs["confirm"],"pricepart":fs["price"],"btc":fs["btc"],"breadth":fs["breadth"],"rs":fs["rs"],"market_regime":mr["name"],"regime_btc30":mr["btc30"],"regime_btc60":mr["btc60"],"regime_breadth":mr["net"],"regime_rs":mr["rs"],"r":{},"mfe":{},"mae":{},"best":0.0,"worst":0.0})

def score_outcomes():
 n=time.time()
 for e in score_pending[:]:
  p=price(e["s"])
  if p:
   raw=(p/e["p"]-1)*100; direction=1 if e["score"]>0 else -1 if e["score"]<0 else 0
   if direction:
    e["best"]=max(e["best"],raw*direction); e["worst"]=max(e["worst"],-raw*direction)
  for h in (30,60,180,300,900):
   if str(h) not in e["r"] and n>=e["ts"]+h and p:
    e["r"][str(h)]=(p/e["p"]-1)*100; e["mfe"][str(h)]=max(0,e["best"]); e["mae"][str(h)]=max(0,e["worst"])
  if "900" in e["r"]:
   r=e["r"];m=e["mfe"];q=e["mae"]; newc=not CALIB.exists()
   with CALIB.open("a",newline="") as f:
    w=csv.writer(f)
    if newc:w.writerow(["time","symbol","entry_price","score","score_regime","market_regime","regime_btc30","regime_btc60","regime_breadth_net","regime_rs60","flow_component","confirm_component","price_component","btc_component","breadth_component","rs60","ret30","ret60","ret180","ret300","ret900","mfe30","mae30","mfe60","mae60","mfe180","mae180","mfe300","mae300","mfe900","mae900"])
    w.writerow([e["ts"],e["s"],e["p"],e["score"],e["regime"],e["market_regime"],e["regime_btc30"],e["regime_btc60"],e["regime_breadth"],e["regime_rs"],e["flow"],e["confirm"],e["pricepart"],e["btc"],e["breadth"],e["rs"],r["30"],r["60"],r["180"],r["300"],r["900"],m["30"],q["30"],m["60"],q["60"],m["180"],q["180"],m["300"],q["300"],m["900"],q["900"]])
   news=not SCORES.exists()
   with SCORES.open("a",newline="") as f:
    w=csv.writer(f)
    if news:w.writerow(["time","symbol","entry_price","score","regime","flow_component","confirm_component","price_component","btc_component","breadth_component","rs60","ret30","ret60","ret180","ret300","ret900"])
    w.writerow([e["ts"],e["s"],e["p"],e["score"],e["regime"],e["flow"],e["confirm"],e["pricepart"],e["btc"],e["breadth"],e["rs"],r["30"],r["60"],r["180"],r["300"],r["900"]])
   base=calibration_baseline(e["market_regime"],180)
   bt=(f"baseline3m(n={base['n']} up={base['up']:.1f}% down={base['down']:.1f}% avg={base['avg']:+.3f}%)" if base else "baseline3m=WARMING")
   print(f'SCORE RESULT {e["s"]} score={e["score"]:+d} market={e["market_regime"]} 30s={r["30"]:+.3f}% 60s={r["60"]:+.3f}% 3m={r["180"]:+.3f}% 5m={r["300"]:+.3f}% 15m={r["900"]:+.3f}% | MFE3m={m["180"]:+.3f}% MAE3m={q["180"]:+.3f}% | {bt}')
   score_pending.remove(e)

def fast_setup(s):
 """Report-only entry candidate. Does not place trades or change raw EVENT logic."""
 n=time.time(); r=[x for x in flow_history[s] if n-x["ts"]<=15]
 if len(r)<3:return None

 pos=sum(x["d"]>0 for x in r); neg=sum(x["d"]<0 for x in r)
 side="BUY" if pos>neg else "SELL" if neg>pos else "NEUTRAL"
 if side=="NEUTRAL":return None

 persist=max(pos,neg)/len(r)
 cum=sum(x["d"] for x in r)
 tb=sum(x["gbuy"] for x in r); ts=sum(x["gsell"] for x in r); total=tb+ts
 wimb=cum/total*100 if total else 0

 vb=sum(x["vbuy"] for x in r); vs=sum(x["vsell"] for x in r); vt=vb+vs
 agreement=max(vb,vs)/vt if vt else 0
 vote="BUY" if vb>vs else "SELL" if vs>vb else "NEUTRAL"

 spot=sum(x["sd"] for x in r); fut=sum(x["fd"] for x in r)
 aligned=(side=="BUY" and spot>0 and fut>0) or (side=="SELL" and spot<0 and fut<0)

 # Fast signal: persistence + strong normalized flow + broad feed agreement + spot/futures alignment.
 ok=(persist>=.67 and abs(wimb)>=45 and agreement>=.75 and vote==side and aligned)
 if not ok:return None

 # Avoid printing the same setup every 5 seconds.
 key=(s,side)
 if n-setup_last.get(key,0)<15:return None
 setup_last[key]=n

 p=price(s)
 return (f"TRADE SETUP {s} {side} price={p} | window=15s "
         f"| persistence={persist*100:.0f}% | cumDelta=${cum:,.0f} "
         f"| weightedIMB={wimb:+.1f}% | agreement={agreement*100:.0f}% "
         f"| spotFut=YES | REPORT-ONLY")

async def report():
 spot=["BINANCE_SPOT","BYBIT_SPOT","OKX_SPOT","GATE_SPOT"]; fut=["BINANCE_FUTURES","BYBIT_FUTURES","OKX_FUTURES","GATE_FUTURES"]
 while 1:
  await asyncio.sleep(5)
  # Run Telegram reporter every 60s
  if int(time.time()) % 60 == 0:
    await telegram_market_regime_reporter()
  print(f"\n=== FLOW RADAR V4.1 | 4 EXCHANGES | SPOT + FUTURES | {W}s | FLOW CONFIRM ===")
  for s in SYMBOLS:
   print(f"\n{s} price={price(s)}")
   votes=[]
   for ex in spot+fut:
    b,se,d,im,c=flow(s,W,ex);print(f" {ex:<17} B=${b:,.0f} S=${se:,.0f} D=${d:,.0f} IMB={im:+.1f}% n={c}")
    if b+se>=1000:votes.append("BUY" if d>0 else "SELL")
   sb,ss,sd,si,sc=group(s,spot);fb,fs,fd,fi,fc=group(s,fut);ab,ase,ad,ai,ac=flow(s)
   print(f" SPOT TOTAL        D=${sd:,.0f} IMB={si:+.1f}% B=${sb:,.0f} S=${ss:,.0f}")
   print(f" FUTURES TOTAL     D=${fd:,.0f} IMB={fi:+.1f}% B=${fb:,.0f} S=${fs:,.0f}")
   print(f" GLOBAL FLOW       D=${ad:,.0f} IMB={ai:+.1f}% B=${ab:,.0f} S=${ase:,.0f}")
   print(f" AGREEMENT         BUY={votes.count('BUY')}/{len(votes)} SELL={votes.count('SELL')}/{len(votes)} active feeds")
   remember_confirm(s,price(s),sd,fd,ad,ai,ab,ase,votes.count("BUY"),votes.count("SELL"))
   print(" "+confirm(s,15))
   print(" "+confirm(s,30))
   print(" "+confirm(s,60))
   fs=flow_score(s)
   if fs:
    print(f" FLOW SCORE        {fs['score']:+d}/100 | {fs['regime']} | flow={fs['flow']:+.1f} confirm={fs['confirm']:+.1f} price={fs['price']:+.1f} btc={fs['btc']:+.1f} breadth={fs['breadth']:+.1f} | RS60={fs['rs']:+.3f}% valid={fs['bvalid']} | 30s={fs['m30']} 60s={fs['m60']} | REPORT-ONLY")
    record_score(s,fs)
   setup=fast_setup(s)
   if setup: print(" "+setup)
   detect(s)
  print("\n "+alt_breadth(30))
  print(" "+alt_breadth(60))
  outcomes(); score_outcomes()

async def main():
 print("FLOW RADAR V4.1 STARTED | REGIME + BASELINE + MFE/MAE CALIBRATION | FLOW SCORE UNCHANGED | 15 SYMBOLS | ALT BREADTH | BINANCE + BYBIT + OKX + GATE | SPOT + FUTURES | READ-ONLY | FAST SETUP 15s + FLOW CONFIRM 15s/30s/60s")
 await load_meta()
 tasks=[report()]
 for s in SYMBOLS:
  tasks += [binance(s,1),binance(s,0),bybit(s,1),bybit(s,0),okx(s,1),okx(s,0),gate(s,1),gate(s,0)]
 await asyncio.gather(*tasks)
asyncio.run(main())
