import asyncio,json,os,time,csv,urllib.request
from collections import defaultdict,deque
from pathlib import Path
import websockets

SYMBOLS=[x.strip().upper() for x in os.getenv("SYMBOLS","BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,APTUSDT,ATOMUSDT,ARBUSDT,ALGOUSDT,OPUSDT,SUIUSDT,SEIUSDT,NEARUSDT,INJUSDT,STXUSDT,TIAUSDT").split(",")]
ALT_CORE=[x.strip().upper() for x in os.getenv("ALT_CORE","APTUSDT,ATOMUSDT,ARBUSDT,ALGOUSDT,OPUSDT,SUIUSDT,SEIUSDT,NEARUSDT,INJUSDT,STXUSDT,TIAUSDT").split(",") if x.strip()]
ALT_MIN_AVG_USD=float(os.getenv("ALT_MIN_AVG_USD","5000")); ALT_MIN_AVG_FEEDS=float(os.getenv("ALT_MIN_AVG_FEEDS","2"))
W=int(os.getenv("FLOW_WINDOW","5")); MIN=float(os.getenv("EVENT_MIN_USD","50000")); IMB=float(os.getenv("EVENT_IMBALANCE","70")); COOL=int(os.getenv("EVENT_COOLDOWN","10"))
D=Path("data");D.mkdir(exist_ok=True); RAW=D/"trades.jsonl"; EVENTS=D/"events.csv"
buf=defaultdict(lambda:deque(maxlen=500000)); pending=[]; last={}; multipliers={}
# V3.5 diagnostic confirmation layer; original EVENT/RESULT logic remains unchanged.
flow_history=defaultdict(lambda:deque(maxlen=120))
setup_last={}

def add(ex,s,p,base_qty,side,ts):
 usd=p*base_qty
 t={"ts":ts,"ex":ex,"s":s,"p":p,"qty":base_qty,"usd":usd,"side":side};buf[s].append(t)
 with RAW.open("a") as f:f.write(json.dumps(t,separators=(",",":"))+"\n")

def http_json(url):
 with urllib.request.urlopen(url,timeout=15) as r:return json.loads(r.read())

async def load_meta():
 # Load OKX SWAP contract values dynamically for every tracked symbol.
 # OKX trade sz is contract count, so ctVal converts it to base quantity.
 try:
  x=await asyncio.to_thread(http_json,"https://www.okx.com/api/v5/public/instruments?instType=SWAP")
  wanted={s.replace("USDT","-USDT-SWAP") for s in SYMBOLS}
  for z in x.get("data",[]):
   inst=z.get("instId")
   if inst in wanted and z.get("ctVal"):
    multipliers[("OKX",inst)]=float(z["ctVal"])*float(z.get("ctMult") or 1)
  print("OKX CONTRACT META API OK",len([k for k in multipliers if k[0]=="OKX"]),"/",len(wanted))
 except Exception as e:
  print("OKX META API ERROR",repr(e))
  # Safe fallback for the original majors only; unknown contracts are skipped rather than mis-sized.
  for inst,val in {"BTC-USDT-SWAP":0.01,"ETH-USDT-SWAP":0.1,"SOL-USDT-SWAP":1.0,"XRP-USDT-SWAP":100.0}.items():
   multipliers[("OKX",inst)]=val
 # Gate metadata remains unchanged.
 for s in SYMBOLS:
  try:
   c=s.replace("USDT","_USDT")
   x=await asyncio.to_thread(http_json,f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{c}")
   multipliers[("GATE",c)]=float(x["quanto_multiplier"])
  except Exception as e: print("GATE META ERROR",s,e)
 print("GATE CONTRACT META OK",len([k for k in multipliers if k[0]=="GATE"]))

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
  await asyncio.sleep(5);print(f"\n=== FLOW RADAR V3.7 | 4 EXCHANGES | SPOT + FUTURES | {W}s | FLOW CONFIRM ===")
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
   setup=fast_setup(s)
   if setup: print(" "+setup)
   detect(s)
  print("\n "+alt_breadth(30))
  print(" "+alt_breadth(60))
  outcomes()

async def main():
 print("FLOW RADAR V3.7 STARTED | 15 SYMBOLS | ALT BREADTH | BINANCE + BYBIT + OKX + GATE | SPOT + FUTURES | READ-ONLY | FAST SETUP 15s + FLOW CONFIRM 15s/30s/60s")
 await load_meta()
 tasks=[report()]
 for s in SYMBOLS:
  tasks += [binance(s,1),binance(s,0),bybit(s,1),bybit(s,0),okx(s,1),okx(s,0),gate(s,1),gate(s,0)]
 await asyncio.gather(*tasks)
asyncio.run(main())
