import asyncio,json,os,time,csv,urllib.request,urllib.parse,math
from collections import defaultdict,deque
from pathlib import Path
import websockets

SYMBOLS=[x.strip().upper() for x in os.getenv("SYMBOLS","BTCUSDT,ETHUSDT").split(",") if x.strip()]
ALT_CORE=[]
ALT_MIN_AVG_USD=float(os.getenv("ALT_MIN_AVG_USD","1000")); ALT_MIN_AVG_FEEDS=float(os.getenv("ALT_MIN_AVG_FEEDS","2"))
PREMOVE_ENABLED=os.getenv("PREMOVE_ENABLED","1").lower() not in ("0","false","no")
PREMOVE_TOP_N=int(os.getenv("PREMOVE_TOP_N","10")); PREMOVE_MIN_PERSIST=int(os.getenv("PREMOVE_MIN_PERSIST","3"))
PREMOVE_SCAN_SECONDS=int(os.getenv("PREMOVE_SCAN_SECONDS","15")); PREMOVE_MIN_SCORE=float(os.getenv("PREMOVE_MIN_SCORE","58"))
PREMOVE_STRONG_SCORE=float(os.getenv("PREMOVE_STRONG_SCORE","72")); PREMOVE_MIN_24H_QUOTE=float(os.getenv("PREMOVE_MIN_24H_QUOTE","5000000"))
PREMOVE_QUIET_5M=float(os.getenv("PREMOVE_QUIET_5M","1.25")); PREMOVE_MAX_15M=float(os.getenv("PREMOVE_MAX_15M","2.25")); PREMOVE_MAX_1H=float(os.getenv("PREMOVE_MAX_1H","4.50"))
PREMOVE_STATE=defaultdict(lambda:deque(maxlen=12)); PREMOVE_MARKET={}; PREMOVE_LAST_PRINT=0.0
MOMENTUM_VERBOSE=os.getenv("MOMENTUM_VERBOSE","0").lower() in ("1","true","yes")
VENUE_VERBOSE=os.getenv("VENUE_VERBOSE","0").lower() in ("1","true","yes")
VENUE_AVAILABLE=defaultdict(set)  # symbol -> available venue families
OKX_SPOT_SUPPORTED=set()
OKX_FUT_SUPPORTED=set()
OKX_DISCOVERY_READY=asyncio.Event()
TRADFI_BASES={x.strip().upper() for x in os.getenv("PREMOVE_TRADFI_BASES","AAPL,AMZN,GOOG,GOOGL,META,MSFT,NVDA,TSLA,COIN,MSTR,SPX,SP500,NDX,NASDAQ,DJI,DOW,XAU,XAG,GOLD,SILVER,WTI,BRENT").split(",") if x.strip()}
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


def is_crypto_perp(z):
 sym=str(z.get("symbol") or "").upper(); base=str(z.get("baseAsset") or "").upper()
 if not sym.endswith("USDT") or z.get("quoteAsset")!="USDT" or z.get("contractType")!="PERPETUAL" or z.get("status")!="TRADING": return False
 if base in TRADFI_BASES:return False
 return not any(k in (base+" "+sym) for k in ("STOCK","INDEX","GOLD","SILVER","NASDAQ","SP500"))

async def load_binance_universe():
 global SYMBOLS,ALT_CORE
 try:
  info=await asyncio.to_thread(http_json,"https://fapi.binance.com/fapi/v1/exchangeInfo")
  tick=await asyncio.to_thread(http_json,"https://fapi.binance.com/fapi/v1/ticker/24hr")
  qv={str(x.get("symbol","")).upper():float(x.get("quoteVolume") or 0) for x in tick}
  dyn=[x["symbol"].upper() for x in info.get("symbols",[]) if is_crypto_perp(x) and qv.get(x["symbol"].upper(),0)>=PREMOVE_MIN_24H_QUOTE]
  for x in ("BTCUSDT","ETHUSDT"):
   if x not in dyn:dyn.append(x)
  SYMBOLS=sorted(set(dyn),key=lambda x:(x not in ("BTCUSDT","ETHUSDT"),-qv.get(x,0)))
 except Exception as e: print("PREMOVE UNIVERSE ERROR",repr(e))
 ALT_CORE=[x for x in SYMBOLS if x not in ("BTCUSDT","ETHUSDT")]
 for x in SYMBOLS: VENUE_AVAILABLE[x].add("BINANCE")
 print(f"PREMOVE UNIVERSE | total={len(SYMBOLS)} altCandidates={len(ALT_CORE)}")

def futures_json(path):return http_json("https://fapi.binance.com"+path)

def premove_rest_sync(sym):
 out={"k":{},"oi":None,"funding":None,"ts":time.time()}
 try:
  a=futures_json("/fapi/v1/klines?symbol="+urllib.parse.quote(sym)+"&interval=1m&limit=61")
  c=[float(x[4]) for x in a]; last=c[-1]
  ret=lambda n:(last/c[-1-n]-1)*100
  out["k"]={"r5":ret(5),"r15":ret(15),"r60":ret(60)}
 except Exception:pass
 try:
  a=futures_json("/futures/data/openInterestHist?symbol="+urllib.parse.quote(sym)+"&period=5m&limit=4")
  v=[float(x.get("sumOpenInterestValue") or 0) for x in a]
  if len(v)>1 and v[0]>0:out["oi"]=(v[-1]/v[0]-1)*100
 except Exception:pass
 try:
  out["funding"]=float(futures_json("/fapi/v1/premiumIndex?symbol="+urllib.parse.quote(sym)).get("lastFundingRate") or 0)*100
 except Exception:pass
 return out

async def refresh_premove_market():
 sem=asyncio.Semaphore(10)
 async def one(x):
  async with sem:return x,await asyncio.to_thread(premove_rest_sync,x)
 while 1:
  try:PREMOVE_MARKET.update(dict(await asyncio.gather(*(one(x) for x in ALT_CORE))));print("PREMOVE REST REFRESH",len(PREMOVE_MARKET))
  except Exception as e:print("PREMOVE REST ERROR",repr(e))
  await asyncio.sleep(max(60,int(os.getenv("PREMOVE_REST_SECONDS","300"))))

def premove_candidate(sym):
 if sym in ("BTCUSDT","ETHUSDT"):return None
 a,b=window_metrics(sym,30),window_metrics(sym,60); btc=window_metrics("BTCUSDT",60); md=PREMOVE_MARKET.get(sym,{})
 k=md.get("k") or {}
 if not a or not b or not k:return None
 side=1 if a["wimb"]+b["wimb"]>=0 else -1; word="LONG" if side>0 else "SHORT"
 pumped=abs(k["r5"])>PREMOVE_QUIET_5M or abs(k["r15"])>PREMOVE_MAX_15M or abs(k["r60"])>PREMOVE_MAX_1H
 w30=side*a["wimb"]; w60=side*b["wimb"]
 flow=max(0,min(1,(.55*w30+.45*w60)/55)); improve=max(0,min(1,(w30-w60+20)/40))
 persist=max(0,min(1,(a["persist"]+b["persist"])/1.44)); agree=max(0,min(1,(a["agreement"]+b["agreement"])/1.5))
 align=1 if a["aligned"] and b["aligned"] else .35 if a["aligned"] or b["aligned"] else 0
 early=max(0,min(1,(side*(.6*a["move"]+.4*b["move"])+.03)/.25))
 quiet=max(0,1-min(1,abs(k["r5"])/max(.01,PREMOVE_QUIET_5M)))
 rs60=side*(b["move"]-(btc["move"] if btc else 0)); rs=max(0,min(1,(rs60+.05)/.35))
 oi=md.get("oi"); fr=md.get("funding"); ois=.5 if oi is None else max(0,min(1,(side*oi+.15)))
 fs=.5 if fr is None else (1 if side*fr<=.01 else max(0,1-(side*fr-.01)/.08))
 score=100*(.23*flow+.10*improve+.14*persist+.12*agree+.10*align+.08*early+.08*quiet+.07*rs+.05*ois+.03*fs)
 h=PREMOVE_STATE[sym];h.append((side,score)); same=sum(1 for d,q in h if d==side and q>=PREMOVE_MIN_SCORE)
 status="REJECT" if pumped else ("EARLY_"+word+"_WATCH" if same>=PREMOVE_MIN_PERSIST and score>=PREMOVE_STRONG_SCORE else "NOT_CONFIRMED" if score>=PREMOVE_MIN_SCORE else "REJECT")
 reasons=[]
 if pumped:reasons.append("recent-expansion")
 if quiet>=.65:reasons.append("quiet")
 if improve>=.6:reasons.append("flow-improving")
 if align==1:reasons.append("spot+futures")
 if rs>=.6:reasons.append("RS-vs-BTC")
 if oi is not None and side*oi>0:reasons.append("OI-confirm")
 if fr is not None and fs>=.7:reasons.append("funding-ok")
 vc,vset=venue_coverage(sym)
 return dict(symbol=sym,side=word,status=status,score=score,same=same,w30=a["wimb"],w60=b["wimb"],m30=a["move"],m60=b["move"],rs=rs60,oi=oi,fr=fr,k=k,venues=vc,venue_names="/".join(sorted(vset)),reasons=",".join(reasons) or "weak-evidence")

def print_premove_top():
 global PREMOVE_LAST_PRINT
 if not PREMOVE_ENABLED or time.time()-PREMOVE_LAST_PRINT<PREMOVE_SCAN_SECONDS:return
 PREMOVE_LAST_PRINT=time.time(); rows=[q for x in ALT_CORE if (q:=premove_candidate(x))]
 rows.sort(key=lambda q:(q["status"].startswith("EARLY_"),q["score"]),reverse=True)
 print(f"\n=== PREMOVE TOP {PREMOVE_TOP_N} | SCANNER-ONLY | NO ORDER ROUTING ===")
 for i,q in enumerate(rows[:PREMOVE_TOP_N],1):
  oi="N/A" if q["oi"] is None else f"{q['oi']:+.2f}%"; fr="N/A" if q["fr"] is None else f"{q['fr']:+.4f}%"
  print(f" PREMOVE #{i:02d} {q['symbol']} {q['status']} {q['side']} score={q['score']:.1f} VENUES={q['venues']}/4[{q['venue_names']}] persistCycles={q['same']}/{PREMOVE_MIN_PERSIST} | wIMB30={q['w30']:+.1f}% wIMB60={q['w60']:+.1f}% | px30={q['m30']:+.3f}% px60={q['m60']:+.3f}% | 5m={q['k']['r5']:+.3f}% 15m={q['k']['r15']:+.3f}% 1h={q['k']['r60']:+.3f}% | RS60={q['rs']:+.3f}% OI={oi} funding={fr} | {q['reasons']}")


async def discover_bybit():
 """Discover Bybit linear USDT symbols without blocking PREMOVE startup."""
 try:
  cursor=""
  found=set()
  for _ in range(10):
   url="https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"
   if cursor:url+="&cursor="+urllib.parse.quote(cursor)
   x=await asyncio.to_thread(http_json,url)
   result=x.get("result",{}) or {}
   for z in result.get("list",[]) or []:
    sym=str(z.get("symbol") or "").upper()
    if sym in SYMBOLS and str(z.get("status") or "").lower()=="trading": found.add(sym)
   cursor=result.get("nextPageCursor") or ""
   if not cursor:break
  for sym in found:VENUE_AVAILABLE[sym].add("BYBIT")
  print("BYBIT DISCOVERY",len(found),"/",len(SYMBOLS))
 except Exception as e:print("BYBIT DISCOVERY FAILED",repr(e))

async def venue_metadata_background():
 """Load optional exchange metadata in background; never blocks PREMOVE/Binance startup."""
 await asyncio.gather(load_meta(),discover_bybit(),return_exceptions=True)
 print("VENUE DISCOVERY READY | coverage known for",len(VENUE_AVAILABLE),"symbols")

def venue_coverage(sym):
 # Count venue families with either discovered metadata or recent actual trade data.
 fam=set(VENUE_AVAILABLE.get(sym,set()))
 for t in list(buf[sym])[-5000:]:
  ex=str(t.get("ex",""))
  if ex.startswith("BINANCE"):fam.add("BINANCE")
  elif ex.startswith("BYBIT"):fam.add("BYBIT")
  elif ex.startswith("OKX"):fam.add("OKX")
  elif ex.startswith("GATE"):fam.add("GATE")
 return len(fam),fam

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

 # Discover OKX SPOT separately. A futures contract existing on OKX does not mean
 # the corresponding spot market exists. This prevents unsupported subscriptions/reconnect storms.
 try:
  sx=await asyncio.to_thread(http_json,"https://www.okx.com/api/v5/public/instruments?instType=SPOT")
  wanted={s.replace("USDT","-USDT"):s for s in SYMBOLS}
  for z in sx.get("data",[]):
   inst=str(z.get("instId") or "")
   state=str(z.get("state") or "live").lower()
   if inst in wanted and state in ("live","trading"):
    OKX_SPOT_SUPPORTED.add(wanted[inst])
  print("OKX SPOT DISCOVERY",len(OKX_SPOT_SUPPORTED),"/",len(SYMBOLS))
 except Exception as e:
  print("OKX SPOT DISCOVERY FAILED",repr(e))

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
   multipliers[("OKX",inst)]=cv*cm; OKX_FUT_SUPPORTED.add(sym); VENUE_AVAILABLE[sym].add("OKX"); okx_ok+=1
   VENUE_VERBOSE and print("OKX META OK",sym,inst,"ctVal=",cv,"ctMult=",cm,"mult=",cv*cm,"ccy=",ccy)
  except Exception as e:
   okx_skip.append(sym); VENUE_VERBOSE and print("OKX META SKIP",sym,repr(e))
 print("OKX CONTRACT META V3.9",okx_ok,"/",len(SYMBOLS),"loaded","skipped="+",".join(okx_skip) if okx_skip else "all-ok")

 # Safety fallback for the four original contracts only, preserving the original known values.
 fallback={"BTC-USDT-SWAP":0.01,"ETH-USDT-SWAP":0.1,"SOL-USDT-SWAP":1.0,"XRP-USDT-SWAP":100.0}
 for inst,m in fallback.items():
  if ("OKX",inst) not in multipliers:
   multipliers[("OKX",inst)]=m; VENUE_VERBOSE and print("OKX META FALLBACK",inst,"mult=",m)
  sym=inst.replace("-USDT-SWAP","USDT")
  if sym in SYMBOLS: OKX_FUT_SUPPORTED.add(sym); VENUE_AVAILABLE[sym].add("OKX")

 # Release OKX collectors now; Gate metadata can continue independently afterwards.
 OKX_DISCOVERY_READY.set()
 print("OKX SUPPORTED | spot=",len(OKX_SPOT_SUPPORTED),"futures=",len(OKX_FUT_SUPPORTED))

 # Gate metadata unchanged.
 for s in SYMBOLS:
  try:
   c=s.replace("USDT","_USDT")
   x=await asyncio.to_thread(http_json,f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{c}")
   multipliers[("GATE",c)]=float(x["quanto_multiplier"]); VENUE_AVAILABLE[s].add("GATE")
  except Exception as e: VENUE_VERBOSE and print("GATE META SKIP",s,repr(e))

async def binance(s,spot):
 ex="BINANCE_SPOT" if spot else "BINANCE_FUTURES"
 u=(f"wss://stream.binance.com:9443/ws/{s.lower()}@aggTrade" if spot else f"wss://fstream.binance.com/market/ws/{s.lower()}@aggTrade")
 while 1:
  try:
   async with websockets.connect(u,ping_interval=20,ping_timeout=20,max_queue=30000) as w:
    VENUE_VERBOSE and print(ex,"CONNECTED",s); first=True
    async for r in w:
     x=json.loads(r)
     if first: VENUE_VERBOSE and print(ex,"DATA OK",s); first=False
     p=float(x["p"]);q=float(x["q"]);add(ex,s,p,q,"SELL" if x.get("m") else "BUY",x.get("T",x.get("E",time.time()*1000))/1000)
  except Exception as e: VENUE_VERBOSE and print(ex,"reconnect",s,repr(e)); await asyncio.sleep(3)

async def bybit(s,spot):
 ex="BYBIT_SPOT" if spot else "BYBIT_FUTURES"; u="wss://stream.bybit.com/v5/public/"+("spot" if spot else "linear")
 while 1:
  try:
   async with websockets.connect(u,ping_interval=20,ping_timeout=20,max_queue=30000) as w:
    await w.send(json.dumps({"op":"subscribe","args":[f"publicTrade.{s}"]})); VENUE_VERBOSE and print(ex,"CONNECTED",s); first=True
    async for r in w:
     x=json.loads(r)
     for z in x.get("data",[]):
      if first: VENUE_VERBOSE and print(ex,"DATA OK",s); first=False
      add(ex,s,float(z["p"]),float(z["v"]),"BUY" if z["S"]=="Buy" else "SELL",int(z["T"])/1000)
  except Exception as e: VENUE_VERBOSE and print(ex,"reconnect",s,repr(e)); await asyncio.sleep(3)

async def okx(s,spot):
 ex="OKX_SPOT" if spot else "OKX_FUTURES"; inst=s.replace("USDT","-USDT")+("" if spot else "-SWAP")
 # Wait for one metadata pass, then permanently skip unsupported OKX markets.
 # This avoids endless 30-second no-data reconnect loops without affecting PREMOVE scoring.
 await OKX_DISCOVERY_READY.wait()
 supported=OKX_SPOT_SUPPORTED if spot else OKX_FUT_SUPPORTED
 if s not in supported:
  VENUE_VERBOSE and print(ex,"UNSUPPORTED SKIP",s,inst)
  return
 u="wss://ws.okx.com:8443/ws/v5/public"
 channel="trades"
 while 1:
  try:
   async with websockets.connect(u,ping_interval=20,ping_timeout=20,max_queue=30000) as w:
    arg={"channel":channel,"instId":inst}
    await w.send(json.dumps({"op":"subscribe","args":[arg]}))
    VENUE_VERBOSE and print(ex,"CONNECTED",s,"channel="+channel,"instId="+inst)
    first=True; diag=0
    async for r in w:
     x=json.loads(r)
     if x.get("event") in ("subscribe","error"):
      VENUE_VERBOSE and print(ex,"OKX RESPONSE",s,json.dumps(x,separators=(",",":"))[:1000])
      continue
     rows=x.get("data",[])
     if not rows: continue
     if diag<2:
      VENUE_VERBOSE and print(ex,"RAW DATA",s,json.dumps(rows[0],separators=(",",":"))[:1000]);diag+=1
     for z in rows:
      p=float(z["px"]); q=float(z["sz"])
      if not spot:
       m=multipliers.get(("OKX",inst))
       if not m:
        VENUE_VERBOSE and print(ex,"SKIP NO ctVal",s,inst,"raw_sz="+str(z.get("sz")))
        continue
       q*=m
      if first: VENUE_VERBOSE and print(ex,"DATA OK",s,"ctVal="+str(multipliers.get(("OKX",inst),"SPOT"))); first=False
      add(ex,s,p,q,z["side"].upper(),int(z["ts"])/1000)
  except Exception as e:
   VENUE_VERBOSE and print(ex,"reconnect",s,repr(e));await asyncio.sleep(3)

async def gate(s,spot):
 ex="GATE_SPOT" if spot else "GATE_FUTURES"; c=s.replace("USDT","_USDT")
 u="wss://api.gateio.ws/ws/v4/" if spot else "wss://fx-ws.gateio.ws/v4/ws/usdt"
 ch="spot.trades" if spot else "futures.trades"
 while 1:
  try:
   async with websockets.connect(u,ping_interval=20,ping_timeout=20,max_queue=30000) as w:
    await w.send(json.dumps({"time":int(time.time()),"channel":ch,"event":"subscribe","payload":[c]})); VENUE_VERBOSE and print(ex,"CONNECTED",s); first=True
    async for r in w:
     x=json.loads(r)
     if x.get("event")!="update":continue
     rows=x.get("result",[]); rows=rows if isinstance(rows,list) else [rows]
     for z in rows:
      if first: VENUE_VERBOSE and print(ex,"DATA OK",s); first=False
      p=float(z["price"])
      if spot:q=float(z["amount"]);side=z["side"].upper();ts=float(z.get("create_time_ms",time.time()*1000))/1000
      else:
       size=float(z["size"]);m=multipliers.get(("GATE",c))
       if not m:continue
       q=abs(size)*m;side="BUY" if size>0 else "SELL";ts=float(z.get("create_time_ms",time.time()*1000))/1000
      add(ex,s,p,q,side,ts)
  except Exception as e: VENUE_VERBOSE and print(ex,"reconnect",s,repr(e)); await asyncio.sleep(3)

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
 MOMENTUM_VERBOSE and print(f"EVENT {s} {direction} entry={p} delta=${d:,.0f} imbalance={im:+.1f}%")

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
   MOMENTUM_VERBOSE and print(f'RESULT {e["s"]} {e["dir"]} 5s={r["5"]:+.3f}% 30s={r["30"]:+.3f}% 60s={r["60"]:+.3f}% 300s={r["300"]:+.3f}%');pending.remove(e)

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
   MOMENTUM_VERBOSE and print(f'SCORE RESULT {e["s"]} score={e["score"]:+d} market={e["market_regime"]} 30s={r["30"]:+.3f}% 60s={r["60"]:+.3f}% 3m={r["180"]:+.3f}% 5m={r["300"]:+.3f}% 15m={r["900"]:+.3f}% | MFE3m={m["180"]:+.3f}% MAE3m={q["180"]:+.3f}% | {bt}')
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

# --- Telegram market regime reporter (report-only; does not alter Flow Radar calculations) ---
TG_TOKEN=os.getenv("TELEGRAM_BOT_TOKEN","").strip()
TG_CHAT_ID=os.getenv("TELEGRAM_CHAT_ID","").strip()
TG_HOURLY=int(os.getenv("TELEGRAM_HOURLY_SECONDS","3600"))
TG_CHANGE_COOLDOWN=int(os.getenv("TELEGRAM_CHANGE_COOLDOWN","180"))
TG_MIN_VALID=int(os.getenv("TELEGRAM_MIN_VALID","5"))
reporter_state={"regime":None,"last_change":0.0,"last_hourly":0.0,"public":{},"public_ts":0.0}

def telegram_send_sync(text):
 try:
  if not TG_TOKEN or not TG_CHAT_ID:
   print("TELEGRAM REPORTER DISABLED | missing TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID"); return False
  data=urllib.parse.urlencode({"chat_id":TG_CHAT_ID,"text":text,"disable_web_page_preview":"true"}).encode()
  req=urllib.request.Request(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",data=data,headers={"User-Agent":"FlowRadar-Telegram/1.0"})
  with urllib.request.urlopen(req,timeout=10) as r:
   ok=200<=getattr(r,"status",200)<300
  print("TELEGRAM SENT" if ok else "TELEGRAM SEND FAILED"); return ok
 except Exception as e:
  print("TELEGRAM SEND ERROR",repr(e)); return False

async def telegram_send(text):
 return await asyncio.to_thread(telegram_send_sync,text)

def public_market_sync():
 try:
  # CoinGecko global endpoint: no key required. Public context is secondary and failure-safe.
  x=http_json("https://api.coingecko.com/api/v3/global").get("data",{})
  pct=x.get("market_cap_percentage",{}) or {}; ch=x.get("market_cap_change_percentage_24h_usd")
  return {"btc_dom":float(pct.get("btc",0) or 0),"market24":float(ch or 0)}
 except Exception as e:
  print("PUBLIC MARKET CONTEXT ERROR",repr(e)); return {}

async def public_market():
 n=time.time()
 if n-reporter_state["public_ts"]>300:
  reporter_state["public"]=await asyncio.to_thread(public_market_sync); reporter_state["public_ts"]=n
 return reporter_state["public"]

def reporter_snapshot():
 b30=breadth_metrics(30); b60=breadth_metrics(60)
 btc30=window_metrics("BTCUSDT",30); btc60=window_metrics("BTCUSDT",60); bfs=flow_score("BTCUSDT")
 if not btc30 or not btc60:
  return None
 valid=min(b30["valid"],b60["valid"]); meaningful=valid>=TG_MIN_VALID
 sell30=b30["net"]<=-.40 and b30["rs"]<0; sell60=b60["net"]<=-.40 and b60["rs"]<0
 buy30=b30["net"]>=.40 and b30["rs"]>0; buy60=b60["net"]>=.40 and b60["rs"]>0
 btc_weak=btc60["move"]<0 and (btc60["sign"]<0 or (bfs and bfs["score"]<=-35))
 btc_stable=btc60["move"]>=-.08 and not (bfs and bfs["score"]<=-65)
 if meaningful and sell30 and sell60 and (btc_weak or b60["rs"]<=-.15): regime="RED"
 elif (sell30 and sell60) or (b60["rs"]<-.10 and b60["net"]<0) or btc_weak: regime="ORANGE"
 elif meaningful and buy30 and buy60 and btc_stable and b60["positive"]>=.60: regime="GREEN"
 else: regime="YELLOW"
 return {"regime":regime,"b30":b30,"b60":b60,"btc30":btc30,"btc60":btc60,"btcfs":bfs,"valid":valid}

def regime_message(snap,pub,reason):
 icons={"GREEN":"🟢","YELLOW":"🟡","ORANGE":"🟠","RED":"🔴"}; r=snap["regime"]; b30=snap["b30"]; b60=snap["b60"]; bf=snap["btcfs"]
 btcscore=(f"{bf['score']:+d}/100 {bf['regime']}" if bf else "warming")
 dom=(f"{pub.get('btc_dom',0):.2f}%" if pub.get('btc_dom') else "N/A"); m24=(f"{pub.get('market24',0):+.2f}%" if pub.get('market24') is not None and pub else "N/A")
 return (f"{icons[r]} FLOW RADAR — {r}\n"
         f"Reason: {reason}\n"
         f"BTC: ${price('BTCUSDT') or 0:,.2f} | 30s {snap['btc30']['move']:+.3f}% | 60s {snap['btc60']['move']:+.3f}%\n"
         f"BTC Flow Score: {btcscore}\n"
         f"ALT 30s: net={b30['net']:+.2f} | RS={b30['rs']:+.3f}% | valid={b30['valid']}/{len(ALT_CORE)}\n"
         f"ALT 60s: net={b60['net']:+.2f} | RS={b60['rs']:+.3f}% | valid={b60['valid']}/{len(ALT_CORE)} | positive={b60['positive']*100:.0f}%\n"
         f"BTC Dominance: {dom} | Total market 24h: {m24}\n"
         f"REPORT-ONLY. Short-term flow can reverse; this is not a certain directional outcome.")

async def telegram_reporter_tick():
 snap=reporter_snapshot()
 if not snap:return
 n=time.time(); old=reporter_state["regime"]; changed=old is not None and snap["regime"]!=old
 first=old is None; hourly=n-reporter_state["last_hourly"]>=TG_HOURLY
 if first or (changed and n-reporter_state["last_change"]>=TG_CHANGE_COOLDOWN) or hourly:
  pub=await public_market()
  reason="STARTUP" if first else (f"REGIME CHANGE {old} -> {snap['regime']}" if changed else "HOURLY SUMMARY")
  if await telegram_send(regime_message(snap,pub,reason)):
   if first or changed: reporter_state["regime"]=snap["regime"]; reporter_state["last_change"]=n
   reporter_state["last_hourly"]=n


async def report():
 spot=["BINANCE_SPOT","BYBIT_SPOT","OKX_SPOT","GATE_SPOT"]; fut=["BINANCE_FUTURES","BYBIT_FUTURES","OKX_FUTURES","GATE_FUTURES"]
 while 1:
  await asyncio.sleep(5)
  if MOMENTUM_VERBOSE: print(f"\n=== MOMENTUM | FLOW RADAR V5.1 | {W}s | FLOW CONFIRM ===")
  for sym in SYMBOLS:
   votes=[]
   if MOMENTUM_VERBOSE: print(f"\n{sym} price={price(sym)}")
   for ex in spot+fut:
    b,se,d,im,c=flow(sym,W,ex)
    if MOMENTUM_VERBOSE: print(f" {ex:<17} B=${b:,.0f} S=${se:,.0f} D=${d:,.0f} IMB={im:+.1f}% n={c}")
    if b+se>=1000:votes.append("BUY" if d>0 else "SELL")
   sb,ss,sd,si,sc=group(sym,spot)
   fb,ffs,fd,fi,fc=group(sym,fut)
   ab,ase,ad,ai,ac=flow(sym)
   remember_confirm(sym,price(sym),sd,fd,ad,ai,ab,ase,votes.count("BUY"),votes.count("SELL"))
   fs=flow_score(sym)
   if fs:
    if MOMENTUM_VERBOSE:
     print(f" FLOW SCORE        {fs['score']:+d}/100 | {fs['regime']} | flow={fs['flow']:+.1f} confirm={fs['confirm']:+.1f} price={fs['price']:+.1f} btc={fs['btc']:+.1f} breadth={fs['breadth']:+.1f} | RS60={fs['rs']:+.3f}% valid={fs['bvalid']} | 30s={fs['m30']} 60s={fs['m60']} | REPORT-ONLY")
    record_score(sym,fs)
   if MOMENTUM_VERBOSE:
    setup=fast_setup(sym)
    if setup: print(" "+setup)
    detect(sym)
  if MOMENTUM_VERBOSE:
   print("\n "+alt_breadth(30))
   print(" "+alt_breadth(60))
  print_premove_top()
  await telegram_reporter_tick()
  outcomes()
  score_outcomes()
async def main():
 print("FLOW RADAR V5.2 STARTED | PREMOVE TOP-10 QUIET LOG | OKX SUPPORTED-MARKETS ONLY | 4-VENUE FLOW | MOMENTUM CALCS ACTIVE | READ-ONLY | NO ORDER ROUTING")
 await load_binance_universe()
 # Optional exchange metadata runs in background so PREMOVE starts immediately.
 if TG_TOKEN and TG_CHAT_ID:
  ok=await telegram_send("FLOW RADAR TELEGRAM REPORTER ONLINE - waiting for 30s/60s warm-up.")
  print("TELEGRAM STARTUP TEST OK" if ok else "TELEGRAM STARTUP TEST FAILED")
 else:
  print("TELEGRAM REPORTER DISABLED | missing TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID")
 tasks=[report(),refresh_premove_market(),venue_metadata_background()]
 # Keep all four venue families. Unsupported contracts reconnect harmlessly; discovered
 # metadata/actual trades are reflected in VENUES x/4 instead of blocking the scanner.
 for s in SYMBOLS:
  tasks += [binance(s,1),binance(s,0),bybit(s,1),bybit(s,0),okx(s,1),okx(s,0),gate(s,1),gate(s,0)]
 await asyncio.gather(*tasks)
asyncio.run(main())
