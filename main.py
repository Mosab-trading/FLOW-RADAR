import asyncio,json,os,time,csv,urllib.request
from collections import defaultdict,deque
from pathlib import Path
import websockets

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
