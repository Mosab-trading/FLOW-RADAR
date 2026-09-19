import asyncio,json,os,time,csv,urllib.request
from collections import defaultdict,deque
from pathlib import Path
import websockets

SYMBOLS=[x.strip().upper() for x in os.getenv("SYMBOLS","BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT").split(",")]
W=int(os.getenv("FLOW_WINDOW","5")); MIN=float(os.getenv("EVENT_MIN_USD","50000")); IMB=float(os.getenv("EVENT_IMBALANCE","70")); COOL=int(os.getenv("EVENT_COOLDOWN","10"))
D=Path("data");D.mkdir(exist_ok=True); RAW=D/"trades.jsonl"; EVENTS=D/"events.csv"
buf=defaultdict(lambda:deque(maxlen=500000)); pending=[]; last={}; multipliers={}

def add(ex,s,p,base_qty,side,ts):
 usd=p*base_qty
 t={"ts":ts,"ex":ex,"s":s,"p":p,"qty":base_qty,"usd":usd,"side":side};buf[s].append(t)
 with RAW.open("a") as f:f.write(json.dumps(t,separators=(",",":"))+"\n")

def http_json(url):
 with urllib.request.urlopen(url,timeout=15) as r:return json.loads(r.read())

async def load_meta():
 # OKX USDT-SWAP contract multipliers for the four symbols tracked by this build.
 # Avoids Railway REST/WS metadata failures; SWAP trade sz is contract count.
 okx_ctval={
  "BTC-USDT-SWAP":0.01,
  "ETH-USDT-SWAP":0.1,
  "SOL-USDT-SWAP":1.0,
  "XRP-USDT-SWAP":100.0,
 }
 for inst,val in okx_ctval.items():
  multipliers[("OKX",inst)]=val
 print("OKX CONTRACT META LOCAL OK",len(okx_ctval))
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

async def report():
 spot=["BINANCE_SPOT","BYBIT_SPOT","OKX_SPOT","GATE_SPOT"]; fut=["BINANCE_FUTURES","BYBIT_FUTURES","OKX_FUTURES","GATE_FUTURES"]
 while 1:
  await asyncio.sleep(5);print(f"\n=== FLOW RADAR V3.3 | 4 EXCHANGES | SPOT + FUTURES | {W}s ===")
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
   detect(s)
  outcomes()

async def main():
 print("FLOW RADAR V3.3 STARTED | BINANCE + BYBIT + OKX + GATE | SPOT + FUTURES | READ-ONLY")
 await load_meta()
 tasks=[report()]
 for s in SYMBOLS:
  tasks += [binance(s,1),binance(s,0),bybit(s,1),bybit(s,0),okx(s,1),okx(s,0),gate(s,1),gate(s,0)]
 await asyncio.gather(*tasks)
asyncio.run(main())
