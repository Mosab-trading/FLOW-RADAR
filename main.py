import asyncio,json,os,time,csv
from collections import defaultdict,deque
from pathlib import Path
import websockets
SYMBOLS=[x.strip().upper() for x in os.getenv("SYMBOLS","BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT").split(",")]
W=int(os.getenv("FLOW_WINDOW","5")); MIN=float(os.getenv("EVENT_MIN_USD","50000")); IMB=float(os.getenv("EVENT_IMBALANCE","70")); COOL=int(os.getenv("EVENT_COOLDOWN","10"))
D=Path("data");D.mkdir(exist_ok=True); raw=D/"trades.jsonl"; events=D/"events.csv"
buf=defaultdict(lambda:deque(maxlen=400000)); pending=[]; last={}
def add(ex,s,p,q,side,ts):
 t={"ts":ts,"ex":ex,"s":s,"p":p,"q":q,"usd":p*q,"side":side};buf[s].append(t)
 with raw.open("a") as f:f.write(json.dumps(t)+"\n")
async def bn(s):
 u=f"wss://fstream.binance.com/public/ws/{s.lower()}@aggTrade"
 while 1:
  try:
   async with websockets.connect(u,ping_interval=120,ping_timeout=20) as w:
    async for r in w:
     x=json.loads(r);p=float(x["p"]);q=float(x["q"]);add("BINANCE",s,p,q,"SELL" if x.get("m") else "BUY",x.get("T",x["E"])/1000)
  except Exception as e: print("BINANCE reconnect",s,e);await asyncio.sleep(3)
async def bb(s):
 u="wss://stream.bybit.com/v5/public/linear"
 while 1:
  try:
   async with websockets.connect(u,ping_interval=20,ping_timeout=20) as w:
    await w.send(json.dumps({"op":"subscribe","args":[f"publicTrade.{s}"]}))
    async for r in w:
     x=json.loads(r)
     for z in x.get("data",[]):
      p=float(z["p"]);q=float(z["v"]);add("BYBIT",s,p,q,"BUY" if z["S"]=="Buy" else "SELL",int(z["T"])/1000)
  except Exception as e: print("BYBIT reconnect",s,e);await asyncio.sleep(3)
def flow(s,ex=None):
 n=time.time();b=se=0.;c=0
 for t in reversed(buf[s]):
  if n-t["ts"]>W:break
  if ex and t["ex"]!=ex:continue
  c+=1
  if t["side"]=="BUY":b+=t["usd"]
  else:se+=t["usd"]
 tot=b+se;d=b-se;im=d/tot*100 if tot else 0
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
   if str(h) not in e["r"] and n>=e["ts"]+h and price(e["s"]):
    e["r"][str(h)]=(price(e["s"])/e["entry"]-1)*100
  if "300" in e["r"]:
   new=not events.exists()
   with events.open("a",newline="") as f:
    w=csv.writer(f)
    if new:w.writerow(["time","symbol","direction","entry","buy_usd","sell_usd","delta","imbalance","ret5","ret30","ret60","ret300"])
    r=e["r"];w.writerow([e["ts"],e["s"],e["dir"],e["entry"],e["b"],e["sell"],e["d"],e["im"],r["5"],r["30"],r["60"],r["300"]])
   print(f'RESULT {e["s"]} {e["dir"]} 5s={r["5"]:+.3f}% 30s={r["30"]:+.3f}% 60s={r["60"]:+.3f}% 300s={r["300"]:+.3f}%');pending.remove(e)
async def report():
 while 1:
  await asyncio.sleep(5);print(f"\n=== FLOW RADAR V2 | {W}s | DIAGNOSTIC ===")
  for s in SYMBOLS:
   print(f"\n{s} price={price(s)}")
   for ex in ("BINANCE","BYBIT"):
    b,se,d,im,c=flow(s,ex);print(f" {ex:<8} B=${b:,.0f} S=${se:,.0f} DELTA=${d:,.0f} IMB={im:+.1f}% trades={c}")
   b,se,d,im,c=flow(s);print(f" COMBINED B=${b:,.0f} S=${se:,.0f} DELTA=${d:,.0f} IMB={im:+.1f}% trades={c}");detect(s)
  outcomes()
async def main():
 print("FLOW RADAR V2 STARTED | READ-ONLY | NO ORDERS")
 print("EVENT RULE:",W,"sec | min volume $",MIN,"| imbalance",IMB,"%")
 await asyncio.gather(report(),*[x for s in SYMBOLS for x in (bn(s),bb(s))])
asyncio.run(main())
