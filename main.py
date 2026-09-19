import asyncio, json, os, time
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
import websockets

SYMBOLS = [x.strip().upper() for x in os.getenv("SYMBOLS","BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT").split(",") if x.strip()]
WINDOWS = [1,5,30,60,300]
PRINT_EVERY = float(os.getenv("PRINT_EVERY","5"))
DATA_DIR = Path(os.getenv("DATA_DIR","data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
RAW_FILE = DATA_DIR / "trades.jsonl"

# Public market-data only: NO API keys and NO order placement.
@dataclass
class Trade:
    ts: float
    exchange: str
    symbol: str
    price: float
    qty: float
    usd: float
    side: str  # BUY = aggressive/taker buy; SELL = aggressive/taker sell

buf = defaultdict(lambda: deque(maxlen=300000))
last_price = {}

def norm_symbol(s): return s.replace("-","").replace("_","").upper()

def record(t: Trade):
    buf[t.symbol].append(t)
    last_price[(t.exchange,t.symbol)] = t.price
    with RAW_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(t.__dict__, separators=(",",":")) + "\n")

async def binance(symbol):
    # Binance changed USD-M public WS routing in 2026.
    url = f"wss://fstream.binance.com/public/ws/{symbol.lower()}@aggTrade"
    while True:
        try:
            async with websockets.connect(url, ping_interval=120, ping_timeout=20, max_queue=10000) as ws:
                async for raw in ws:
                    d=json.loads(raw)
                    p=float(d["p"]); q=float(d["q"])
                    # m=True => buyer is maker => aggressive seller
                    side="SELL" if d.get("m") else "BUY"
                    record(Trade(d.get("T",d.get("E",int(time.time()*1000)))/1000,"BINANCE",symbol,p,q,p*q,side))
        except Exception as e:
            print("BINANCE reconnect",symbol,repr(e)); await asyncio.sleep(3)

async def bybit(symbol):
    url="wss://stream.bybit.com/v5/public/linear"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_queue=10000) as ws:
                await ws.send(json.dumps({"op":"subscribe","args":[f"publicTrade.{symbol}"]}))
                async for raw in ws:
                    d=json.loads(raw)
                    if not str(d.get("topic","")).startswith("publicTrade."): continue
                    for x in d.get("data",[]):
                        p=float(x["p"]); q=float(x["v"])
                        side="BUY" if x["S"].lower()=="buy" else "SELL"
                        record(Trade(int(x.get("T",d.get("ts",int(time.time()*1000))))/1000,"BYBIT",symbol,p,q,p*q,side))
        except Exception as e:
            print("BYBIT reconnect",symbol,repr(e)); await asyncio.sleep(3)

async def okx(symbol):
    # OKX linear perpetual convention, e.g. BTC-USDT-SWAP.
    inst = symbol[:-4] + "-USDT-SWAP" if symbol.endswith("USDT") else symbol
    url="wss://ws.okx.com:8443/ws/v5/public"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_queue=10000) as ws:
                await ws.send(json.dumps({"op":"subscribe","args":[{"channel":"trades","instId":inst}]}))
                async for raw in ws:
                    d=json.loads(raw)
                    if "data" not in d: continue
                    for x in d["data"]:
                        p=float(x["px"]); q=float(x["sz"])
                        # IMPORTANT: OKX swap sz is contracts, not universally base-asset qty.
                        # Keep OKX directional flow, but do not merge its USD amount until contract-value conversion is added.
                        side="BUY" if x["side"].lower()=="buy" else "SELL"
                        record(Trade(int(x["ts"])/1000,"OKX",symbol,p,q,0.0,side))
        except Exception as e:
            print("OKX reconnect",symbol,repr(e)); await asyncio.sleep(3)

def calc(symbol, seconds):
    now=time.time(); buy=sell=0.0; nbuy=nsell=0
    for t in reversed(buf[symbol]):
        if now-t.ts > seconds: break
        if t.exchange=="OKX": continue  # avoid false cross-exchange USD math in V1
        if t.side=="BUY": buy+=t.usd; nbuy+=1
        else: sell+=t.usd; nsell+=1
    total=buy+sell
    delta=buy-sell
    imbalance=(delta/total*100) if total else 0
    return buy,sell,delta,imbalance,nbuy,nsell

async def reporter():
    while True:
        await asyncio.sleep(PRINT_EVERY)
        print("\n=== FLOW RADAR | aggressive executed trades | Binance + Bybit USD flow; OKX recorded separately ===")
        for s in SYMBOLS:
            parts=[]
            for w in WINDOWS:
                b,se,d,im,nb,ns=calc(s,w)
                parts.append(f"{w}s Δ=${d:,.0f} ({im:+.1f}%) B=${b:,.0f} S=${se:,.0f}")
            print(s," | ".join(parts))

async def main():
    print("FLOW RADAR V1 STARTED | READ-ONLY | NO TRADING | symbols:", ",".join(SYMBOLS))
    tasks=[asyncio.create_task(reporter())]
    for s in SYMBOLS:
        tasks += [asyncio.create_task(binance(s)), asyncio.create_task(bybit(s)), asyncio.create_task(okx(s))]
    await asyncio.gather(*tasks)

if __name__=="__main__":
    asyncio.run(main())
