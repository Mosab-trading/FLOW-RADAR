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
  await asyncio.sleep(5);print(f"\n=== FLOW RADAR V4.1 | 4 EXCHANGES | SPOT + FUTURES | {W}s | FLOW CONFIRM ===")
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
