# Flow Radar V1

Read-only research collector for aggressive executed trade flow.

## Exchanges
- Binance USD-M Futures
- Bybit linear perpetuals
- OKX perpetual swaps

## What V1 does
- Records public executed trades to `data/trades.jsonl`
- Classifies taker/aggressive BUY vs SELL
- Calculates net USD flow (CVD-style) over 1s, 5s, 30s, 60s and 5m
- Binance + Bybit are combined for USD-flow calculations
- OKX is recorded, but excluded from merged USD totals in V1 because swap size is contract-denominated and needs instrument-specific contract-value conversion.

## Railway
Deploy these files as a service. No API keys are needed.
Optional variables:
- SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT
- PRINT_EVERY=5
- DATA_DIR=data

This version does NOT place orders. It is intentionally a measurement/data-collection stage.
