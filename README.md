Flow Radar V3.2 — OKX Futures diagnostic fix
No strategy/event threshold changes.
Changes only to OKX Futures:
- subscribes to OKX public `trades-all` channel for SWAP instruments
- keeps Spot on `trades`
- prints OKX subscription/error response
- prints first two raw trade payloads per symbol
- prints explicit SKIP NO ctVal if contract metadata is missing
- converts SWAP contract size to base quantity with ctVal before USD notional
Read-only; no API keys; no orders.
