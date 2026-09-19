Flow Radar V3.1
Same V3 logic and feeds. Only OKX futures contract metadata loading was changed:
- uses OKX official public WebSocket `instruments` channel for SWAP metadata
- avoids the Railway HTTP 403 seen on OKX REST
- keeps Spot/Futures separation, Gate normalization, events, outcomes and agreement unchanged.
Read-only; no API keys; no orders.
