"""
Shadow-Live — realistic paper trading that captures execution reality.

Unlike naive dry-run which assumes `fill @ order.price`, shadow-live:
  1. Walks the live Polymarket orderbook to compute realistic fill price
  2. Records adverse selection by sampling the mid price at T+5s/60s/5min
     after each virtual fill (how much did the price drift against us?)
  3. Marks virtual positions to market as underlying markets resolve
  4. Persists everything to `virtual_trades` table for offline analysis

This builds real data faster than waiting for API keys — every signal
fired gets a full execution profile with actual orderbook snapshots.
"""
