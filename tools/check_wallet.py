import os, json
from dotenv import load_dotenv
load_dotenv()

import httpx

RPC = os.getenv("POLYGON_RPC")
WALLET = os.getenv("WALLET_ADDRESS")
USDC_NATIVE = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"
USDC_E      = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
NEG_RISK     = "0xC5d563A36AE78145C45a50134d48A1215220f80a"

def call(method, params):
    r = httpx.post(RPC, json={"jsonrpc":"2.0","method":method,"params":params,"id":1}, timeout=15)
    return r.json().get("result")

def hex_to_int(h):
    return int(h, 16) if h and h != "0x" else 0

def pad(addr):
    return addr[2:].lower().rjust(64, "0")

print(f"Wallet: {WALLET}")
print(f"RPC: {RPC}\n")

matic_wei = hex_to_int(call("eth_getBalance", [WALLET, "latest"]))
print(f"MATIC: {matic_wei/1e18:.6f}")

for name, addr in [("USDC.e (Polymarket primary)", USDC_E), ("USDC native", USDC_NATIVE)]:
    bal_data = "0x70a08231" + pad(WALLET)
    raw = hex_to_int(call("eth_call", [{"to": addr, "data": bal_data}, "latest"]))
    print(f"{name}: {raw/1e6:.4f}")
    for spname, spaddr in [("CTF Exchange", CTF_EXCHANGE), ("NegRisk", NEG_RISK)]:
        allow_data = "0xdd62ed3e" + pad(WALLET) + pad(spaddr)
        allow_raw = hex_to_int(call("eth_call", [{"to": addr, "data": allow_data}, "latest"]))
        if allow_raw > 1e30:
            shown = "MAX (unlimited)"
        else:
            shown = f"{allow_raw/1e6:.4f}"
        print(f"  allowance -> {spname}: {shown}")

txcount = hex_to_int(call("eth_getTransactionCount", [WALLET, "latest"]))
print(f"\nTx count (nonce): {txcount}")
