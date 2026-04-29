"""POLYGON_RPC을 무료 공개 RPC로 교체."""
from pathlib import Path

ENV = Path(__file__).resolve().parent.parent / ".env"
NEW_RPC = "https://polygon-bor-rpc.publicnode.com"

lines = ENV.read_text(encoding="utf-8").splitlines()
new = []
replaced = False
for line in lines:
    if line.startswith("POLYGON_RPC="):
        old = line.split("=", 1)[1].strip()
        new.append(f"POLYGON_RPC={NEW_RPC}")
        print(f"교체: {old}")
        print(f"  -> {NEW_RPC}")
        replaced = True
    else:
        new.append(line)
if not replaced:
    new.append(f"POLYGON_RPC={NEW_RPC}")
    print(f"추가: POLYGON_RPC={NEW_RPC}")
ENV.write_text("\n".join(new) + "\n", encoding="utf-8")
print("저장 완료.")
