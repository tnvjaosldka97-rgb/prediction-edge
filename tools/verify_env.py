#!/usr/bin/env python
"""
.env 검증 스크립트.

체크 항목:
1. PRIVATE_KEY로 유도되는 주소가 WALLET_ADDRESS와 일치
2. POLYGON_RPC 연결 가능
3. 지갑의 POL(가스) 잔고
4. 지갑의 USDC 잔고
5. Polymarket 인증 키 자동 유도 가능 여부

PRIVATE_KEY는 화면에 절대 출력 안 함.
"""
from __future__ import annotations
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = ROOT / ".env"


def load_env() -> dict:
    if not ENV_PATH.exists():
        print(f"[FAIL] {ENV_PATH} 없음")
        sys.exit(1)
    out = {}
    for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def check_address_match(env: dict) -> bool:
    print("\n[1/5] PRIVATE_KEY -> WALLET_ADDRESS 검증...")
    pk = env.get("PRIVATE_KEY", "")
    expected = env.get("WALLET_ADDRESS", "")
    if not pk:
        print("  FAIL: PRIVATE_KEY 비어있음")
        return False
    if not expected:
        print("  FAIL: WALLET_ADDRESS 비어있음")
        return False
    try:
        from eth_account import Account
        acct = Account.from_key(pk)
        derived = acct.address
        if derived.lower() == expected.lower():
            print(f"  OK: {derived[:6]}...{derived[-4:]} 일치")
            return True
        else:
            print(f"  FAIL: 유도 주소가 WALLET_ADDRESS와 다름")
            return False
    except Exception as e:
        print(f"  FAIL: {e}")
        return False


def check_rpc(env: dict) -> bool:
    print("\n[2/5] POLYGON_RPC 연결...")
    rpc = env.get("POLYGON_RPC", "")
    if not rpc:
        print("  FAIL: POLYGON_RPC 비어있음")
        return False
    try:
        from web3 import Web3
        w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 10}))
        # is_connected()가 오작동하는 RPC 있어서 직접 chain_id로 검증
        chain_id = w3.eth.chain_id
        if chain_id != 137:
            print(f"  FAIL: chain_id={chain_id} (Polygon은 137)")
            return False
        block = w3.eth.block_number
        print(f"  OK: Polygon mainnet 연결, 최신 블록 #{block:,}")
        return True
    except Exception as e:
        print(f"  FAIL: {e}")
        return False


def check_pol_balance(env: dict) -> tuple[bool, float]:
    print("\n[3/5] POL (가스) 잔고...")
    addr = env.get("WALLET_ADDRESS", "")
    rpc = env.get("POLYGON_RPC", "")
    try:
        from web3 import Web3
        w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 10}))
        bal_wei = w3.eth.get_balance(Web3.to_checksum_address(addr))
        bal = bal_wei / 1e18
        if bal < 0.01:
            print(f"  WARN: {bal:.6f} POL — 가스비 부족 (Polymarket 첫 거래 시 필요)")
            print("  -> 거래소에서 1~5 POL 추가 출금 필요")
            return True, bal  # 경고만, 실패 아님
        else:
            print(f"  OK: {bal:.4f} POL")
            return True, bal
    except Exception as e:
        print(f"  FAIL: {e}")
        return False, 0.0


def check_usdc_balance(env: dict) -> tuple[bool, float]:
    print("\n[4/5] USDC 잔고...")
    addr = env.get("WALLET_ADDRESS", "")
    rpc = env.get("POLYGON_RPC", "")
    # Polygon native USDC (Circle 발행) — Polymarket이 사용
    USDC_NATIVE = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"
    USDC_BRIDGED = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # USDC.e (구버전, 일부 거래소가 보냄)
    erc20_abi = [{
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function",
    }]
    try:
        from web3 import Web3
        w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 10}))
        wallet = Web3.to_checksum_address(addr)

        bal_native = 0.0
        bal_bridged = 0.0
        try:
            c1 = w3.eth.contract(address=Web3.to_checksum_address(USDC_NATIVE), abi=erc20_abi)
            bal_native = c1.functions.balanceOf(wallet).call() / 1e6
        except Exception:
            pass
        try:
            c2 = w3.eth.contract(address=Web3.to_checksum_address(USDC_BRIDGED), abi=erc20_abi)
            bal_bridged = c2.functions.balanceOf(wallet).call() / 1e6
        except Exception:
            pass

        total = bal_native + bal_bridged
        print(f"  Native USDC:  ${bal_native:.4f}")
        print(f"  Bridged USDC: ${bal_bridged:.4f}  (USDC.e — 일부 거래소가 이걸 보냄)")
        if total < 1:
            print(f"  INFO: 합계 ${total:.4f} — 입금 필요 (테스트는 $5+, 운용은 $50+ 권장)")
        else:
            print(f"  OK: 합계 ${total:.4f}")
        return True, total
    except Exception as e:
        print(f"  FAIL: {e}")
        return False, 0.0


def check_clob_creds(env: dict) -> bool:
    print("\n[5/5] Polymarket CLOB 인증 키 유도...")
    pk = env.get("PRIVATE_KEY", "")
    if not pk:
        print("  SKIP: PRIVATE_KEY 없음")
        return False
    try:
        from py_clob_client.client import ClobClient
        client = ClobClient("https://clob.polymarket.com", key=pk, chain_id=137)
        api_creds = client.create_or_derive_api_creds()
        # 키 일부만 마스킹해서 표시
        api_key = api_creds.api_key
        masked = api_key[:8] + "..." + api_key[-4:] if len(api_key) > 12 else "***"
        print(f"  OK: CLOB API 키 유도 성공 ({masked})")
        return True
    except ImportError:
        print("  SKIP: py_clob_client 미설치 (필요시 'pip install py-clob-client')")
        return False
    except Exception as e:
        print(f"  FAIL: {e}")
        return False


def main() -> int:
    print("=" * 60)
    print("  prediction_edge .env 검증")
    print("=" * 60)

    env = load_env()

    results = []
    results.append(("주소 일치", check_address_match(env)))
    results.append(("RPC 연결", check_rpc(env)))
    pol_ok, pol_bal = check_pol_balance(env)
    results.append(("POL 잔고", pol_ok))
    usdc_ok, usdc_bal = check_usdc_balance(env)
    results.append(("USDC 잔고", usdc_ok))
    results.append(("CLOB 인증", check_clob_creds(env)))

    print()
    print("=" * 60)
    print("  요약")
    print("=" * 60)
    for name, ok in results:
        flag = "OK  " if ok else "FAIL"
        print(f"  [{flag}] {name}")

    print()
    if pol_bal < 0.01:
        print("다음 할 일: POL 1~5개 + USDC 입금 (둘 다 Polygon 네트워크)")
    elif usdc_bal < 5:
        print("다음 할 일: USDC 입금 (Polygon 네트워크)")
    else:
        print("입금 완료. 라이브 파일럿 준비 가능.")

    failed = sum(1 for _, ok in results if not ok)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
