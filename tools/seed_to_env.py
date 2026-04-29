#!/usr/bin/env python
"""
시드구문 → PRIVATE_KEY 변환기.

12단어 시드를 터미널에서 비공개 입력 받아 (getpass — 화면에 안 보임)
MetaMask Account 1 표준 경로(m/44'/60'/0'/0/0)로 개인키 유도 후 .env에 저장.

사용:
    venv/Scripts/python tools/seed_to_env.py

안전장치:
- 시드구문은 화면에 입력 중 표시 안 됨
- 유도 주소가 .env의 WALLET_ADDRESS와 일치하는지 자동 검증
- 일치 안 하면 사용자 확인 후에만 저장
- PRIVATE_KEY는 화면 출력 절대 안 함
- 함수 종료 시 시드/개인키 변수 None 처리
"""
from __future__ import annotations
import sys
import getpass
from pathlib import Path

try:
    from eth_account import Account
except ImportError:
    print("[ERROR] eth_account 라이브러리 필요")
    print("설치 명령:")
    print("  venv/Scripts/pip install eth-account")
    sys.exit(1)


# 스크립트는 prediction_edge/tools/ 안에 있고 .env는 한 단계 위
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"


def _read_env() -> list[str]:
    if not ENV_PATH.exists():
        print(f"[ERROR] {ENV_PATH} 없음")
        sys.exit(1)
    return ENV_PATH.read_text(encoding="utf-8").splitlines()


def _get_env_value(lines: list[str], key: str) -> str | None:
    prefix = key + "="
    for line in lines:
        if line.startswith(prefix):
            return line[len(prefix):].strip()
    return None


def _replace_or_append(lines: list[str], key: str, value: str) -> list[str]:
    prefix = key + "="
    out = []
    replaced = False
    for line in lines:
        if line.startswith(prefix):
            out.append(f"{key}={value}")
            replaced = True
        else:
            out.append(line)
    if not replaced:
        out.append(f"{key}={value}")
    return out


def main() -> int:
    print("=" * 60)
    print("  시드구문 -> PRIVATE_KEY 변환기")
    print("=" * 60)
    print()
    print("종이에 적어둔 12단어 시드구문을 입력하세요.")
    print("(단어 사이는 공백, 입력 중 화면에 표시되지 않음)")
    print()

    # 비공개 입력 — 터미널에 안 보임
    seed = getpass.getpass("시드구문: ").strip().lower()

    words = seed.split()
    if len(words) not in (12, 15, 18, 21, 24):
        print()
        print(f"[ERROR] 단어 개수가 {len(words)}개. 12/15/18/21/24 중 하나여야 함.")
        seed = None  # noqa: F841 — clear sensitive
        return 1

    mnemonic = " ".join(words)

    Account.enable_unaudited_hdwallet_features()

    # MetaMask Account 1 = m/44'/60'/0'/0/0 (BIP44 Ethereum 표준 첫 계정)
    try:
        acct = Account.from_mnemonic(mnemonic, account_path="m/44'/60'/0'/0/0")
    except Exception as e:
        print()
        print(f"[ERROR] 시드 -> 개인키 변환 실패: {e}")
        seed = None
        mnemonic = None
        return 1

    derived_address = acct.address
    private_key_hex = acct.key.hex()
    if not private_key_hex.startswith("0x"):
        private_key_hex = "0x" + private_key_hex

    # 메모리에서 시드 즉시 제거
    seed = None
    mnemonic = None

    print()
    print(f"유도된 주소: {derived_address}")

    # .env의 WALLET_ADDRESS와 교차 검증
    env_lines = _read_env()
    expected = _get_env_value(env_lines, "WALLET_ADDRESS")

    if expected:
        if derived_address.lower() == expected.lower():
            print(f"[OK] .env WALLET_ADDRESS와 일치")
        else:
            print()
            print(f"[WARN] .env WALLET_ADDRESS({expected})와 다름!")
            print("- 시드를 잘못 입력했거나")
            print("- 다른 계정(Account 2 등)의 시드일 가능성")
            print()
            confirm = input("이 키를 그래도 .env에 저장할까요? (yes/no): ").strip().lower()
            if confirm != "yes":
                print("취소됨. .env 미수정.")
                private_key_hex = None
                return 1
    else:
        print("[INFO] .env에 WALLET_ADDRESS 없음 — 검증 스킵")

    # PRIVATE_KEY 갱신 + WALLET_ADDRESS 누락 시 함께 채움
    new_lines = _replace_or_append(env_lines, "PRIVATE_KEY", private_key_hex)
    if not expected:
        new_lines = _replace_or_append(new_lines, "WALLET_ADDRESS", derived_address)

    ENV_PATH.write_text("\n".join(new_lines) + "\n", encoding="utf-8")

    # 메모리 클리어
    private_key_hex = None
    derived_address = None
    acct = None

    print()
    print(f"[OK] PRIVATE_KEY가 {ENV_PATH}에 저장됨")
    print()
    print("주의:")
    print("- 종이의 시드구문은 안전한 곳에 보관하세요")
    print("- 이 터미널 출력에 PRIVATE_KEY는 표시되지 않았습니다")
    print("- .env를 다른 사람에게 공유하지 마세요")
    return 0


if __name__ == "__main__":
    sys.exit(main())
