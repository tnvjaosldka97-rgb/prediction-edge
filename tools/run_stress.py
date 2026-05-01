"""
Stress test 수동 실행 도구.

사용:
    venv/Scripts/python tools/run_stress.py
    venv/Scripts/python tools/run_stress.py --json    # JSON 출력
"""
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from stress.suite import run_all


def main():
    result = asyncio.run(run_all())
    if "--json" in sys.argv:
        print(json.dumps(result, indent=2))
    else:
        print("=" * 60)
        print(f"  Stress Test 결과")
        print("=" * 60)
        print(f"  Total: {result['n_total']}")
        print(f"  Pass:  {result['n_pass']}")
        print(f"  Fail:  {result['n_fail']}")
        print(f"  Pass rate: {result['pass_rate']*100:.0f}%")
        print()
        if result["failures"]:
            print("FAILURES:")
            for f in result["failures"]:
                print(f"  ❌ {f['name']}: {f['error']}")
        else:
            print("✅ ALL PASSED")
    return 0 if result["n_fail"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
