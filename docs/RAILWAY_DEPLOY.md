# Railway 재배포 가이드

새 지갑·새 .env로 Railway에 배포하기. DRY_RUN 모드로 시작.

## 사전 준비

- [x] 로컬 .env 파일 작동 확인 (`tools/verify_env.py` 5/5 OK)
- [x] 새 지갑 PRIVATE_KEY .env에 입력
- [x] Polygon RPC 작동 (publicnode.com 또는 Alchemy)
- [ ] Railway 계정 + 기존 prediction_edge 프로젝트 (있으면 기존 프로젝트에 새 환경변수만 갈아끼움)

---

## Step 1 — Railway 환경변수 셋업

Railway 대시보드 → 프로젝트 → **Variables** 탭

### 필수 변수 (전부 .env와 동일하게)
```
PRIVATE_KEY              = 0x...                    # 64자 hex
WALLET_ADDRESS           = 0xA30F5859A44f94...      # 본인 새 주소
POLY_API_KEY             =                          # 빈 값 (봇이 자동 생성)
POLY_API_SECRET          =
POLY_API_PASSPHRASE      =
POLYGON_RPC              = https://polygon-bor-rpc.publicnode.com
DRY_RUN                  = true                     # ⚠️ 첫 배포는 반드시 true
BANKROLL                 = 100
DB_PATH                  = /data/prediction_edge.db # Railway 영구 볼륨
ANTHROPIC_API_KEY        = sk-ant-...               # claude_oracle 시그널 사용 시
```

### 신규 (Day 4~5에서 추가)
```
ADMIN_PASSWORD_HASH      = sha256:abc123...         # 아래 생성 명령 참조
DASHBOARD_SESSION_SECRET = 64자 랜덤 hex            # secrets.token_hex(32) 출력
ADMIN_IPS                =                          # 빈 값 (모든 IP 허용) 또는 본인 IP
```

### ADMIN_PASSWORD_HASH 생성 (로컬에서 한 번)
```bash
venv\Scripts\python -c "
from dashboard.auth import hash_password_for_env
print(hash_password_for_env('내 대시보드 비밀번호'))
"
# 출력: sha256:abc123... 또는 $2b$...
```
이 값을 Railway 변수에 붙여넣기.

### DASHBOARD_SESSION_SECRET 생성
```bash
venv\Scripts\python -c "import secrets; print(secrets.token_hex(32))"
# 출력: 64자 랜덤 hex
```

---

## Step 2 — 코드 푸시

```bash
git push origin main
```

Railway는 main 브랜치 push 시 자동 빌드·배포됨 (railway.toml 설정).

---

## Step 3 — 빌드 로그 확인

Railway 대시보드 → Deployments → 최신 → Logs

성공 시 다음 로그 떠야 함:
```
[config] DB_PATH ok: /data/prediction_edge.db
[config] DRY_RUN raw='true' → parsed=True
[OK] CLOB API 키 유도 성공
[OK] Polygon mainnet 연결, 최신 블록 #...
[bot] DRY_RUN paper trading 시작
```

---

## Step 4 — 대시보드 URL 확인

Railway 프로젝트 → Settings → **Networking** → Generate Domain

URL: `https://your-app.up.railway.app`

브라우저 접속:
- `/` → 메인 대시보드 (read-only)
- `/auth/login` 으로 POST 비밀번호 → 컨트롤 사용 가능

### curl로 로그인 테스트
```bash
curl -c cookies.txt -X POST https://your-app.up.railway.app/auth/login \
  -H "Content-Type: application/json" \
  -d '{"password":"내 비밀번호"}'
```

---

## Step 5 — DRY_RUN 검증 (24~48시간)

1. Railway 로그에서 시그널 발생 확인 (closing_convergence 시그널 떠야 함)
2. 대시보드에서 portfolio 곡선 추적
3. friction_traces 테이블 채워지는지 확인:
   ```bash
   curl -b cookies.txt https://your-app.up.railway.app/api/friction/summary?since_hours=24
   ```
4. 24~48시간 후 trace 50건 이상 누적되면:
   ```bash
   curl -b cookies.txt -X POST https://your-app.up.railway.app/api/control/calibrate
   ```
   → 모델 첫 캘리브레이션 완료

---

## Step 6 — LIVE_PILOT 전환 (USDC 입금 후)

⚠️ **순서 절대 중요**

1. 메타마스크에 USDC ≥ $20 + POL ≥ 1 도착 확인
2. 대시보드 로그인 (5분 이내)
3. **자본 cap 설정**:
   ```bash
   curl -b cookies.txt -X POST https://your-app.up.railway.app/api/control/bankroll_cap \
     -d '{"bankroll_cap_usd":20}'
   ```
4. **모드 변경 — LIVE_PILOT**:
   ```bash
   curl -b cookies.txt -X POST https://your-app.up.railway.app/api/control/mode \
     -d '{"mode":"LIVE_PILOT","confirm_token":"PROMOTE"}'
   ```
5. Railway 로그에서 첫 실주문 확인:
   ```
   [FILL] BUY 10sh @ 0.5500 | $5.50 | strategy=closing_convergence
   ```

---

## Step 7 — 비상 정지 (필요 시)

```bash
curl -b cookies.txt -X POST https://your-app.up.railway.app/api/control/emergency_stop
```
→ 즉시 DRY_RUN 복귀 + killswitch trip + 미체결 주문 취소 시도

---

## 비용

| 플랜 | 가동 | 월 비용 |
|---|---|---|
| Free | 약 500시간/월 (한 달 못 채움) | $0 |
| **Hobby** | **무제한 24/7** | **$5** |

→ 정식 운용 시 Hobby 권장. 무료는 검증용.

---

## 트러블슈팅

| 증상 | 해결 |
|---|---|
| 빌드 실패 — pip install error | requirements.txt 확인 |
| 시작 후 즉시 crash | Railway Logs → 마지막 에러 확인. 보통 .env 누락 |
| 대시보드 401 | ADMIN_PASSWORD_HASH 미설정 또는 비밀번호 다름 |
| 대시보드 403 IP not allowed | ADMIN_IPS에 본인 IP 추가 또는 비워두기 |
| friction_traces 안 채워짐 | DRY_RUN 모드는 instrumentation 안 함. LIVE/SHADOW만 채워짐 |
| Polygon RPC 429 | publicnode.com 무료라 가끔 throttle. Alchemy/Infura 유료로 |
| DB 안 영구화 | DB_PATH=/data/... 인지, Volume 마운트 됐는지 |
