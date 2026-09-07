# Global HR & Market Intelligence Dashboard — 자동화 파이프라인 (v3)

한국타이어 진출 **39개국**의 경제 지표·환율·현지 뉴스·비즈니스/HR 동향을 매일 한국시간(KST) 06:00에
자동 수집하여, **권역 탭 → 국가 칩** 클릭만으로 새로고침 없이 조회할 수 있는 대시보드를 갱신하고
**GitHub Pages**로 서빙하는 파이프라인입니다.

---

## 1. 디렉토리 구조

```
hankook-daily-brief/
├── requirements.txt              # Python 의존성
├── README.md                     # 이 문서
├── .gitignore
├── src/
│   ├── collector.py               # ① 39개국 데이터 수집 (경제지표/환율/뉴스/산업동향, 국가 단위 병렬 처리)
│   └── build_site.py              # ② 39개국 JSON을 템플릿에 통째로 임베드 → docs/index.html 생성
├── template/
│   └── template.html              # Jinja2 셸 + 클라이언트 JS (권역 탭/국가 칩/차트를 전부 JS로 렌더링)
├── data/
│   └── countries_data.json        # collector.py 결과 통합 캐시 (39개국, 자동 생성/갱신)
├── docs/
│   ├── index.html                 # 최종 배포 산출물 (GitHub Pages가 이 폴더를 서빙)
│   └── archive/
│       ├── index.json              # 유효 아카이브 날짜 목록(min/max) — 날짜 피커 범위 지정용
│       └── YYYY-MM-DD.json         # 일별 스냅샷 (날짜 피커에서 fetch로 비동기 로드, 180일 보존)
└── .github/
    └── workflows/
        └── update.yml              # 매일 KST 06:00 자동 실행 스케줄러
```

**데이터 흐름**: `collector.py` (39개국 병렬 수집) → `data/countries_data.json` + `docs/archive/{오늘날짜}.json`
→ `build_site.py` (JSON을 `<script type="application/json">` 블록으로 임베드 + 아카이브 날짜 범위 전달)
→ `docs/index.html` → GitHub Pages 배포

> 국가 전환은 서버 재렌더링이 아니라 **브라우저에서 임베드된 JSON을 즉시 다시 그리는 방식**이라
> 페이지 새로고침이 전혀 발생하지 않습니다. 오늘 날짜는 임베드된 데이터를 재사용하고,
> 과거 날짜는 `docs/archive/{날짜}.json`을 `fetch()`로 불러와 동일한 방식으로 다시 그립니다.

---

## 2. 로컬에서 먼저 테스트하기

```bash
# 1) 가상환경 생성 (선택)
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate

# 2) 의존성 설치
pip install -r requirements.txt

# 3) 39개국 데이터 수집 (국가 단위 병렬, 다소 시간 소요될 수 있음)
python src/collector.py

# 4) HTML 생성
python src/build_site.py

# 5) 결과 확인 (브라우저로 열기)
open docs/index.html           # macOS
# 또는 Windows: start docs\index.html
```

`data/countries_data.json`에 39개국 전체 데이터가 저장되고, `docs/index.html`을 열면
상단 권역 탭(전체/한국/중국/미주/유럽/중남미/아태/중아)과 국가 칩으로 즉시 전환하며 확인할 수 있습니다.
일부 국가·일부 API가 실패해도 해당 항목만 이전 캐시값으로 대체되어 전체 실행이 중단되지 않습니다.

---

## 3. 이번 버전(v5)에서 새로 추가된 것

1. **날짜 아카이브 조회**: 매일 수집 결과가 `docs/archive/YYYY-MM-DD.json`으로도 저장되며, 헤더의 날짜 피커에서 과거 날짜를 선택하면 `fetch('./archive/2026-09-01.json')`로 비동기 로드되어 화면 전체(프로필/환율/뉴스/동향)가 새로고침 없이 교체됩니다. `docs/archive/index.json`에 유효 날짜 범위가 기록되어 날짜 피커의 min/max로 사용되며, **180일이 지난 아카이브는 자동 삭제**됩니다.
   > ⚠️ 로컬에서 `docs/index.html`을 `file://`로 그냥 더블클릭해 열면 브라우저 보안 정책상 `fetch()`가 차단되어 날짜 이동이 동작하지 않습니다. 로컬 테스트 시에는 `python -m http.server --directory docs`로 띄운 뒤 `http://localhost:8000`으로 접속하세요.
2. **한국어 기사 우선 수집**: 39개국 전체(한국 포함)에 대해 먼저 한국어 Google News(`hl=ko&gl=KR`)로 검색하고, 슬롯이 부족하면 영문 Google News로 나머지를 채운 뒤 `deep-translator`로 제목/요약을 한국어로 자동 번역합니다. 번역된 항목은 화면에 "EN→KO 자동번역" 배지로 표시되며, 마우스를 올리면 원문 제목을 확인할 수 있습니다.
3. **환율 수집 방식 개선**: 기존 `interval="1mo"` 월봉 대신 ~400일치 일봉을 받아 파이썬(pandas)에서 직접 월별 종가로 리샘플합니다. 직접 페어(`{통화}KRW=X`)의 거래일이 30일 미만이면 자동으로 USD 교차 환산으로 전환해, 일부 통화에서 차트가 비어 보이던 문제를 해결했습니다.
4. **소액 통화 100단위 환산**: VND/JPY/IDR은 1단위 환율이 반올림 시 0원으로 보이는 문제가 있어 `100 {통화}` 기준으로 재계산합니다(`exchange_rate.unit_base` 필드). 카드 타이틀과 차트 캡션에 "100 JPY 기준"처럼 자동으로 표기됩니다.

---

## 4. 39개국 메타데이터 & 권역 구성

`src/collector.py`의 `COUNTRIES`(국가 메타데이터)와 `REGIONS`(권역 정의)에 구조화되어 있습니다.

| 권역 키 | 라벨 | 소속 국가 |
|---|---|---|
| `KR` | 한국 | KR |
| `CN` | 중국 | CN |
| `AMER` | 미주 | US, CA |
| `EU` | 유럽 | DE, HU, GB(영국), FR, IT, ES, PL, CZ, NL, SE, AT, RO, RU, UA, TR, RS, HR, MA |
| `LATAM` | 중남미 | MX, BR, CL, CO, PA |
| `APAC` | 아태 | ID, AU, JP, SG, MY, TH, VN, TW |
| `MEA` | 중아 | AE, SA, EG, KZ |

각 국가 항목에는 `hubs`(한국타이어 거점 리스트), `currency`(통화코드), `flag`(flagcdn 국기 코드)가 함께
매핑되어 있으며, 프로필/거점 카드에 그대로 표시됩니다.

> **영국 코드 안내**: World Bank·flagcdn 등 대부분의 공식 API가 ISO 3166-1 alpha-2 기준 `GB`를
> 사용하므로, 내부적으로는 `GB`로 관리합니다(화면에는 "영국"으로 정상 표시됩니다).

---

## 5. 사용 데이터 소스

| 항목 | 1순위 소스 | 대체(Fallback) 체인 |
|---|---|---|
| 수도 / 총 인구 / 연간 GDP | World Bank Open API (키 불필요, 39개국 공통) | 이전 캐시값 → 기본값(`-`) |
| 인플레이션(CPI YoY) / 실업률 — **미국만** | BLS API (키 없어도 동작) | FRED API(`FRED_API_KEY`) → World Bank 연간지표 → 캐시 |
| 인플레이션 / 실업률 — **미국 외 38개국** | World Bank 연간 지표 (키 불필요) | 이전 캐시값 |
| 연방/테네시 최저임금 (미국만 표시) | DOL 고시 기준 상수값 (`STATUTORY_MIN_WAGE`) | 법정 수치라 API 대신 상수로 관리 |
| 현지통화/KRW 환율(실시간) + 최근 12개월 종가 | Yahoo Finance 직접 페어 (`{통화}KRW=X`) | 동일 통화의 USD 교차 환산(`{통화}USD=X` × `USDKRW=X`) → 캐시 |
| 현지 주요 뉴스 3건 | Google News RSS, 최근 3일(`when:3d`) 최신순 | 이전 캐시값 |
| 산업 동향 (AUTO MARKET/HR & LABOR/ECONOMY/MANAGEMENT) | Google News RSS 키워드 검색, 최근 14일(`when:14d`) | 이전 캐시값 |

> 한국(KRW)은 자국 통화이므로 환율 카드에서 "기준 통화" 표시로 처리되며 차트가 생략됩니다.
> Taiwan(TW)처럼 World Bank가 데이터를 제공하지 않는 국가는 자동으로 캐시/기본값으로 대체됩니다.
> Google News RSS와 World Bank API는 **별도 키 없이 바로 동작**합니다. BLS/FRED 키는 선택 사항입니다.

---

## 6. GitHub 저장소 준비 및 배포

### 5-1. 저장소 생성 & 코드 업로드
```bash
git init
git add .
git commit -m "init: global HR & market intelligence dashboard (39 countries)"
git branch -M main
git remote add origin https://github.com/<YOUR_ID>/<YOUR_REPO>.git
git push -u origin main
```

### 5-2. GitHub Pages 활성화
1. 저장소 **Settings → Pages**
2. **Source**를 **GitHub Actions**로 선택 (워크플로우가 `upload-pages-artifact`로 `docs/`를 업로드합니다)

### 5-3. (선택) Secrets 등록 — 미국 물가/고용 지표 API 호출 한도를 늘리고 싶은 경우
1. BLS: https://www.bls.gov/developers/ 에서 무료 등록키 발급
2. FRED: https://fred.stlouisfed.org/docs/api/api_key.html 에서 무료 API 키 발급
3. 저장소 **Settings → Secrets and variables → Actions → New repository secret**
   - `BLS_API_KEY`, `FRED_API_KEY`

두 키 모두 등록하지 않아도 파이프라인은 정상 동작합니다(World Bank로 자동 폴백).

### 5-4. 워크플로우 동작 확인
- **Actions** 탭에서 `Daily Global Brief Update` 워크플로우 확인 (39개국 병렬 수집이라 수 분 정도 소요될 수 있어 `timeout-minutes: 30`으로 여유를 뒀습니다)
- 최초 1회는 **Run workflow** 버튼으로 수동 실행 → `docs/index.html`이 생성되고 Pages에 배포됩니다
- 이후 매일 **KST 06:00 (UTC 21:00)** 자동 실행됩니다
- 배포 주소: `https://<YOUR_ID>.github.io/<YOUR_REPO>/`

> ⚠️ GitHub Actions의 `schedule` cron은 정확히 정시에 실행되지 않을 수 있으며(수 분~십수 분 지연 가능),
> 저장소에 60일 이상 커밋이 없으면 스케줄이 자동 비활성화됩니다.

---

## 7. 커스터마이징 가이드

- **국가/거점 추가·수정**: `src/collector.py`의 `_RAW_COUNTRIES` 리스트에 `(code, region, name_kr, name_en, currency, flag, hubs)` 튜플만 추가하면 자동으로 대시보드 전 구간(권역 탭·국가 칩·프로필·환율·뉴스·산업동향)에 반영됩니다.
- **권역 구성 변경**: `REGIONS` 리스트 순서/라벨 수정, 국가별 `region` 값만 바꾸면 됨
- **뉴스/산업 키워드 변경**: `get_headlines()`의 쿼리, `INDUSTRY_TOPICS`의 `query_tpl` 수정
- **UI/디자인 변경**: `template.html`의 Tailwind 클래스 및 `<style>` 블록만 수정 (DOM id는 JS와 연결되어 있으므로 id 자체는 유지)
- **차트 툴팁/색상 변경**: `template.html` 하단 `renderChart()` 함수의 Chart.js `options` 수정
- **실행 시각 변경**: `.github/workflows/update.yml`의 `cron: "0 21 * * *"` 값을 UTC 기준으로 수정

---

## 8. 트러블슈팅

| 증상 | 원인/해결 |
|---|---|
| Actions에서 `git push` 실패(권한 오류) | 저장소 Settings → Actions → General → Workflow permissions를 **Read and write permissions**로 변경 |
| 특정 국가만 계속 캐시값 표시 | 해당 국가의 통화 페어가 Yahoo Finance에 없거나(교차 환산도 실패) World Bank가 해당국 지표를 미제공하는 경우 — 로그에서 `[코드]` 태그로 원인 확인 가능 |
| Pages가 404 | Settings → Pages에서 Source가 **GitHub Actions**로 되어 있는지, `deploy-pages` 단계가 성공했는지 확인 |
| 수집이 너무 오래 걸림 | `src/collector.py`의 `MAX_WORKERS` 값을 늘리거나(Rate limit 주의), 뉴스/산업 동향의 `when:` 기간을 넓혀 재시도 횟수를 줄이는 방법 고려 |
| 물가/고용 지표가 갱신되지 않음(미국) | BLS API 일일 호출 한도 초과 가능성 — `BLS_API_KEY`/`FRED_API_KEY` 등록 권장 |
| 국가 칩을 눌러도 화면이 안 바뀜 | 브라우저 콘솔에서 JS 에러 확인 — `dashboard-data` JSON 파싱 실패가 대부분의 원인이므로 `data/countries_data.json`이 유효한 JSON인지 점검 |
| 날짜 피커에서 과거 날짜를 골라도 안 바뀜 | `docs/index.html`을 `file://`로 직접 열면 브라우저가 `fetch()`를 차단합니다. `python -m http.server --directory docs`로 로컬 서버를 띄우고 접속하세요. 배포된 GitHub Pages에서는 정상 동작합니다 |
| 특정 날짜 선택 시 "데이터를 찾을 수 없습니다" 경고 | 해당 날짜에 아직 아카이브가 쌓이지 않았거나(180일 보존 기간 경과로 삭제) `docs/archive/{날짜}.json`이 커밋되지 않은 경우입니다 |
| 뉴스/동향이 전부 영문 번역본만 나옴 | 해당 국가·카테고리 키워드로 최근 기간 내 한국어 기사가 실제로 없는 경우 정상 동작입니다(1순위 한국어 → 2순위 영문+번역 폴백) |
| 번역이 이상하거나 느림 | `deep-translator`의 `GoogleTranslator`는 비공식 무료 엔드포인트라 트래픽이 몰리면 느려지거나 실패할 수 있습니다 — 실패 시 자동으로 원문(영문) 제목이 유지되도록 폴백 처리되어 있습니다 |

---

## 9. 요약 실행 순서 (Cheat Sheet)

```bash
pip install -r requirements.txt
python src/collector.py
python src/build_site.py
```
→ GitHub에 push → Actions 자동 스케줄 등록 → 매일 KST 06:00 `docs/index.html` 자동 갱신 & Pages 배포
