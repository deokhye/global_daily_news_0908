"""
collector.py
------------
Global Daily News — 39개 진출국 데이터 수집기 (v9)

v9 변경점 — GitHub Actions "Process completed with exit code 1" 크래시 수정
  이전 버전은 39개국 병렬 수집 루프 자체는 국가 단위로 안전하게 예외 처리되어 있었지만,
  루프가 끝난 뒤 결과를 파일로 저장하는 `save_cache()` / `save_archive()` 호출에는
  아무런 try-except가 없었다. 만약 39개국 중 단 하나의 값이라도(예: pandas/numpy 스칼라가
  실수로 섞여 들어가는 경우 등) JSON으로 직렬화되지 않으면, `json.dump()`가
  `TypeError`를 던지며 스크립트 전체가 그 자리에서 죽어버렸다 — 이게 바로
  "Process completed with exit code 1"의 가장 유력한 원인이다. 이번 버전에서 다음을
  적용해 이 문제를 근본적으로 차단한다.

    1) json.dump(..., default=str) — 혹시라도 직렬화 불가능한 값이 섞여도 예외를 던지는
       대신 str()로 강제 변환해 저장을 계속 진행한다 (데이터 유실보다 안전).
    2) save_cache()/save_archive() 호출을 main()에서 각각 개별 try-except로 감싸,
       하나가 실패해도 스크립트가 죽지 않고 나머지 작업을 계속하며, 최종적으로
       핵심 산출물(data/countries_data.json)이 정말 생성되지 않은 경우에만 실패로 간주한다.
    3) `if __name__ == "__main__":` 진입점에 최상위 try-except를 두어, 정말로 복구
       불가능한 예외가 발생하면 `log.exception()`으로 전체 스택 트레이스를 GitHub
       Actions 로그에 명확히 남긴 뒤에만 실패를 다시 알린다 — 이제 "exit code 1"만
       보고 원인을 못 찾는 상황 자체가 재발하지 않는다.

핵심 기능 요약
  1) 번역 실패/빈 값 시 원문(영문) 그대로 반환 — 화면에 'Error' 텍스트가 뜨는 일이 없다.
  2) 39개국 수집 루프 전 구간(국가/헤드라인/산업동향/환율/프로필)에 다단계 try-except를
     적용해, 특정 국가·특정 분야가 실패해도 '현지 산업 및 정책 모니터링 중' 같은 안전한
     기본값으로 채우고 다음 국가로 계속 진행한다.
  3) 한국(KR) 기사는 번역 파이프라인 없이 국내 한국어 소스(hl=ko&gl=KR&ceid=KR:ko)에서
     직접 수집한다.
  4) 39개국 전체 최저임금 메타데이터(MIN_WAGE_DATA)를 매핑하고, 미국 전용이었던
     테네시 하드코딩을 제거해 국가별 거점(hubs)이 완전히 분리되어 있음을 보장한다.
  5) VND/JPY/IDR 등 소액 통화는 100단위로 환산하고, 모든 원화 환산값은 소수점
     1자리로 통일 포맷팅한다.
  6) 국가 내 헤드라인(3)+산업동향(4) 전체 구간에서 `seen_titles = set()`으로 중복 기사를
     제거하고, 일별 아카이브(docs/archive/YYYY-MM-DD.json)를 저장하며 180일 초과분은
     자동 삭제한다(Retention Policy).
"""

import os
import re
import json
import logging
from datetime import datetime, timedelta
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import feedparser
import pandas as pd
import pytz

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
log = logging.getLogger("collector")

KST = pytz.timezone("Asia/Seoul")
BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
DATA_DIR = os.path.join(BASE_DIR, "data")
CACHE_PATH = os.path.join(DATA_DIR, "countries_data.json")
ARCHIVE_DIR = os.path.join(BASE_DIR, "docs", "archive")
ARCHIVE_INDEX_PATH = os.path.join(ARCHIVE_DIR, "index.json")
ARCHIVE_RETENTION_DAYS = 180

MONTH_KR = ["1월", "2월", "3월", "4월", "5월", "6월",
            "7월", "8월", "9월", "10월", "11월", "12월"]

BLS_API_KEY = os.environ.get("BLS_API_KEY", "")
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")

REQUEST_TIMEOUT = 10
MAX_WORKERS = 8  # 국가 단위 병렬 수집 스레드 수

# 소액 통화 — 100단위 기준으로 환산해 저장 (1단위 환율이 반올림 시 '0원'이 되는 문제 방지)
SMALL_UNIT_CURRENCIES = {"VND": 100, "JPY": 100, "IDR": 100}

# 원화 환산값(현재가/월별 히스토리) 반올림 자릿수 — 항상 소수점 1자리로 통일 표기
WON_DECIMALS = 1

# 모든 안전 Fallback 문구를 이 상수 하나로 통일 관리 (요청하신 정확한 문구)
FALLBACK_TEXT = "현지 산업 및 정책 모니터링 중"


# ---------------------------------------------------------------------------
# 0. 39개국 메타데이터 & 권역 정의
#    hubs 는 "한국어 설명 (English Name)" 형태로 통일 — 국가별로 완전히 분리된 값이며
#    다른 국가에 테네시 등이 잘못 노출되는 하드코딩 버그가 없음을 명시적으로 보장한다.
# ---------------------------------------------------------------------------
REGIONS = [
    {"key": "ALL", "label": "전체"},
    {"key": "KR", "label": "한국"},
    {"key": "CN", "label": "중국"},
    {"key": "AMER", "label": "미주"},
    {"key": "EU", "label": "유럽"},
    {"key": "LATAM", "label": "중남미"},
    {"key": "APAC", "label": "아태"},
    {"key": "MEA", "label": "중아"},
]

# (code, region, name_kr, name_en, currency, flag, hubs[])
# code 는 World Bank / flagcdn 이 사용하는 ISO 3166-1 alpha-2 기준 (영국=GB)
_RAW_COUNTRIES = [
    ("KR", "KR", "한국", "South Korea", "KRW", "kr",
     ["본사 (Pangyo Technoplex, HQ)", "중앙연구소 (Hankook Technodome, Central R&D)",
      "대전공장 (Daejeon Plant)", "금산공장 (Geumsan Plant)",
      "한국테크노링 (Hankook Technoring, Proving Ground)"]),
    ("CN", "CN", "중국", "China", "CNY", "cn",
     ["중국본부 (Shanghai HQ)", "연구소 (CTC Jiaxing, R&D)", "가흥공장 (Jiaxing Plant)",
      "강소공장 (Jiangsu Plant)", "중경공장 (Chongqing Plant)", "영업지사 11개 (11 Sales Branches)"]),
    ("US", "AMER", "미국", "United States", "USD", "us",
     ["미주본부 (Nashville HQ)", "미국기술센터 (ATC, Akron/Ohio)", "테네시공장 (Clarksville Plant, TN)"]),
    ("CA", "AMER", "캐나다", "Canada", "CAD", "ca",
     ["캐나다 판매법인 (Ontario Sales Corp.)"]),
    ("DE", "EU", "독일", "Germany", "EUR", "de",
     ["유럽본부 (Neu-Isenburg HQ)", "유럽기술센터 (ETC, Hannover)", "독일 판매법인 (Germany Sales Corp.)"]),
    ("HU", "EU", "헝가리", "Hungary", "HUF", "hu",
     ["헝가리공장 (Rácalmás Plant)", "헝가리 판매법인 (Budapest Sales Corp.)"]),
    ("GB", "EU", "영국", "United Kingdom", "GBP", "gb",
     ["영국 판매법인 (Daventry Sales Corp.)"]),
    ("FR", "EU", "프랑스", "France", "EUR", "fr",
     ["프랑스 판매법인 (Lyon Sales Corp.)"]),
    ("IT", "EU", "이탈리아", "Italy", "EUR", "it",
     ["이탈리아 판매법인 (Milan Sales Corp.)"]),
    ("ES", "EU", "스페인", "Spain", "EUR", "es",
     ["스페인 판매법인 (Madrid Sales Corp.)"]),
    ("PL", "EU", "폴란드", "Poland", "PLN", "pl",
     ["폴란드 판매법인 (Warsaw Sales Corp.)"]),
    ("CZ", "EU", "체코", "Czech Republic", "CZK", "cz",
     ["체코 판매법인 (Prague Sales Corp.)"]),
    ("NL", "EU", "네덜란드", "Netherlands", "EUR", "nl",
     ["네덜란드 판매법인 (Amsterdam Sales Corp.)"]),
    ("SE", "EU", "스웨덴", "Sweden", "SEK", "se",
     ["스웨덴 판매법인 (Stockholm Sales Corp.)"]),
    ("AT", "EU", "오스트리아", "Austria", "EUR", "at",
     ["오스트리아 판매법인 (Vienna Sales Corp.)"]),
    ("RO", "EU", "루마니아", "Romania", "RON", "ro",
     ["루마니아 판매법인 (Bucharest Sales Corp.)"]),
    ("RU", "EU", "러시아", "Russia", "RUB", "ru",
     ["러시아 판매법인 (Moscow Sales Corp.)"]),
    ("UA", "EU", "우크라이나", "Ukraine", "UAH", "ua",
     ["우크라이나 지사 (Kyiv Branch)"]),
    ("TR", "EU", "튀르키예", "Turkey", "TRY", "tr",
     ["튀르키예 판매법인 (Istanbul Sales Corp.)"]),
    ("RS", "EU", "세르비아", "Serbia", "RSD", "rs",
     ["세르비아 세일즈 오피스 (Serbia Sales Office)"]),
    ("HR", "EU", "크로아티아", "Croatia", "EUR", "hr",
     ["크로아티아 세일즈 오피스 (Croatia Sales Office)"]),
    ("MA", "EU", "모로코", "Morocco", "MAD", "ma",
     ["모로코 세일즈 오피스 (Casablanca Sales Office)"]),
    ("MX", "LATAM", "멕시코", "Mexico", "MXN", "mx",
     ["멕시코 판매법인 (Mexico City Sales Corp.)"]),
    ("BR", "LATAM", "브라질", "Brazil", "BRL", "br",
     ["브라질 판매법인 (São Paulo Sales Corp.)"]),
    ("CL", "LATAM", "칠레", "Chile", "CLP", "cl",
     ["칠레 판매법인 (Santiago Sales Corp.)"]),
    ("CO", "LATAM", "콜롬비아", "Colombia", "COP", "co",
     ["콜롬비아 지사 (Bogotá Branch)"]),
    ("PA", "LATAM", "파나마", "Panama", "PAB", "pa",
     ["파나마 판매법인 (Panama City Sales Corp.)"]),
    ("ID", "APAC", "인도네시아", "Indonesia", "IDR", "id",
     ["아태본부 (APAC HQ)", "인도네시아공장 (Cikarang Plant)", "인도네시아 판매법인 (Indonesia Sales Corp.)"]),
    ("AU", "APAC", "호주", "Australia", "AUD", "au",
     ["호주 판매법인 (Sydney Sales Corp.)"]),
    ("JP", "APAC", "일본", "Japan", "JPY", "jp",
     ["일본기술센터 (JTC, Japan Technical Center)", "일본 판매법인 (Tokyo Sales Corp.)"]),
    ("SG", "APAC", "싱가포르", "Singapore", "SGD", "sg",
     ["싱가포르 법인 (Singapore Corp.)"]),
    ("MY", "APAC", "말레이시아", "Malaysia", "MYR", "my",
     ["말레이시아 판매법인 (Kuala Lumpur Sales Corp.)"]),
    ("TH", "APAC", "태국", "Thailand", "THB", "th",
     ["태국 판매법인 (Bangkok Sales Corp.)"]),
    ("VN", "APAC", "베트남", "Vietnam", "VND", "vn",
     ["베트남 판매법인 (Ho Chi Minh Sales Corp.)"]),
    ("TW", "APAC", "대만", "Taiwan", "TWD", "tw",
     ["대만 지사 (Taipei Branch)"]),
    ("AE", "MEA", "아랍에미리트", "United Arab Emirates", "AED", "ae",
     ["중동본부 (Dubai HQ)"]),
    ("SA", "MEA", "사우디아라비아", "Saudi Arabia", "SAR", "sa",
     ["사우디 세일즈 오피스 (Jeddah Sales Office)"]),
    ("EG", "MEA", "이집트", "Egypt", "EGP", "eg",
     ["이집트 지사 (Cairo Branch)"]),
    ("KZ", "MEA", "카자흐스탄", "Kazakhstan", "KZT", "kz",
     ["카자흐스탄 지사 (Almaty Branch)"]),
]

COUNTRIES = [
    {"code": c, "region": r, "name_kr": nk, "name_en": ne, "currency": cur, "flag": fl, "hubs": h}
    for c, r, nk, ne, cur, fl, h in _RAW_COUNTRIES
]

CAPITAL_FALLBACK = {"TW": "Taipei"}


# ---------------------------------------------------------------------------
# 39개국 최저임금 참고 데이터 (정적 메타데이터)
# ---------------------------------------------------------------------------
# 최저임금은 국가마다 공시 주기·통화·단위(시급/월급/일급)가 제각각이고 이를 실시간으로
# 통합 제공하는 무료 공식 API가 없다. 따라서 공신력 있는 자료(각국 노동부, Eurostat,
# Trading Economics 등)를 참고해 정기적으로 갱신하는 정적 테이블로 관리한다.
# "note"가 있는 항목은 시행 시점/지역 편차/적용 예외 등 반드시 확인해야 할 참고사항이다.
MIN_WAGE_DATA = {
    "KR": {"display": "10,320원 / 시간", "note": "2026년 기준"},
    "CN": {"display": "약 25위안 / 시간", "note": "지역별 상이(참고치), 상하이 등 대도시 기준"},
    "US": {"display": "$7.25 / 시간", "note": "연방 기준, 2009-07-24 이후 동결·테네시주 별도 기준 없음"},
    "CA": {"display": "약 CAD 17.75 / 시간", "note": "연방부문 기준, 주(州)별로 상이"},
    "DE": {"display": "€13.90 / 시간", "note": "2026년 기준"},
    "HU": {"display": "약 290,800포린트 / 월", "note": "비숙련 기준, 2025년 기준"},
    "GB": {"display": "£12.71 / 시간", "note": "National Living Wage(21세 이상), 2025~26년 기준"},
    "FR": {"display": "€12.02 / 시간", "note": "SMIC 기준"},
    "IT": {"display": "법정 최저임금 없음", "note": "업종별 단체협약(CBA)으로 결정"},
    "ES": {"display": "약 €1,381 / 월", "note": "연 14회 분할 지급 관행, 2025.1 Eurostat 기준"},
    "PL": {"display": "zł31.4 / 시간", "note": "2026년 기준"},
    "CZ": {"display": "약 20,800코루나 / 월", "note": "2025년 기준"},
    "NL": {"display": "€14.71 / 시간", "note": "2026년 기준"},
    "SE": {"display": "법정 최저임금 없음", "note": "업종별 단체협약(CBA)으로 결정"},
    "AT": {"display": "법정 최저임금 없음", "note": "업종별 단체협약(CBA)으로 결정"},
    "RO": {"display": "약 €814 / 월", "note": "2025.1 Eurostat 기준"},
    "RU": {"display": "약 22,440루블 / 월", "note": "MROT 기준, 2025년"},
    "UA": {"display": "약 8,000흐리브냐 / 월", "note": "2025년 기준"},
    "TR": {"display": "₺164.94 / 시간", "note": "2026년 기준"},
    "RS": {"display": "약 308디나르 / 시간", "note": "참고치"},
    "HR": {"display": "약 €970 / 월", "note": "2025.1 Eurostat 기준, 2023년부터 유로 사용"},
    "MA": {"display": "약 17.07디르함 / 시간", "note": "참고치"},
    "MX": {"display": "약 9,583.52페소 / 월", "note": "일반지역 기준, 2026년, 국경지대는 더 높음"},
    "BR": {"display": "약 1,631헤알 / 월", "note": "2026년 기준(참고치), 13번째 급여 별도 지급 관행"},
    "CL": {"display": "약 539,000페소 / 월", "note": "2025년 기준(참고치)"},
    "CO": {"display": "1,423,500페소 / 월", "note": "2025년 기준"},
    "PA": {"display": "업종별 상이", "note": "참고치, 업종·지역별 세분화된 고시"},
    "ID": {"display": "약 5,396,761루피아 / 월", "note": "자카르타(DKI) 기준, 지역별 상이"},
    "AU": {"display": "A$26.44 / 시간", "note": "2025~26년 기준"},
    "JP": {"display": "¥1,121 / 시간", "note": "2026년 전국 가중평균, 지역별 상이(도쿄 ¥1,226)"},
    "SG": {"display": "법정 최저임금 없음", "note": "Progressive Wage Model로 업종별 하한선 운영"},
    "MY": {"display": "RM8.72 / 시간", "note": "월 RM1,700 기준, 2025.2 시행"},
    "TH": {"display": "1일 400바트", "note": "2025년 기준, 지역별 상이"},
    "VN": {"display": "약 5,310,000동 / 월", "note": "1지역(하노이·호치민 등) 기준, 2026.1, 지역별 상이"},
    "TW": {"display": "NT$190 / 시간", "note": "월 NT$29,500, 2026년 기준"},
    "AE": {"display": "법정 최저임금 없음", "note": "-"},
    "SA": {"display": "SAR23.08 / 시간", "note": "공공부문 기준, 외국인 근로자 미적용"},
    "EG": {"display": "약 7,000파운드 / 월", "note": "공공부문 기준(참고치)"},
    "KZ": {"display": "약 85,000텐게 / 월", "note": "2025년 기준"},
}


# ---------------------------------------------------------------------------
# 유틸
# ---------------------------------------------------------------------------
def load_cache() -> dict:
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH, "r", encoding="utf-8") as f:
                payload = json.load(f)
            return {c["code"]: c for c in payload.get("countries", [])}
        except Exception as e:
            log.warning(f"캐시 로드 실패: {e}")
    return {}


def save_cache(data: dict) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        # default=str: 혹시라도 JSON으로 직렬화할 수 없는 값이 섞여도 예외로 죽지 않고
        # 문자열로 강제 변환해 저장을 계속한다 (exit code 1 크래시의 가장 유력한 원인 차단).
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)


def save_archive(data: dict) -> None:
    """docs/archive/YYYY-MM-DD.json 저장 + index.json 갱신 + 180일 보존정책 적용."""
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    date_str = data["generated_at"][:10]  # KST 기준 YYYY-MM-DD

    archive_path = os.path.join(ARCHIVE_DIR, f"{date_str}.json")
    with open(archive_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, default=str)  # 용량 절약을 위해 압축(무들여쓰기) 저장

    _apply_retention_and_reindex()


def _apply_retention_and_reindex() -> None:
    cutoff = datetime.now(KST).date() - timedelta(days=ARCHIVE_RETENTION_DAYS)
    valid_dates = []

    try:
        archive_files = os.listdir(ARCHIVE_DIR)
    except Exception as e:
        log.warning(f"[retention] 아카이브 디렉토리 조회 실패, 보존정책 이번 회차는 건너뜀: {e}")
        return

    for fname in archive_files:
        if fname == "index.json" or not fname.endswith(".json"):
            continue
        date_part = fname[:-5]
        try:
            d = datetime.strptime(date_part, "%Y-%m-%d").date()
        except ValueError:
            continue  # 형식에 안 맞는 파일은 건드리지 않음

        if d < cutoff:
            try:
                os.remove(os.path.join(ARCHIVE_DIR, fname))
                log.info(f"[retention] {fname} 삭제 (180일 경과)")
            except Exception as e:
                log.warning(f"[retention] {fname} 삭제 실패: {e}")
            continue

        valid_dates.append(date_part)

    valid_dates.sort()
    index_payload = {
        "dates": valid_dates,
        "min_date": valid_dates[0] if valid_dates else None,
        "latest": valid_dates[-1] if valid_dates else None,
    }
    try:
        with open(ARCHIVE_INDEX_PATH, "w", encoding="utf-8") as f:
            json.dump(index_payload, f, ensure_ascii=False, indent=2, default=str)
    except Exception as e:
        log.warning(f"[retention] index.json 저장 실패: {e}")


def _strip_html(text: str) -> str:
    try:
        return re.sub(r"<[^>]+>", "", text or "").strip()
    except Exception:
        return ""


def _shorten(text: str, max_len: int) -> str:
    try:
        text = " ".join((text or "").split())
        return text if len(text) <= max_len else text[:max_len].rstrip() + "…"
    except Exception:
        return text or ""


def _safe_get(url: str, **kwargs):
    resp = requests.get(url, timeout=REQUEST_TIMEOUT, **kwargs)
    resp.raise_for_status()
    return resp


def _round_won(value: float) -> float:
    """원화 환산값을 소수점 1자리로 통일 반올림."""
    try:
        return round(float(value), WON_DECIMALS)
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# 1. World Bank — 수도 / 인구 / GDP / (기본) 인플레이션·실업률
# ---------------------------------------------------------------------------
def _wb_country_meta(code: str) -> str:
    url = f"https://api.worldbank.org/v2/country/{code}?format=json"
    payload = _safe_get(url).json()
    row = payload[1][0]
    capital = row.get("capitalCity") or ""
    if not capital:
        raise ValueError("수도 정보 없음")
    return capital


def _wb_latest_value(code: str, indicator: str):
    url = f"https://api.worldbank.org/v2/country/{code}/indicator/{indicator}?format=json&per_page=5"
    payload = _safe_get(url).json()
    for row in payload[1]:
        if row.get("value") is not None:
            return float(row["value"]), row["date"]
    raise ValueError("유효 데이터 없음")


# ---------------------------------------------------------------------------
# 2. 미국 전용 — 인플레이션·실업률 BLS → FRED → World Bank 폴백
# ---------------------------------------------------------------------------
def _bls_series(series_ids: list, start_year: int, end_year: int) -> dict:
    url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    payload = {"seriesid": series_ids, "startyear": str(start_year), "endyear": str(end_year)}
    if BLS_API_KEY:
        payload["registrationkey"] = BLS_API_KEY
    resp = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    body = resp.json()
    if body.get("status") != "REQUEST_SUCCEEDED":
        raise ValueError("BLS API 오류")
    return {s["seriesID"]: s["data"] for s in body["Results"]["series"]}


def _us_labor_stats_bls() -> dict:
    year = datetime.now(KST).year
    series = {"unemployment": "LNS14000000", "cpi": "CUUR0000SA0"}
    data = _bls_series(list(series.values()), year - 1, year)
    unemployment = float(data[series["unemployment"]][0]["value"])
    cpi_points = data[series["cpi"]]
    latest = cpi_points[0]
    prior = next(p for p in cpi_points if p["period"] == latest["period"] and int(p["year"]) == int(latest["year"]) - 1)
    inflation = (float(latest["value"]) - float(prior["value"])) / float(prior["value"]) * 100
    return {"inflation": f"{inflation:.1f}%", "unemployment": f"{unemployment:.1f}%",
            "stats_source": "BLS", "stats_asof": f"{latest['year']}-{latest['period'].replace('M', '')}"}


def _us_labor_stats_fred() -> dict:
    if not FRED_API_KEY:
        raise RuntimeError("FRED_API_KEY 미설정")
    url = "https://api.stlouisfed.org/fred/series/observations"

    def obs(series_id, limit):
        params = {"series_id": series_id, "api_key": FRED_API_KEY, "file_type": "json",
                   "sort_order": "desc", "limit": limit}
        return _safe_get(url, params=params).json()["observations"]

    unemployment = float(obs("UNRATE", 1)[0]["value"])
    cpi = obs("CPIAUCSL", 13)
    inflation = (float(cpi[0]["value"]) - float(cpi[-1]["value"])) / float(cpi[-1]["value"]) * 100
    return {"inflation": f"{inflation:.1f}%", "unemployment": f"{unemployment:.1f}%",
            "stats_source": "FRED", "stats_asof": cpi[0]["date"][:7]}


def get_inflation_unemployment(code: str, cached_profile: dict) -> dict:
    if code == "US":
        for label, fn in (("BLS", _us_labor_stats_bls), ("FRED", _us_labor_stats_fred)):
            try:
                return fn()
            except Exception as e:
                log.warning(f"[US/{label}] 물가·고용 지표 실패: {e}")
    try:
        infl, infl_date = _wb_latest_value(code, "FP.CPI.TOTL.ZG")
        unemp, _ = _wb_latest_value(code, "SL.UEM.TOTL.ZS")
        return {"inflation": f"{infl:.1f}%", "unemployment": f"{unemp:.1f}%",
                "stats_source": "World Bank", "stats_asof": infl_date}
    except Exception as e:
        log.warning(f"[{code}/World Bank] 물가·고용 지표 실패 → 캐시 사용: {e}")
        return {
            "inflation": cached_profile.get("inflation", "-"),
            "unemployment": cached_profile.get("unemployment", "-"),
            "stats_source": cached_profile.get("stats_source", "캐시"),
            "stats_asof": cached_profile.get("stats_asof", "-"),
        }


def build_profile(country: dict, cached: dict) -> dict:
    code = country["code"]
    cached_profile = (cached or {}).get("profile", {})
    profile = {}

    try:
        profile["capital"] = _wb_country_meta(code)
    except Exception as e:
        log.warning(f"[{code}] 수도 정보 실패: {e}")
        profile["capital"] = CAPITAL_FALLBACK.get(code, cached_profile.get("capital", "-"))

    try:
        pop, _ = _wb_latest_value(code, "SP.POP.TOTL")
        profile["population"] = f"약 {pop / 1e8:.2f}억 명" if pop >= 1e8 else f"약 {pop / 1e4:.0f}만 명"
    except Exception as e:
        log.warning(f"[{code}] 인구 지표 실패 → 캐시 사용: {e}")
        profile["population"] = cached_profile.get("population", "-")

    try:
        gdp, _ = _wb_latest_value(code, "NY.GDP.MKTP.CD")
        profile["gdp"] = f"약 {gdp / 1e12:.2f}조 USD" if gdp >= 1e12 else f"약 {gdp / 1e9:.1f}십억 USD"
    except Exception as e:
        log.warning(f"[{code}] GDP 지표 실패 → 캐시 사용: {e}")
        profile["gdp"] = cached_profile.get("gdp", "-")

    try:
        profile.update(get_inflation_unemployment(code, cached_profile))
    except Exception as e:
        log.warning(f"[{code}] 물가·고용 지표 처리 중 예기치 못한 오류 → 캐시 사용: {e}")
        profile["inflation"] = cached_profile.get("inflation", "-")
        profile["unemployment"] = cached_profile.get("unemployment", "-")
        profile["stats_source"] = cached_profile.get("stats_source", "캐시")
        profile["stats_asof"] = cached_profile.get("stats_asof", "-")

    # 39개국 전체 최저임금 매핑 (미국 전용 하드코딩 제거)
    try:
        wage = MIN_WAGE_DATA.get(code)
        if wage:
            profile["min_wage"] = wage["display"]
            profile["min_wage_note"] = wage.get("note", "")
        else:
            log.warning(f"[{code}] 최저임금 메타데이터 누락 — MIN_WAGE_DATA 테이블 점검 필요")
            profile["min_wage"] = cached_profile.get("min_wage", "-")
            profile["min_wage_note"] = cached_profile.get("min_wage_note", "")
    except Exception as e:
        log.warning(f"[{code}] 최저임금 처리 중 예기치 못한 오류: {e}")
        profile["min_wage"] = cached_profile.get("min_wage", "-")
        profile["min_wage_note"] = cached_profile.get("min_wage_note", "")

    return profile


# ---------------------------------------------------------------------------
# 3. 환율 (현지통화 / KRW) — 일별 종가 기반 수집 + 자체 월별 리샘플 + 소수점 1자리 통일
# ---------------------------------------------------------------------------
MIN_RELIABLE_TRADING_DAYS = 30  # 이보다 적으면 직접 페어를 신뢰하지 않고 교차 환산으로 전환


def _yf_daily_close(ticker: str, days: int = 400) -> pd.Series:
    try:
        import yfinance as yf
        hist = yf.Ticker(ticker).history(period=f"{days}d", interval="1d")
        if hist.empty:
            return pd.Series(dtype="float64")
        return hist["Close"].dropna()
    except Exception as e:
        log.warning(f"[yfinance] {ticker} 조회 실패: {e}")
        return pd.Series(dtype="float64")


def _monthly_from_daily(daily: pd.Series, months: int = 12):
    """일별 종가 시리즈 → (연,월) 그룹의 마지막 종가로 월별 시리즈 생성, 최근 months개월 반환."""
    if daily.empty:
        return [], []
    try:
        grouped = daily.groupby(daily.index.to_period("M")).last().tail(months)
        labels = [str(MONTH_KR[int(p.month) - 1]) for p in grouped.index]
        values = [_round_won(v) for v in grouped]
        return labels, values
    except Exception as e:
        log.warning(f"월별 리샘플 실패: {e}")
        return [], []


def _direct_pair_series(currency: str) -> pd.Series:
    return _yf_daily_close(f"{currency}KRW=X")


def _cross_pair_series(currency: str) -> pd.Series:
    """직접 페어가 부실한 통화는 USD 경유 교차 환산: CUR/KRW = (USD당 1CUR 값) × (KRW당 1USD 값)."""
    cur_usd = _yf_daily_close(f"{currency}USD=X")
    usd_krw = _yf_daily_close("USDKRW=X")
    if cur_usd.empty or usd_krw.empty:
        return pd.Series(dtype="float64")
    try:
        combined = pd.DataFrame({"cur_usd": cur_usd, "usd_krw": usd_krw}).sort_index().ffill().dropna()
        if combined.empty:
            return pd.Series(dtype="float64")
        return combined["cur_usd"] * combined["usd_krw"]
    except Exception as e:
        log.warning(f"교차 환산 실패: {e}")
        return pd.Series(dtype="float64")


def get_exchange_rate(country: dict, cached: dict) -> dict:
    currency = country["currency"]
    cached_fx = (cached or {}).get("exchange_rate", {})
    unit_base = SMALL_UNIT_CURRENCIES.get(currency, 1)

    if currency == "KRW":
        return {"is_base": True, "unit_base": 1, "current_rate": 1.0, "change_pct": 0.0,
                "history_labels": [], "history_values": [], "source": "기준통화"}

    try:
        daily = _direct_pair_series(currency)
        source = "Yahoo Finance"
        if len(daily) < MIN_RELIABLE_TRADING_DAYS:
            log.info(f"[{country['code']}] 직접 페어 데이터 부실({len(daily)}일) → 교차 환산으로 전환")
            cross = _cross_pair_series(currency)
            if len(cross) >= len(daily):
                daily, source = cross, "Yahoo Finance (USD 교차 환산)"

        if daily.empty:
            raise ValueError("일별 환율 데이터 없음(직접/교차 모두 실패)")

        current_rate_raw = float(daily.iloc[-1])
        change_pct = (
            round((current_rate_raw - float(daily.iloc[-2])) / float(daily.iloc[-2]) * 100, 2)
            if len(daily) >= 2 else 0.0
        )
        history_labels, history_values_raw = _monthly_from_daily(daily, months=12)
        if not history_labels:
            raise ValueError("월별 히스토리 생성 실패")

        return {
            "is_base": False,
            "unit_base": unit_base,
            "current_rate": _round_won(current_rate_raw * unit_base),
            "change_pct": change_pct,  # 비율이므로 단위 환산의 영향을 받지 않음
            "history_labels": history_labels,
            "history_values": [_round_won(v * unit_base) for v in history_values_raw],
            "source": source,
        }
    except Exception as e:
        log.warning(f"[{country['code']}] 환율 수집 실패 → 캐시/기본값 사용: {e}")
        if cached_fx:
            return cached_fx
        return {"is_base": False, "unit_base": unit_base, "current_rate": 0.0, "change_pct": 0.0,
                "history_labels": [], "history_values": [], "source": "수집 실패"}


# ---------------------------------------------------------------------------
# 4. 자동 번역 (영문 기사를 한국어로 표출할 때 사용, KR 국가는 호출되지 않음)
#    실패하거나 빈 값이 나오면 절대 Error 텍스트를 만들지 않고 원문(영문)을 그대로 반환한다.
# ---------------------------------------------------------------------------
@lru_cache(maxsize=2048)
def translate_to_ko(text: str) -> str:
    """번역 실패/빈 값/예외 발생 시 수집된 영문 원문을 그대로 반환한다 (Error 텍스트 절대 금지)."""
    original = text if isinstance(text, str) else ("" if text is None else str(text))
    if not original.strip():
        return original
    try:
        from deep_translator import GoogleTranslator
        translated = GoogleTranslator(source="auto", target="ko").translate(original)
        if not translated or not str(translated).strip():
            # 번역 결과가 비어 있으면 원문을 그대로 사용
            return original
        return translated
    except Exception as e:
        log.warning(f"번역 실패, 원문(영문) 그대로 사용: {e}")
        return original


# 이전 버전과의 호환을 위한 별칭 (내부적으로 동일 함수 사용)
_translate_to_ko = translate_to_ko


# ---------------------------------------------------------------------------
# 5. 뉴스 공통 유틸 — Google News RSS
# ---------------------------------------------------------------------------
def _google_news_url_ko(query: str) -> str:
    return f"https://news.google.com/rss/search?q={requests.utils.quote(query)}&hl=ko&gl=KR&ceid=KR:ko"


def _google_news_top_url_ko() -> str:
    """국내 주요 언론사 상위 헤드라인 (검색어 없이 Google News 한국어 기본 피드)."""
    return "https://news.google.com/rss?hl=ko&gl=KR&ceid=KR:ko"


def _google_news_url_en(query: str) -> str:
    return f"https://news.google.com/rss/search?q={requests.utils.quote(query)}&hl=en-US&gl=US&ceid=US:en"


def _google_news_search_link(query_en: str) -> str:
    """수집 실패 시 화면에 노출할 공식 Google News 검색 링크(항상 유효)."""
    try:
        return f"https://news.google.com/search?q={requests.utils.quote(query_en)}&hl=en-US&gl=US&ceid=US:en"
    except Exception:
        return "https://news.google.com/"


def _parse_entry(entry) -> dict:
    """개별 기사 항목 파싱. 실패해도 예외를 위로 던져 호출부에서 해당 기사만 건너뛰게 한다."""
    title_raw = getattr(entry, "title", "") or ""
    link = getattr(entry, "link", "") or ""
    if not title_raw or not link:
        raise ValueError("title 또는 link 누락")

    source = ""
    try:
        if hasattr(entry, "source"):
            source = getattr(entry.source, "title", "") or ""
        elif " - " in title_raw:
            source = title_raw.split(" - ")[-1]
    except Exception:
        source = ""

    summary = _strip_html(getattr(entry, "summary", ""))
    title = title_raw.split(" - ")[0] if " - " in title_raw else title_raw

    return {
        "title": title.strip(),
        "source": (source or "Google News").strip(),
        "link": link.strip(),
        "summary": _shorten(summary, 90) if summary else "",
    }


def _fetch_feed(url: str, limit: int) -> list:
    """RSS를 파싱하되, 개별 기사 파싱 실패는 건너뛰고 유효한 기사만 최대 limit개 모은다."""
    try:
        feed = feedparser.parse(url)
    except Exception as e:
        log.warning(f"피드 요청/파싱 실패: {e}")
        return []

    items = []
    try:
        entries = getattr(feed, "entries", []) or []
    except Exception as e:
        log.warning(f"피드 엔트리 접근 실패: {e}")
        return []

    # 파싱 실패분/중복 제거를 감안해 넉넉히 훑되, 필요한 개수만 채우면 즉시 중단
    for entry in entries[: max(limit * 4, limit)]:
        try:
            items.append(_parse_entry(entry))
        except Exception as e:
            log.warning(f"개별 기사 파싱 실패, 건너뜀: {e}")
            continue
        if len(items) >= limit:
            break
    return items


def _dedup_key(item: dict) -> str:
    try:
        link = (item.get("link") or "").strip().lower()
        if link:
            return link
        return (item.get("title") or "").strip().lower()
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# 6. 현지 주요 뉴스 (Local Headlines)
#    — KR: 100% 순수 한국어 (번역 파이프라인 없음, hl=ko&gl=KR)
#    — 그 외 38개국: 정확도 우선 글로벌 영문 검색 → 한국어로 100% 자동 번역
#      (번역 실패 시 원문 영문 그대로 노출, 절대 Error 텍스트 없음)
# ---------------------------------------------------------------------------
def _fallback_headline_item(country: dict) -> dict:
    try:
        query_en = country.get("name_en", "")
        link = _google_news_search_link(query_en)
        name_kr = country.get("name_kr", "")
    except Exception:
        link = "https://news.google.com/"
        name_kr = ""
    return {
        "title": FALLBACK_TEXT,
        "source": "Google News 검색",
        "link": link,
        "summary": f"{name_kr} 관련 최신 기사를 찾지 못해 공식 Google 뉴스 검색 결과로 연결됩니다." if name_kr else "관련 최신 기사를 찾지 못해 공식 Google 뉴스 검색 결과로 연결됩니다.",
        "lang": "ko",
        "translated": False,
        "original_title": "",
    }


def _get_headlines_kr_native(country: dict, cached: dict, seen_titles: set) -> list:
    """한국(KR) 전용 — 번역 없이 국내 언론사 한국어 기사를 그대로 수집 (hl=ko&gl=KR&ceid=KR:ko)."""
    limit = 3
    selected = []
    try:
        candidates = _fetch_feed(_google_news_top_url_ko(), limit=limit * 8)
        for item in candidates:
            try:
                key = _dedup_key(item)
                if not key or key in seen_titles:
                    continue
                item["lang"] = "ko"
                item["translated"] = False
                item["original_title"] = ""
                selected.append(item)
                seen_titles.add(key)
                if len(selected) >= limit:
                    break
            except Exception as e:
                log.warning(f"[KR] 개별 기사 처리 실패, 건너뜀: {e}")
                continue
    except Exception as e:
        log.warning(f"[KR] 국내 뉴스 수집 실패: {e}")

    if selected:
        return selected

    try:
        cached_headlines = (cached or {}).get("headlines", [])
    except Exception:
        cached_headlines = []
    if cached_headlines:
        return cached_headlines
    return [_fallback_headline_item(country)]


def _get_headlines_global_translated(country: dict, cached: dict, seen_titles: set) -> list:
    """해외 38개국 — 정확도 우선 글로벌 영문 검색 → 항상 한국어로 번역해 표출.
    번역이 실패하거나 빈 값이면 영문 원문을 그대로 사용한다(절대 Error 텍스트 없음)."""
    limit = 3
    selected = []
    try:
        candidates = _fetch_feed(_google_news_url_en(f"{country['name_en']} when:3d"), limit=limit * 8)
        for item in candidates:
            try:
                key = _dedup_key(item)
                if not key or key in seen_titles:
                    continue
                original_title = item.get("title", "")
                original_summary = item.get("summary", "")
                try:
                    item["title"] = translate_to_ko(original_title)
                    item["summary"] = translate_to_ko(original_summary) if original_summary else ""
                    item["lang"] = "en"
                    item["translated"] = (item["title"] != original_title)
                    item["original_title"] = original_title
                except Exception as e:
                    # 번역 단계 자체에서 예기치 못한 예외가 나도 원문을 그대로 사용
                    log.warning(f"[{country['code']}] 헤드라인 번역 실패, 원문 유지: {e}")
                    item["title"] = original_title
                    item["summary"] = original_summary
                    item["lang"] = "en"
                    item["translated"] = False
                    item["original_title"] = original_title
                selected.append(item)
                seen_titles.add(key)
                if len(selected) >= limit:
                    break
            except Exception as e:
                log.warning(f"[{country['code']}] 개별 기사 처리 실패, 건너뜀: {e}")
                continue
    except Exception as e:
        log.warning(f"[{country['code']}] 현지 뉴스 수집 실패: {e}")

    if selected:
        return selected

    try:
        cached_headlines = (cached or {}).get("headlines", [])
    except Exception:
        cached_headlines = []
    if cached_headlines:
        return cached_headlines

    # 캐시조차 없는 완전 최초 실행 실패 상황 — 절대 빈 화면/Error 텍스트를 남기지 않는다.
    return [_fallback_headline_item(country)]


def get_headlines(country: dict, cached: dict, seen_titles: set) -> list:
    try:
        if country.get("code") == "KR":
            return _get_headlines_kr_native(country, cached, seen_titles)
        return _get_headlines_global_translated(country, cached, seen_titles)
    except Exception as e:
        log.error(f"[{country.get('code')}] get_headlines 최상위 예외, 안전 Fallback 사용: {e}")
        cached_headlines = (cached or {}).get("headlines", [])
        return cached_headlines if cached_headlines else [_fallback_headline_item(country)]


# ---------------------------------------------------------------------------
# 7. 주요 산업 및 비즈니스 동향 — 한국어 우선 → 영문 대체 + 번역 (기존 전략 유지)
#    AUTO MARKET / HR & LABOR / ECONOMY / MANAGEMENT — 4개 슬롯은 항상 채워진다.
#    (멕시코 HR, 베트남 등 특정 국가/분야가 실패해도 FALLBACK_TEXT로 안전하게 채움)
# ---------------------------------------------------------------------------
INDUSTRY_TOPICS = [
    {
        "category": "AUTO MARKET", "tag": "완성차·타이어 시장", "tag_class": "bg-rose-100 text-rose-700",
        "query_en_tpl": "{name_en} auto OEM tire market EV demand",
        "query_kr_tpl": "{name_kr} 완성차 타이어 시장 전기차 수요",
    },
    {
        "category": "HR & LABOR", "tag": "노동법·인력", "tag_class": "bg-indigo-100 text-indigo-700",
        "query_en_tpl": "{name_en} labor law manufacturing wages hiring",
        "query_kr_tpl": "{name_kr} 노동법 제조업 임금 채용",
    },
    {
        "category": "ECONOMY", "tag": "경기·금리", "tag_class": "bg-emerald-100 text-emerald-700",
        "query_en_tpl": "{name_en} economy interest rate manufacturing PMI outlook",
        "query_kr_tpl": "{name_kr} 경제 기준금리 제조업 PMI 전망",
    },
    {
        "category": "MANAGEMENT", "tag": "관세·공급망", "tag_class": "bg-amber-100 text-amber-700",
        "query_en_tpl": "{name_en} tariff trade policy supply chain logistics cost",
        "query_kr_tpl": "{name_kr} 관세 통상 정책 공급망 물류비",
    },
]


def _fallback_trend_item(spec: dict, country: dict) -> dict:
    try:
        query_en = spec["query_en_tpl"].format(name_en=country.get("name_en", ""))
        link = _google_news_search_link(query_en)
    except Exception:
        link = "https://news.google.com/"
    return {
        "category": spec.get("category", ""),
        "tag": spec.get("tag", ""),
        "tag_class": spec.get("tag_class", "bg-slate-100 text-slate-700"),
        "title": FALLBACK_TEXT,
        "desc": f"{spec.get('tag', '')} 관련 최신 기사를 찾지 못해 공식 Google 뉴스 검색 결과로 연결됩니다. 다음 갱신 시 자동으로 업데이트됩니다.",
        "source": "Google News 검색",
        "link": link,
        "lang": "ko",
        "translated": False,
        "original_title": "",
    }


def get_localized_items(query_kr: str, query_en: str, limit: int, when_filter: str, seen_titles: set) -> list:
    """1순위 한국어 검색 → 부족한 슬롯만 2순위 영문 검색 + 자동 번역으로 채운다 (중복 제거 포함).
    번역 실패 시에는 절대 Error 텍스트를 만들지 않고 영문 원문을 그대로 사용한다."""
    results = []

    try:
        kr_candidates = _fetch_feed(_google_news_url_ko(f"{query_kr} {when_filter}"), limit=limit * 8)
        for it in kr_candidates:
            try:
                key = _dedup_key(it)
                if not key or key in seen_titles:
                    continue
                it["lang"] = "ko"
                it["translated"] = False
                it["original_title"] = ""
                results.append(it)
                seen_titles.add(key)
                if len(results) >= limit:
                    break
            except Exception as e:
                log.warning(f"[KO 검색] 개별 기사 처리 실패, 건너뜀: {e}")
                continue
    except Exception as e:
        log.warning(f"[KO 검색] 실패 ({query_kr}): {e}")

    remaining = limit - len(results)
    if remaining > 0:
        try:
            en_candidates = _fetch_feed(_google_news_url_en(f"{query_en} {when_filter}"), limit=remaining * 8)
            for it in en_candidates:
                try:
                    key = _dedup_key(it)
                    if not key or key in seen_titles:
                        continue
                    original_title = it.get("title", "")
                    original_summary = it.get("summary", "")
                    try:
                        it["title"] = translate_to_ko(original_title)
                        it["summary"] = translate_to_ko(original_summary) if original_summary else ""
                        it["lang"] = "en"
                        it["translated"] = (it["title"] != original_title)
                        it["original_title"] = original_title
                    except Exception as e:
                        # 번역 단계에서 무슨 일이 있어도 원문(영문)을 그대로 사용
                        log.warning(f"번역 단계 실패, 원문(영문) 그대로 사용: {e}")
                        it["title"] = original_title
                        it["summary"] = original_summary
                        it["lang"] = "en"
                        it["translated"] = False
                        it["original_title"] = original_title
                    results.append(it)
                    seen_titles.add(key)
                    if len(results) >= limit:
                        break
                except Exception as e:
                    log.warning(f"[EN 대체 검색] 개별 기사 처리 실패, 건너뜀: {e}")
                    continue
        except Exception as e:
            log.warning(f"[EN 대체 검색] 실패 ({query_en}): {e}")

    return results


def get_industry_trends(country: dict, cached: dict, seen_titles: set) -> list:
    cached_by_category = {t.get("category"): t for t in (cached or {}).get("hr_trends", [])}
    trends = []

    for spec in INDUSTRY_TOPICS:
        item = None
        try:
            query_kr = spec["query_kr_tpl"].format(name_kr=country.get("name_kr", ""))
            query_en = spec["query_en_tpl"].format(name_en=country.get("name_en", ""))
            picked = get_localized_items(query_kr, query_en, limit=1, when_filter="when:14d", seen_titles=seen_titles)
            if picked:
                item = picked[0]
        except Exception as e:
            log.warning(f"[{country.get('code')}] '{spec.get('category')}' 산업 동향 수집 예외: {e}")
            item = None

        if item:
            try:
                trends.append({
                    "category": spec["category"],
                    "tag": spec["tag"],
                    "tag_class": spec["tag_class"],
                    "title": _shorten(item.get("title", ""), 46),
                    "desc": item.get("summary") or _shorten(item.get("title", ""), 70),
                    "source": item.get("source", "Google News"),
                    "link": item.get("link", ""),
                    "lang": item.get("lang", "ko"),
                    "translated": item.get("translated", False),
                    "original_title": item.get("original_title", ""),
                })
                continue
            except Exception as e:
                log.warning(f"[{country.get('code')}] '{spec.get('category')}' 결과 가공 실패: {e}")

        # 여기 도달하면: 검색 실패 / 결과 없음 / 가공 실패
        # (예: 멕시코 HR & LABOR, 베트남 등) — 절대 빈 칸이나 Error를 남기지 않고
        # 캐시가 있으면 캐시, 없으면 안전한 FALLBACK_TEXT 항목으로 채운 뒤 다음 국가로 계속 진행한다.
        try:
            cached_item = cached_by_category.get(spec["category"])
        except Exception:
            cached_item = None
        trends.append(cached_item if cached_item else _fallback_trend_item(spec, country))

    return trends


# ---------------------------------------------------------------------------
# 8. 국가 단위 수집 오케스트레이션
#    이 함수 내부의 각 단계는 모두 개별 try-except로 감싸여 있어, 특정 국가의 특정
#    분야(프로필/환율/헤드라인/산업동향) 중 어느 하나가 실패해도 해당 분야만 안전한
#    기본값으로 대체되고, 스레드풀 전체가 죽지 않고 다음 국가로 계속 진행된다.
# ---------------------------------------------------------------------------
def collect_country(country: dict, cache_by_code: dict) -> dict:
    code = country["code"]
    cached = cache_by_code.get(code, {})
    log.info(f"[{code}] 수집 시작 — {country['name_kr']}")

    # 국가 내 헤드라인(3) + 산업동향(4) 전체 구간에서 기사가 중복 채택되지 않도록 공유
    seen_titles: set = set()

    result = dict(country)

    try:
        result["profile"] = build_profile(country, cached)
    except Exception as e:
        log.error(f"[{code}] 프로필 수집 실패, 캐시/빈 값으로 대체 후 계속 진행: {e}")
        result["profile"] = (cached or {}).get("profile", {})

    try:
        result["exchange_rate"] = get_exchange_rate(country, cached)
    except Exception as e:
        log.error(f"[{code}] 환율 수집 실패, 캐시/기본값으로 대체 후 계속 진행: {e}")
        result["exchange_rate"] = (cached or {}).get(
            "exchange_rate",
            {"is_base": False, "unit_base": 1, "current_rate": 0.0, "change_pct": 0.0,
             "history_labels": [], "history_values": [], "source": "수집 실패"},
        )

    try:
        result["headlines"] = get_headlines(country, cached, seen_titles)
    except Exception as e:
        log.error(f"[{code}] 헤드라인 수집 실패, Fallback 문구로 대체 후 계속 진행: {e}")
        cached_headlines = (cached or {}).get("headlines", [])
        result["headlines"] = cached_headlines if cached_headlines else [_fallback_headline_item(country)]

    try:
        result["hr_trends"] = get_industry_trends(country, cached, seen_titles)
    except Exception as e:
        log.error(f"[{code}] 산업 동향 수집 실패, Fallback 문구로 대체 후 계속 진행: {e}")
        cached_trends = (cached or {}).get("hr_trends", [])
        result["hr_trends"] = cached_trends if cached_trends else [
            _fallback_trend_item(spec, country) for spec in INDUSTRY_TOPICS
        ]

    log.info(f"[{code}] 수집 완료")
    return result


def _build_country_fallback(country_code: str) -> dict:
    """국가 단위 수집이 스레드 자체에서 완전히 실패했을 때 사용하는 최종 안전망."""
    fallback_meta = next((c for c in COUNTRIES if c["code"] == country_code), None)
    if fallback_meta is None:
        # 이론상 발생할 수 없지만(항상 COUNTRIES에서 파생된 코드), 방어적으로 최소 골격 반환
        fallback_meta = {"code": country_code, "region": "ALL", "name_kr": country_code,
                          "name_en": country_code, "currency": "USD", "flag": "un", "hubs": []}
    return {
        **fallback_meta,
        "profile": {},
        "exchange_rate": {"is_base": False, "unit_base": 1, "current_rate": 0.0, "change_pct": 0.0,
                           "history_labels": [], "history_values": [], "source": "수집 실패"},
        "headlines": [_fallback_headline_item(fallback_meta)],
        "hr_trends": [_fallback_trend_item(spec, fallback_meta) for spec in INDUSTRY_TOPICS],
    }


def main() -> dict:
    try:
        cache_by_code = load_cache()
    except Exception as e:
        log.error(f"캐시 로드 중 예기치 못한 오류, 빈 캐시로 계속 진행: {e}")
        cache_by_code = {}

    collected = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(collect_country, c, cache_by_code): c["code"] for c in COUNTRIES}
        for future in as_completed(futures):
            code = futures[future]
            try:
                collected[code] = future.result()
            except Exception as e:
                log.error(f"[{code}] 국가 단위 수집이 스레드에서 완전히 실패, 캐시/안전망으로 대체: {e}")
                try:
                    cached_fallback = cache_by_code.get(code)
                    collected[code] = cached_fallback if cached_fallback else _build_country_fallback(code)
                except Exception as inner_e:
                    log.error(f"[{code}] 안전망 구성 중에도 오류 발생, 최소 골격으로 대체: {inner_e}")
                    collected[code] = _build_country_fallback(code)

    # 원본 COUNTRIES 순서를 유지해 UI 정렬을 안정적으로 유지 (프론트엔드에서 가나다순 정렬 적용)
    # .get()으로 조회해 혹시라도 특정 코드가 누락되어도 KeyError로 죽지 않고 안전망으로 채운다.
    countries_result = [collected.get(c["code"]) or _build_country_fallback(c["code"]) for c in COUNTRIES]

    now_kst = datetime.now(KST)
    data = {
        "generated_at": now_kst.isoformat(),
        "generated_at_display": now_kst.strftime("%Y-%m-%d %H:%M KST"),
        # 상단 배너 고정 표기용: 운영 스케줄이 매일 06:00 KST 실행이므로 "YYYY-MM-DD 06:00 KST 기준"으로 고정
        "as_of_display": f"{now_kst.strftime('%Y-%m-%d')} 06:00 KST 기준",
        "regions": REGIONS,
        "countries": countries_result,
    }

    cache_saved = False
    try:
        save_cache(data)
        cache_saved = True
        log.info(f"data/countries_data.json 저장 완료 ({len(countries_result)}개국)")
    except Exception as e:
        log.error(f"data/countries_data.json 저장 실패: {e}")

    try:
        save_archive(data)
        log.info(f"docs/archive/{now_kst.strftime('%Y-%m-%d')}.json 저장 및 보존정책 적용 완료")
    except Exception as e:
        # 아카이브 저장은 '과거 날짜 조회' 기능에만 영향을 주고 사이트 자체 생성에는 영향이 없으므로
        # 실패해도 스크립트를 죽이지 않고 경고만 남긴다.
        log.error(f"아카이브 저장/보존정책 처리 실패 (사이트 자체는 정상 생성됩니다): {e}")

    if not cache_saved:
        # 핵심 산출물(data/countries_data.json)이 정말 생성되지 않은 경우에만 실패로 간주한다.
        # 이 경우 build_site.py가 읽을 데이터 자체가 없으므로 명확히 실패를 알린다.
        raise RuntimeError(
            "data/countries_data.json 저장에 실패해 사이트를 생성할 수 없습니다. "
            "위 로그의 'data/countries_data.json 저장 실패' 항목을 확인하세요."
        )

    log.info(f"전체 {len(countries_result)}개국 수집 완료 → {CACHE_PATH} / {ARCHIVE_DIR}")
    return data


if __name__ == "__main__":
    try:
        main()
    except Exception:
        # 여기까지 도달했다는 것은 정말로 복구 불가능한 실패라는 뜻이다.
        # log.exception()으로 전체 스택 트레이스를 GitHub Actions 로그에 명확히 남겨,
        # 이후에는 "exit code 1"만 보고 원인을 못 찾는 상황이 재발하지 않도록 한다.
        log.exception("collector.py 실행 중 처리되지 않은 예외가 발생했습니다 (원인은 위 스택 트레이스 참고)")
        raise
