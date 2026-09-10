"""
build_site.py
--------------
data/countries_data.json (오늘자 데이터) 을 template/template.html 에 임베드하여
docs/index.html 을 생성한다.

v2 변경점 — GitHub Actions "Process completed with exit code 1" 크래시 수정
  이전 버전은 다음 세 지점에 아무런 방어 로직이 없어, 조건이 맞으면 그대로 죽었다.

    1) load_data() — data/countries_data.json 이 존재하지 않거나(collector.py가 아직
       한 번도 실행되지 않은 저장소, 혹은 이전 단계 실패로 파일이 안 만들어진 경우)
       빈 파일이거나 JSON 문법이 깨져 있으면 open()/json.load() 가 그대로 예외를 던졌다.
    2) env.get_template(TEMPLATE_NAME) / template.render(**ctx) — template.html 자체가
       없거나 문법 오류가 있으면 TemplateNotFound/TemplateSyntaxError 로 죽었다.
    3) json.dumps(...) — 데이터 안에 직렬화 불가능한 값이 섞이면 TypeError 로 죽었다.

  이번 버전은 이 세 지점 모두를 세밀한 try-except로 감싸고, 다음 원칙으로 항상
  "docs/index.html 이 존재하는 상태"를 보장한다.

    - data/countries_data.json 이 없거나 비었거나 깨졌으면 → 빈 골격 데이터(국가 0개)로
      대체해 계속 진행한다. 화면에는 template.html 자체에 내장된 "데이터를 불러오지
      못했습니다" 배너가 뜨는 정도로 그친다(완전히 빈 화면이나 스크립트 크래시보다 낫다).
    - Jinja2 템플릿 로딩/렌더링 자체가 실패하면 → 템플릿에 의존하지 않는 최소한의
      순수 HTML(MINIMAL_FALLBACK_HTML_TEMPLATE)을 대신 기록한다.
    - json.dumps(..., default=str) 로 직렬화 불가능한 값이 섞여도 문자열로 강제
      변환해 저장을 계속한다.
    - 모든 실패 지점에서 print()로 표준 출력에 구체적인 에러 메시지를 남긴다
      (GitHub Actions 로그에서 바로 원인을 확인할 수 있도록).
    - 그럼에도 불구하고 docs/index.html 자체를 디스크에 쓰는 것조차 실패하는
      경우(디스크 권한/용량 등, 진짜로 복구 불가능한 상황)에만 최종적으로 실패를
      알린다.

실행:
    python src/collector.py    # 39개국 수집 -> data/countries_data.json + docs/archive/*.json
    python src/build_site.py   # 템플릿에 데이터 임베드 -> docs/index.html
"""

import os
import sys
import json
import logging
import traceback
from datetime import datetime, timezone

try:
    import pytz
    _KST = pytz.timezone("Asia/Seoul")
except Exception:
    _KST = None

from jinja2 import Environment, FileSystemLoader, select_autoescape, TemplateError
from markupsafe import Markup

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
log = logging.getLogger("build_site")

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
DATA_PATH = os.path.join(BASE_DIR, "data", "countries_data.json")
ARCHIVE_INDEX_PATH = os.path.join(BASE_DIR, "docs", "archive", "index.json")
TEMPLATE_DIR = os.path.join(BASE_DIR, "template")
TEMPLATE_NAME = "template.html"
OUTPUT_PATH = os.path.join(BASE_DIR, "docs", "index.html")

REQUIRED_PROFILE_FIELDS = ["capital", "population", "gdp", "inflation", "unemployment", "min_wage"]

# Jinja2 템플릿 로딩/렌더링 자체가 실패했을 때 대신 기록하는, 템플릿 파일에 전혀
# 의존하지 않는 순수 HTML. docs/index.html 이 항상 존재하도록 보장하기 위한 최후 수단이다.
MINIMAL_FALLBACK_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Global Daily News</title>
<style>
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Pretendard, sans-serif;
    background: #F8F9FA;
    color: #334155;
    display: flex;
    align-items: center;
    justify-content: center;
    min-height: 100vh;
    margin: 0;
    padding: 24px;
    box-sizing: border-box;
  }}
  .box {{
    max-width: 560px;
    text-align: center;
    background: white;
    border: 1px solid #E5E7EB;
    border-radius: 1.25rem;
    padding: 2.5rem 2rem;
    box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.05), 0 2px 4px -2px rgb(0 0 0 / 0.05);
  }}
  h1 {{ font-size: 1.35rem; margin: 0 0 0.75rem; color: #1E293B; }}
  p {{ font-size: 0.9rem; line-height: 1.6; color: #64748B; margin: 0 0 0.5rem; }}
  .err {{
    margin-top: 1.25rem;
    font-size: 0.72rem;
    color: #94A3B8;
    background: #F1F5F9;
    border-radius: 0.5rem;
    padding: 0.75rem;
    text-align: left;
    white-space: pre-wrap;
    word-break: break-all;
  }}
</style>
</head>
<body>
  <div class="box">
    <h1>Global Daily News</h1>
    <p>사이트를 생성하는 중 문제가 발생해 임시 안내 페이지를 표시하고 있습니다.</p>
    <p>다음 자동 갱신(매일 KST 06:00) 시 정상 데이터로 복구될 예정입니다.</p>
    <div class="err">build_site.py 오류 기록 ({timestamp}):
{error}</div>
  </div>
</body>
</html>
"""


def _now_display() -> str:
    try:
        if _KST is not None:
            return datetime.now(_KST).strftime("%Y-%m-%d %H:%M KST")
    except Exception:
        pass
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _empty_skeleton_data() -> dict:
    """data/countries_data.json 이 없거나/비었거나/깨졌을 때 사용하는 안전한 골격 데이터.
    countries가 빈 배열이면 template.html 내장 JS의 '데이터를 불러오지 못했습니다' 배너가
    자연스럽게 뜨므로, 완전히 빈 화면이나 파이썬 크래시보다 훨씬 안전한 결과다."""
    now_dt = datetime.now(_KST) if _KST is not None else datetime.now(timezone.utc)
    date_str = now_dt.strftime("%Y-%m-%d")
    return {
        "generated_at": now_dt.isoformat(),
        "generated_at_display": _now_display(),
        "as_of_display": f"{date_str} 06:00 KST 기준 (데이터 없음)",
        "regions": [
            {"key": "ALL", "label": "전체"},
        ],
        "countries": [],
    }


def load_data() -> dict:
    """data/countries_data.json 을 안전하게 로드한다.
    파일이 없거나/비었거나/JSON 파싱에 실패하거나/기대한 형태(dict)가 아니면
    예외를 던지지 않고 빈 골격 데이터로 폴백한다."""
    if not os.path.exists(DATA_PATH):
        msg = f"[build_site] {DATA_PATH} 파일이 존재하지 않습니다. 빈 골격 데이터로 진행합니다."
        print(msg)
        log.warning(msg)
        return _empty_skeleton_data()

    try:
        with open(DATA_PATH, "r", encoding="utf-8") as f:
            raw = f.read()
    except Exception as e:
        msg = f"[build_site] {DATA_PATH} 파일을 읽는 중 오류 발생: {e}. 빈 골격 데이터로 진행합니다."
        print(msg)
        log.error(msg)
        return _empty_skeleton_data()

    if not raw or not raw.strip():
        msg = f"[build_site] {DATA_PATH} 파일이 비어 있습니다. 빈 골격 데이터로 진행합니다."
        print(msg)
        log.warning(msg)
        return _empty_skeleton_data()

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        msg = f"[build_site] {DATA_PATH} JSON 파싱 실패: {e}. 빈 골격 데이터로 진행합니다."
        print(msg)
        log.error(msg)
        return _empty_skeleton_data()
    except Exception as e:
        msg = f"[build_site] {DATA_PATH} 로드 중 예기치 못한 오류: {e}. 빈 골격 데이터로 진행합니다."
        print(msg)
        log.error(msg)
        return _empty_skeleton_data()

    if not isinstance(data, dict):
        msg = f"[build_site] {DATA_PATH} 내용이 dict 형태가 아닙니다(type={type(data).__name__}). 빈 골격 데이터로 진행합니다."
        print(msg)
        log.error(msg)
        return _empty_skeleton_data()

    # 필수 최상위 키 보정 (부분적으로만 깨진 파일도 최대한 살려서 사용)
    try:
        default_generated_at = datetime.now(_KST).isoformat() if _KST is not None else datetime.now(timezone.utc).isoformat()
    except Exception:
        default_generated_at = datetime.now(timezone.utc).isoformat()

    data.setdefault("generated_at", default_generated_at)
    data.setdefault("generated_at_display", _now_display())
    data.setdefault("as_of_display", f"{str(data.get('generated_at'))[:10]} 06:00 KST 기준")
    data.setdefault("regions", [{"key": "ALL", "label": "전체"}])
    data.setdefault("countries", [])

    if not isinstance(data.get("countries"), list):
        print(f"[build_site] countries 필드가 리스트가 아닙니다(type={type(data.get('countries')).__name__}). 빈 리스트로 대체합니다.")
        data["countries"] = []
    if not isinstance(data.get("regions"), list):
        print(f"[build_site] regions 필드가 리스트가 아닙니다(type={type(data.get('regions')).__name__}). 기본값으로 대체합니다.")
        data["regions"] = [{"key": "ALL", "label": "전체"}]

    return data


def load_archive_range(today_str: str) -> tuple:
    """아카이브 인덱스에서 (최소 날짜, 최대 날짜)를 읽는다. 없거나 오류가 나면 오늘 하루만
    유효 범위로 안전하게 폴백한다 (절대 예외를 던지지 않음)."""
    try:
        if os.path.exists(ARCHIVE_INDEX_PATH):
            with open(ARCHIVE_INDEX_PATH, "r", encoding="utf-8") as f:
                idx = json.load(f)
            min_date = idx.get("min_date") or today_str
            max_date = idx.get("latest") or today_str
            return min_date, max_date
        else:
            print(f"[build_site] {ARCHIVE_INDEX_PATH} 이 아직 없습니다 (최초 실행) → 오늘 날짜로 폴백합니다.")
    except Exception as e:
        print(f"[build_site] 아카이브 인덱스 로드 실패: {e} → 오늘 날짜로 폴백합니다.")
        log.warning(f"아카이브 인덱스 로드 실패 → 오늘 날짜로 폴백: {e}")
    return today_str, today_str


def _validate(data: dict) -> None:
    """진단용 경고만 남기는 검증 — 어떤 경우에도 예외를 던지지 않는다."""
    try:
        if not isinstance(data, dict):
            print(f"[build_site] _validate: data가 dict가 아닙니다(type={type(data).__name__}), 검증을 건너뜁니다.")
            return

        countries = data.get("countries", [])
        if not isinstance(countries, list):
            print("[build_site] _validate: countries가 리스트가 아닙니다, 검증을 건너뜁니다.")
            return

        if not countries:
            print("[build_site] countries 목록이 비어 있습니다 — collector.py를 먼저 실행했는지 확인하세요.")
            log.warning("countries 목록이 비어 있습니다 — collector.py를 먼저 실행했는지 확인하세요.")
            return

        for c in countries:
            try:
                if not isinstance(c, dict):
                    continue
                profile = c.get("profile", {}) or {}
                missing = [f for f in REQUIRED_PROFILE_FIELDS if not profile.get(f)]
                if missing:
                    log.warning(f"[{c.get('code')}] profile 필드 누락: {missing} — 화면에는 '-'로 표시됩니다.")
                if "exchange_rate" not in c:
                    log.warning(f"[{c.get('code')}] exchange_rate 필드 자체가 없습니다.")
                headlines = c.get("headlines", []) or []
                hr_trends = c.get("hr_trends", []) or []
                if not headlines:
                    log.warning(f"[{c.get('code')}] headlines가 비어 있습니다.")
                if len(hr_trends) < 4:
                    log.warning(f"[{c.get('code')}] hr_trends가 4개 미만입니다 ({len(hr_trends)}개) — Fallback 항목 확인 필요.")
            except Exception as e:
                # 국가 1건의 검증 실패가 전체 빌드를 막아서는 안 된다.
                print(f"[build_site] _validate: 국가 항목 검증 중 오류(무시하고 계속): {e}")
                continue
    except Exception as e:
        # _validate 자체는 어떤 경우에도 build()를 막아서는 안 된다.
        print(f"[build_site] _validate 전체 실패(무시하고 빌드 계속): {e}")


def _write_minimal_fallback(error_message: str) -> None:
    """Jinja2 템플릿 로딩/렌더링 자체가 실패했을 때, 템플릿에 의존하지 않는
    최소한의 순수 HTML을 대신 기록해 docs/index.html 이 항상 존재하도록 보장한다."""
    html = MINIMAL_FALLBACK_HTML_TEMPLATE.format(
        timestamp=_now_display(),
        error=error_message.replace("<", "&lt;").replace(">", "&gt;"),
    )
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[build_site] 최소 안내 페이지를 {OUTPUT_PATH} 에 대신 기록했습니다.")
    log.warning(f"최소 안내 페이지를 {OUTPUT_PATH} 에 대신 기록했습니다: {error_message}")


def build(data: dict) -> bool:
    """docs/index.html 생성을 시도한다. 성공하면 True, 최소 안내 페이지로 대체했으면
    False를 반환한다(그래도 파일 자체는 항상 기록됨). 디스크에 아무것도 쓸 수 없는
    진짜 복구 불가능한 상황에서만 예외를 던진다."""
    _validate(data)

    try:
        today_str = str(data.get("generated_at", ""))[:10] or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    archive_min, archive_max = load_archive_range(today_str)

    try:
        as_of_display = data.get("as_of_display") or f"{today_str} 06:00 KST 기준"
    except Exception:
        as_of_display = f"{today_str} 06:00 KST 기준"

    # ------------------------------------------------------------------
    # 1) Jinja2 템플릿 로딩
    # ------------------------------------------------------------------
    try:
        env = Environment(
            loader=FileSystemLoader(TEMPLATE_DIR),
            autoescape=select_autoescape(["html"]),
        )
        template = env.get_template(TEMPLATE_NAME)
    except TemplateError as e:
        err = f"Jinja2 템플릿 로딩 실패 ({TEMPLATE_DIR}/{TEMPLATE_NAME}): {e}"
        print(f"[build_site] ERROR: {err}")
        log.error(err)
        _write_minimal_fallback(err)
        return False
    except Exception as e:
        err = f"템플릿 로딩 중 예기치 못한 오류: {e}"
        print(f"[build_site] ERROR: {err}")
        print(traceback.format_exc())
        log.error(err)
        _write_minimal_fallback(err)
        return False

    # ------------------------------------------------------------------
    # 2) 대시보드 JSON 직렬화 (default=str 로 직렬화 불가능한 값도 안전하게 처리)
    # ------------------------------------------------------------------
    try:
        raw_json = json.dumps(
            {
                "as_of_display": as_of_display,
                "regions": data.get("regions", []),
                "countries": data.get("countries", []),
            },
            ensure_ascii=False,
            default=str,
        ).replace("</", "<\\/")
        dashboard_json = Markup(raw_json)
    except Exception as e:
        err = f"대시보드 JSON 직렬화 실패: {e}"
        print(f"[build_site] ERROR: {err}")
        log.error(err)
        # 직렬화조차 실패하면 최소한 빈 데이터로라도 페이지가 뜨도록 한다.
        dashboard_json = Markup(json.dumps({"as_of_display": as_of_display, "regions": [], "countries": []}))

    # ------------------------------------------------------------------
    # 3) 템플릿 렌더링
    # ------------------------------------------------------------------
    ctx = {
        "as_of_display": as_of_display,
        "generated_at_date": today_str,
        "archive_min_date": archive_min,
        "archive_max_date": archive_max,
        "dashboard_json": dashboard_json,
    }

    try:
        html = template.render(**ctx)
    except TemplateError as e:
        err = f"Jinja2 템플릿 렌더링 실패: {e}"
        print(f"[build_site] ERROR: {err}")
        log.error(err)
        _write_minimal_fallback(err)
        return False
    except Exception as e:
        err = f"템플릿 렌더링 중 예기치 못한 오류: {e}"
        print(f"[build_site] ERROR: {err}")
        print(traceback.format_exc())
        log.error(err)
        _write_minimal_fallback(err)
        return False

    # ------------------------------------------------------------------
    # 4) 파일 기록 — 이 단계가 실패하면 정말로 할 수 있는 것이 없으므로 예외를 다시 던진다.
    # ------------------------------------------------------------------
    try:
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            f.write(html)
    except Exception as e:
        err = f"{OUTPUT_PATH} 파일 기록 실패: {e}"
        print(f"[build_site] FATAL: {err}")
        print(traceback.format_exc())
        log.error(err)
        raise

    country_count = len(data.get("countries", []))
    msg = f"생성 완료 → {OUTPUT_PATH} ({country_count}개국, 아카이브 범위 {archive_min}~{archive_max})"
    print(f"[build_site] {msg}")
    log.info(msg)
    return True


def main() -> None:
    try:
        data = load_data()
    except Exception as e:
        # load_data() 자체는 내부적으로 이미 모든 예외를 흡수하지만, 방어적으로 한 번 더 감싼다.
        print(f"[build_site] load_data() 호출 중 예기치 못한 오류: {e}")
        print(traceback.format_exc())
        data = _empty_skeleton_data()

    try:
        build(data)
    except Exception as e:
        # 여기까지 도달했다는 것은 docs/index.html 파일 기록 자체가 실패했다는 뜻으로,
        # 정말로 복구 불가능한 상황이다. 표준 출력에 원인을 명확히 남긴 뒤 실패를 알린다.
        print(f"[build_site] FATAL: build_site.py 실행이 복구 불가능한 오류로 중단되었습니다: {e}")
        print(traceback.format_exc())
        log.exception("build_site.py 실행 중 처리되지 않은 예외가 발생했습니다")
        sys.exit(1)


if __name__ == "__main__":
    main()
