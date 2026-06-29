# ═══════════════════════════════════════════════════════════════════
# PDF Keyword Search — Streamlit app
# Fixed: 403/mirror fallback, multi-keyword, dark/light mode,
#        _ILLEGAL_CHARS_RE order, disk autosave, 6-sheet Excel,
#        correct status values, future timeout, retry detection
# ═══════════════════════════════════════════════════════════════════
import streamlit as st
import pandas as pd
import io
import os
import re
import time
import json
import random
import threading
import fitz                          # PyMuPDF
from datetime import datetime
from html.parser import HTMLParser
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Page config ────────────────────────────────────────────────────
st.set_page_config(
    page_title="PDF Keyword Search",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ══════════════════════════════════════════════════════════════════
# SECTION 1 — Session state defaults
# ══════════════════════════════════════════════════════════════════
if "results_df"  not in st.session_state: st.session_state.results_df  = None
if "running"     not in st.session_state: st.session_state.running     = False
if "dark_mode"   not in st.session_state: st.session_state.dark_mode   = True

# ══════════════════════════════════════════════════════════════════
# SECTION 2 — Theme
# ══════════════════════════════════════════════════════════════════
def _theme():
    if st.session_state.dark_mode:
        return dict(
            bg="#0f1117", card="#1a1f2e", card2="#16213e", border="#2d3561",
            text="#e8eaf0", subtext="#8892a4", accent="#00d4ff",
            found_bg="#0d4c2b", found_fg="#00e676",
            nf_bg="#4c1a0d",   nf_fg="#ff6b6b",
            scan_bg="#2d2600", scan_fg="#ffd600",
            fail_bg="#2d0024", fail_fg="#f48fb1",
            corrupt_bg="#1a0a2e", corrupt_fg="#ce93d8",
            log_bg="#12161f",  sidebar_bg="#0b0e17",
        )
    return dict(
        bg="#f4f6fa", card="#ffffff", card2="#eef1f8", border="#d0d7e8",
        text="#1a1f2e", subtext="#5a6480", accent="#1565c0",
        found_bg="#e8f5e9", found_fg="#1b5e20",
        nf_bg="#ffebee",   nf_fg="#b71c1c",
        scan_bg="#fffde7", scan_fg="#f57f17",
        fail_bg="#fce4ec", fail_fg="#880e4f",
        corrupt_bg="#f3e5f5", corrupt_fg="#6a1b9a",
        log_bg="#f0f2f8",  sidebar_bg="#edf0f8",
    )

def _inject_css():
    t = _theme()
    st.markdown(f"""
<style>
html, body, [data-testid="stAppViewContainer"], .main {{
    background-color: {t['bg']} !important; color: {t['text']} !important;
}}
[data-testid="stHeader"] {{ background: {t['bg']} !important; }}
section[data-testid="stSidebar"] {{
    background: {t['sidebar_bg']} !important;
    border-right: 1px solid {t['border']};
}}
section[data-testid="stSidebar"] * {{ color: {t['text']} !important; }}
.header-card {{
    background: linear-gradient(135deg, {t['card']} 0%, {t['card2']} 100%);
    border: 1px solid {t['border']}; border-radius: 14px;
    padding: 26px 34px; margin-bottom: 22px;
}}
.header-title {{ font-size: 1.9rem; font-weight: 800; color: {t['accent']}; margin: 0; }}
.header-sub   {{ font-size: 0.93rem; color: {t['subtext']}; margin-top: 5px; }}
.stat-card {{
    background: {t['card']}; border-radius: 12px; padding: 18px 16px;
    text-align: center; border: 1px solid {t['border']};
}}
.stat-number {{ font-size: 1.9rem; font-weight: 800; }}
.stat-label  {{ font-size: 0.74rem; color: {t['subtext']}; margin-top: 4px;
                text-transform: uppercase; letter-spacing: 0.6px; }}
.progress-box {{
    background: {t['log_bg']}; border: 1px solid {t['border']};
    border-radius: 10px; padding: 16px 20px;
    font-family: 'Courier New', monospace; font-size: 0.8rem;
    color: {t['subtext']}; max-height: 210px; overflow-y: auto; line-height: 1.6;
}}
.limit-badge {{
    background: linear-gradient(90deg,#e65100,#ff9800); color:#fff;
    border-radius: 8px; padding: 9px 18px; font-weight: 700;
    font-size: 1.05rem; text-align: center; margin: 6px 0 14px 0;
}}
hr {{ border-color: {t['border']} !important; margin: 18px 0; }}
.stTabs [data-baseweb="tab-list"] {{ gap: 6px; }}
.stTabs [data-baseweb="tab"] {{
    background: {t['card']}; border: 1px solid {t['border']};
    border-radius: 8px 8px 0 0; color: {t['subtext']}; font-weight: 600; padding: 8px 18px;
}}
.stTabs [aria-selected="true"] {{
    background: {t['card2']} !important; color: {t['accent']} !important;
    border-bottom: 2px solid {t['accent']} !important;
}}
.stButton > button {{
    border-radius: 8px; font-weight: 700; border: 1px solid {t['border']};
    background: {t['card']}; color: {t['text']}; transition: all 0.18s;
}}
.stButton > button:hover {{ transform: translateY(-2px); border-color: {t['accent']}; color: {t['accent']}; }}
</style>""", unsafe_allow_html=True)

_inject_css()

# ══════════════════════════════════════════════════════════════════
# SECTION 3 — Constants
# ══════════════════════════════════════════════════════════════════
SEARCH_LIMIT      = 50_000
DEFAULT_WORKERS   = 6
DEFAULT_TIMEOUT   = 20
KEYWORD_SEP       = "|"
_AUTOSAVE_FILE    = "/tmp/pdf_search_autosave.csv"
_AUTOSAVE_META    = "/tmp/pdf_search_meta.json"

# Exact status strings (must be stable — results match on these)
class S:
    FOUND     = "Found"
    NOT_FOUND = "Not Found"
    SCANNED   = "PDF is Non searchable,Advanced Scanned Extraction can make the PDF searchable."
    CORRUPTED = "PDF Not mirrored / Corrupted"
    FAILED    = "Failed to get PDF text"

# Statuses that warrant a retry in pass 2
_RETRY_STATUSES = {S.FAILED}

# Output column order
_OUT_COLS = [
    "URL", "Keyword", "Extraction Option",
    "URL_Status", "URL_Search_Status",
    "Keyword_Status", "feature_name",
    "feature_value", "Keyword_Search_Status",
]

# FIX 4: define _ILLEGAL_CHARS_RE at top level before any function that uses it
_ILLEGAL_CHARS_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")

# ══════════════════════════════════════════════════════════════════
# SECTION 4 — Network layer
# ══════════════════════════════════════════════════════════════════
_thread_local = threading.local()

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/pdf,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    # Required — z2data silently drops connections without Referer
    "Referer":         "https://source.z2data.com/",
}

_CONNECT_TIMEOUT  = 15   # seconds for TCP handshake
_MIN_DELAY_SECS   = 0.20 # minimum gap between requests to the same host
_BLOCK_THRESHOLD  = 5    # consecutive failures before declaring host blocked
_BLOCK_COOLDOWN   = 30   # seconds to pause when blocked

_rate_lock  = threading.Lock()
_block_lock = threading.Lock()
_last_req:  dict = {}
_consec_fail: dict = {}
_blocked_until: dict = {}


def _host(url: str) -> str:
    try:    return url.split("/")[2]
    except: return url


def _make_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update(_HEADERS)
    sess.verify = False
    no_retry = Retry(total=0, raise_on_status=False)
    adapter  = HTTPAdapter(max_retries=no_retry, pool_connections=20, pool_maxsize=20)
    sess.mount("https://", adapter)
    sess.mount("http://",  adapter)
    return sess


def _get_session(fresh: bool = False) -> requests.Session:
    if fresh or not hasattr(_thread_local, "session"):
        _thread_local.session = _make_session()
    return _thread_local.session


def _rate_limit(host: str):
    with _rate_lock:
        gap = time.time() - _last_req.get(host, 0)
        if gap < _MIN_DELAY_SECS:
            time.sleep(_MIN_DELAY_SECS - gap)
        _last_req[host] = time.time()


def _record_failure(host: str):
    with _block_lock:
        _consec_fail[host] = _consec_fail.get(host, 0) + 1
        if _consec_fail[host] >= _BLOCK_THRESHOLD:
            _blocked_until[host] = time.time() + _BLOCK_COOLDOWN
            _consec_fail[host]   = 0


def _record_success(host: str):
    with _block_lock:
        _consec_fail[host] = 0


def _wait_if_blocked(host: str):
    wait = _blocked_until.get(host, 0) - time.time()
    if wait > 0:
        time.sleep(wait)


def _clear_network_state():
    with _rate_lock:  _last_req.clear()
    with _block_lock: _consec_fail.clear(); _blocked_until.clear()


def _get_alternate_urls(url: str) -> list:
    """
    Return alternate mirror URLs to try after a primary failure.
    z2data has two CDN hosts: source.z2data.com and source1.z2data.com.
    Also strips /web/ prefix for old archived URLs.
    """
    alts = []
    if "//source1.z2data.com" in url:
        alts.append(url.replace("//source1.z2data.com", "//source.z2data.com", 1))
    elif "//source.z2data.com" in url:
        alts.append(url.replace("//source.z2data.com", "//source1.z2data.com", 1))

    if "/web/" in url:
        stripped = url.replace("/web/", "/", 1)
        alts.append(stripped)
        if "//source1.z2data.com" in stripped:
            alts.append(stripped.replace("//source1.z2data.com", "//source.z2data.com", 1))
        elif "//source.z2data.com" in stripped:
            alts.append(stripped.replace("//source.z2data.com", "//source1.z2data.com", 1))

    return alts


def _download_one(url: str, timeout: int, fresh: bool = False):
    """
    Single download attempt.
    Returns (content_bytes, error_category) where category is:
      ""        — success
      "403"     — access denied (try mirror)
      "404"     — not found (permanent, stop)
      "429"     — rate limited (pause, try mirror)
      "timeout" — read timeout (retry)
      "ssl"     — TLS error (retry with fresh session)
      "connection" — TCP error (retry with fresh session)
      "http_NNN"   — other HTTP error
    """
    host = _host(url)
    _wait_if_blocked(host)
    _rate_limit(host)
    session = _get_session(fresh=fresh)
    try:
        resp = session.get(url, timeout=(_CONNECT_TIMEOUT, timeout),
                           stream=False, allow_redirects=True)
        if resp.status_code == 200:
            _record_success(host)
            return resp.content, ""
        _record_failure(host)
        code = resp.status_code
        if code in (404, 410): return None, "404"
        if code == 403:        return None, "403"   # FIX 1: was "blocked" — now tries mirror
        if code == 429:
            time.sleep(4 + random.random() * 4)
            return None, "429"                      # FIX 1: was "blocked" — now tries mirror
        return None, f"http_{code}"
    except Exception as e:
        _record_failure(host)
        s = str(e).lower()
        if any(w in s for w in ("ssl", "cert", "tls", "handshake")): return None, "ssl"
        if any(w in s for w in ("timed out", "timeout", "read timed")): return None, "timeout"
        return None, "connection"


def _fetch(url: str, session_timeout: int, use_mirror: bool = True):
    """
    Try primary URL + all mirror alternates.
    Each candidate gets up to 3 attempts with exponential back-off.

    FIX 1: 403 and 429 now break the inner retry loop and fall through to
    the next candidate (mirror) instead of aborting all candidates.
    This is the root fix for 'Failed to get PDF text' on valid z2data URLs.
    """
    candidates = [url] + (_get_alternate_urls(url) if use_mirror else [])
    last_cat   = "timeout"
    conn_err   = False

    for try_url in candidates:
        for attempt in range(1, 4):
            content, cat = _download_one(try_url, session_timeout,
                                         fresh=(conn_err and attempt == 1))
            conn_err = False

            if content is not None:
                if len(content) < 32:
                    last_cat = "corrupted"; break
                sig = content[:64].lstrip()
                if len(content) < 64 \
                        and not sig.startswith(b"%PDF") \
                        and not any(t in sig.lower() for t in (b"<html", b"<!doc", b"<head")):
                    last_cat = "corrupted"; break
                return content, "ok"

            last_cat = cat
            if cat == "404": return None, "404"     # permanent — stop all candidates
            if cat == "403": break                  # FIX 1: try next mirror
            if cat == "429": break                  # FIX 1: try next mirror
            if cat == "ssl": break                  # TLS won't fix itself on retry
            if cat in ("timeout", "connection"):
                conn_err = True

            if attempt < 3:
                time.sleep((2 ** attempt) * (0.5 + 0.5 * random.random()))

    return None, last_cat


# ══════════════════════════════════════════════════════════════════
# SECTION 5 — Text extraction
# ══════════════════════════════════════════════════════════════════
_MAX_PDF_MB    = 40
_MAX_PDF_PAGES = 500
_MAX_TEXT_CHARS = 3_000_000
_MIN_USEFUL_CHARS = 50


def _extract_pdf(data: bytes):
    if len(data) > _MAX_PDF_MB * 1024 * 1024:
        return "", f"error:PDF too large"
    try:
        doc   = fitz.open(stream=data, filetype="pdf")
        pages = doc.page_count
        if pages > _MAX_PDF_PAGES:
            doc.close()
            return "", "error:too many pages"
        parts, total = [], 0
        for page in doc:
            t = page.get_text("text")
            total += len(t)
            parts.append(t)
            if total >= _MAX_TEXT_CHARS:
                break
        doc.close()
        text = "\n".join(parts)[:_MAX_TEXT_CHARS]
        return (text, "searchable") if text.strip() else ("", "scanned")
    except Exception as e:
        return "", f"error:{e}"


class _HtmlStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []
        self._skip = False
    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style", "noscript", "head"): self._skip = True
    def handle_endtag(self, tag):
        if tag in ("script", "style", "noscript", "head"): self._skip = False
    def handle_data(self, data):
        if not self._skip:
            s = data.strip()
            if s: self.parts.append(s)


def _extract_html(data: bytes):
    try:
        html = None
        for enc in ("utf-8", "latin-1", "cp1252"):
            try: html = data.decode(enc); break
            except Exception: pass
        if not html: html = data.decode("utf-8", errors="replace")
        p = _HtmlStripper(); p.feed(html)
        text = "\n".join(p.parts)
        return (text, "searchable") if text.strip() else ("", "scanned")
    except Exception as e:
        return "", f"error:{e}"


def _extract(content: bytes, is_html: bool):
    """Try primary extractor then fallback to the other."""
    if is_html:
        text, status = _extract_html(content)
        if not text and "error:" not in status:
            text, status = _extract_pdf(content)
    else:
        text, status = _extract_pdf(content)
        if "error:" in status:
            text2, status2 = _extract_html(content)
            if text2: text, status = text2, status2
    return text, status


# URL-level text cache — same document never extracted twice
_cache_lock = threading.Lock()
_url_cache: dict = {}


def _get_text(url: str, content: bytes, is_html: bool):
    with _cache_lock:
        if url in _url_cache: return _url_cache[url]
    result = _extract(content, is_html)
    with _cache_lock:
        _url_cache[url] = result
    return result


def _clear_text_cache():
    with _cache_lock: _url_cache.clear()


# ══════════════════════════════════════════════════════════════════
# SECTION 6 — Keyword parsing + search
# ══════════════════════════════════════════════════════════════════
def _parse_keywords(raw: str) -> list:
    """Split 'KW1 | KW2 | KW3' into ['KW1','KW2','KW3']."""
    raw = str(raw).strip()
    if KEYWORD_SEP in raw:
        return [k.strip() for k in raw.split(KEYWORD_SEP) if k.strip()]
    return [raw] if raw else []


def _normalize(text: str) -> str:
    text = _ILLEGAL_CHARS_RE.sub(" ", text)
    text = text.replace("\u00ad", "").replace("\u00a0", " ")
    text = re.sub(r"-\s*\n\s*", "", text)
    text = re.sub(r"\s+", " ", text)
    return text


def _search_keyword(text: str, keyword: str, case_sensitive: bool) -> int:
    if not text or not keyword: return 0
    flags = 0 if case_sensitive else re.IGNORECASE
    try:    return len(re.findall(re.escape(keyword.strip()), text, flags))
    except: return 0


def _best_snippet(text: str, keyword: str, ctx: int = 120) -> str:
    if not text or not keyword: return ""
    kw = str(keyword).lower(); tl = text.lower()
    positions, start, best, seen = [], 0, "", set()
    while True:
        i = tl.find(kw, start)
        if i == -1: break
        positions.append(i); start = i + 1
    for i in positions:
        s   = max(0, i - ctx)
        e   = min(len(text), i + len(kw) + ctx)
        raw = _ILLEGAL_CHARS_RE.sub("", text[s:e].strip())
        if not raw or raw in seen: continue
        seen.add(raw)
        cand = ("\u2026" if s > 0 else "") + raw + ("\u2026" if e < len(text) else "")
        if len(raw) > len(best.replace("\u2026", "")): best = cand
    return best


# ══════════════════════════════════════════════════════════════════
# SECTION 7 — Main per-row processor
# ══════════════════════════════════════════════════════════════════
def process_one_url(url: str, raw_keyword: str,
                    case_sensitive: bool, session_timeout: int) -> dict:
    """
    Download URL once, search every keyword parsed from raw_keyword.
    Returns one result dict per call (multi-keyword collapses into one row).
    """
    base = {
        "URL":                 url,
        "Keyword":             raw_keyword,
        "Extraction Option":   "",
        "URL_Status":          None,
        "URL_Search_Status":   "",
        "Keyword_Status":      None,
        "feature_name":        raw_keyword,
        "feature_value":       "",
        "Keyword_Search_Status": "",
        "_retry":              False,   # internal flag for pass-2
    }

    def done(**kw):
        r = dict(base); r.update(kw); return r

    url = str(url).strip()
    if not url or not url.startswith("http"):
        return done(URL_Status=0, URL_Search_Status="Invalid URL",
                    Keyword_Search_Status=S.FAILED, _retry=False)

    is_html = url.lower().split("?")[0].endswith((".html", ".htm"))
    base["Extraction Option"] = "HTML" if is_html else "PDF"

    # ── Download (FIX 1: mirrors tried on 403) ────────────────────
    content, dl_cat = _fetch(url, session_timeout, use_mirror=True)

    if content is None:
        _cat_msg = {
            "404":        "URL Not Found (404)",
            "403":        "Access Denied on all mirrors (403)",
            "429":        "Rate Limited on all mirrors (429)",
            "ssl":        "SSL/TLS Error",
            "timeout":    "Timeout",
            "connection": "Connection Error",
            "corrupted":  "File Corrupted / Too Small",
        }
        note = _cat_msg.get(dl_cat, f"Download failed: {dl_cat}")
        retry = dl_cat in ("timeout", "connection", "429", "corrupted")
        return done(URL_Status=0, URL_Search_Status=note,
                    Keyword_Search_Status=S.FAILED, Notes=note, _retry=retry)

    # ── Text extraction ────────────────────────────────────────────
    text, ext_status = _get_text(url, content, is_html)

    if "error:" in ext_status or (not text and ext_status not in ("scanned",)):
        return done(URL_Status=3, URL_Search_Status="Done",
                    Keyword_Search_Status=S.CORRUPTED, _retry=False)

    if ext_status == "scanned":
        return done(URL_Status=3, URL_Search_Status="Done",
                    Keyword_Status=None,
                    Keyword_Search_Status=S.SCANNED, _retry=False)

    norm = _normalize(text)
    if len(norm.strip()) < _MIN_USEFUL_CHARS:
        return done(URL_Status=3, URL_Search_Status="Done",
                    Keyword_Search_Status=S.CORRUPTED, _retry=False)

    # ── Keyword search (FIX 2: multi-keyword via | separator) ─────
    keywords = _parse_keywords(raw_keyword)
    if not keywords:
        keywords = [raw_keyword]

    found_kws, missing_kws, total_count = [], [], 0
    first_snippet = ""

    for kw in keywords:
        cnt = _search_keyword(norm, kw, case_sensitive)
        if cnt > 0:
            found_kws.append(kw)
            total_count += cnt
            if not first_snippet:
                first_snippet = _best_snippet(norm, kw)
        else:
            missing_kws.append(kw)

    if found_kws:
        return done(
            URL_Status=3, URL_Search_Status="Done", Keyword_Status=3.0,
            feature_name=", ".join(found_kws),
            feature_value=first_snippet,
            Keyword_Search_Status=S.FOUND,
            _retry=False,
        )
    else:
        return done(
            URL_Status=3, URL_Search_Status="Done", Keyword_Status=3.0,
            feature_name=raw_keyword,
            Keyword_Search_Status=S.NOT_FOUND,
            _retry=False,
        )


# ══════════════════════════════════════════════════════════════════
# SECTION 8 — Output helpers
# ══════════════════════════════════════════════════════════════════
def _sanitize_cell(v):
    return _ILLEGAL_CHARS_RE.sub("", v) if isinstance(v, str) else v


def _build_df(result_dicts: list) -> pd.DataFrame:
    df = pd.DataFrame(result_dicts)
    df.rename(columns={"Extraction_Option": "Extraction Option"}, inplace=True)
    for c in _OUT_COLS:
        if c not in df.columns: df[c] = None
    return df[_OUT_COLS].copy()


def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    fn = getattr(df, "map", None) or df.applymap
    return fn(_sanitize_cell)


def df_to_excel_bytes(df: pd.DataFrame) -> bytes:
    """
    FIX 6: Export to 6 sheets matching README specification:
      All Results / Found / Not Found / Scanned / Corrupted / Failed
    """
    clean = _clean_df(df)
    buf   = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        clean.to_excel(w, index=False, sheet_name="All Results")
        _sheet(w, clean, "Keyword_Search_Status", S.FOUND,     "Found")
        _sheet(w, clean, "Keyword_Search_Status", S.NOT_FOUND, "Not Found")
        _sheet(w, clean, "Keyword_Search_Status", S.SCANNED,   "Scanned")
        _sheet(w, clean, "Keyword_Search_Status", S.CORRUPTED, "Corrupted")
        _sheet(w, clean, "Keyword_Search_Status", S.FAILED,    "Failed")
    return buf.getvalue()


def _sheet(writer, df, col, val, name):
    subset = df[df[col] == val]
    if not subset.empty:
        subset.to_excel(writer, index=False, sheet_name=name)


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def _make_template() -> bytes:
    tpl = pd.DataFrame({
        "URL":     ["https://source.z2data.com/example.pdf",
                    "https://source.z2data.com/example2.pdf",
                    "https://source.z2data.com/example3.html"],
        "Keyword": ["8536507000",
                    "8536507000|8536.50.7000",
                    "85366990|74122000|39174000"],
    })
    return df_to_excel_bytes(tpl)


# ══════════════════════════════════════════════════════════════════
# SECTION 9 — Disk autosave / recovery
# ══════════════════════════════════════════════════════════════════
def _autosave(result_dicts: list, processed: int, total: int):
    """FIX 5: Save partial results to disk so they survive a crash/refresh."""
    try:
        df = _build_df(result_dicts)
        df.to_csv(_AUTOSAVE_FILE, index=False, encoding="utf-8-sig")
        meta = {"processed": processed, "total": total,
                "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "rows": len(df)}
        with open(_AUTOSAVE_META, "w") as f:
            json.dump(meta, f)
    except Exception:
        pass


def _load_autosave():
    try:
        if os.path.exists(_AUTOSAVE_FILE) and os.path.getsize(_AUTOSAVE_FILE) > 0:
            df   = pd.read_csv(_AUTOSAVE_FILE, dtype={"Keyword": str, "URL": str})
            meta = {}
            if os.path.exists(_AUTOSAVE_META):
                with open(_AUTOSAVE_META) as f: meta = json.load(f)
            return df, meta
    except Exception:
        pass
    return None, None


def _clear_autosave():
    for p in (_AUTOSAVE_FILE, _AUTOSAVE_META):
        try: os.remove(p)
        except Exception: pass


# ══════════════════════════════════════════════════════════════════
# SECTION 10 — UI helpers
# ══════════════════════════════════════════════════════════════════
def _status_style(val):
    t = _theme()
    m = {
        S.FOUND:     f"background-color:{t['found_bg']};color:{t['found_fg']}",
        S.NOT_FOUND: f"background-color:{t['nf_bg']};color:{t['nf_fg']}",
        S.SCANNED:   f"background-color:{t['scan_bg']};color:{t['scan_fg']}",
        S.CORRUPTED: f"background-color:{t['corrupt_bg']};color:{t['corrupt_fg']}",
        S.FAILED:    f"background-color:{t['fail_bg']};color:{t['fail_fg']}",
    }
    return m.get(val, "")


def _stat_card(col, value, color, label):
    t = _theme()
    col.markdown(f"""
    <div class="stat-card">
        <div class="stat-number" style="color:{color}">{value:,}</div>
        <div class="stat-label">{label}</div>
    </div>""", unsafe_allow_html=True)


def _stat_row(df):
    t  = _theme()
    kw = "Keyword_Search_Status"
    total    = len(df)
    found    = (df[kw] == S.FOUND).sum()
    not_fnd  = (df[kw] == S.NOT_FOUND).sum()
    scanned  = (df[kw] == S.SCANNED).sum()
    corrupted= (df[kw] == S.CORRUPTED).sum()
    failed   = (df[kw] == S.FAILED).sum()
    cols = st.columns(6)
    _stat_card(cols[0], total,    t["accent"],     "Total")
    _stat_card(cols[1], found,    t["found_fg"],   "Found")
    _stat_card(cols[2], not_fnd,  t["nf_fg"],      "Not Found")
    _stat_card(cols[3], scanned,  t["scan_fg"],    "Scanned")
    _stat_card(cols[4], corrupted,t["corrupt_fg"], "Corrupted")
    _stat_card(cols[5], failed,   t["fail_fg"],    "Failed")


# ══════════════════════════════════════════════════════════════════
# SECTION 11 — SIDEBAR
# ══════════════════════════════════════════════════════════════════
with st.sidebar:
    t = _theme()

    # FIX 3: Dark / Light toggle
    mode_label = "☀️ Light Mode" if st.session_state.dark_mode else "🌙 Dark Mode"
    if st.button(mode_label, use_container_width=True):
        st.session_state.dark_mode = not st.session_state.dark_mode
        st.rerun()

    st.markdown("---")
    st.markdown(f'<div class="limit-badge">⚠️ Limit: {SEARCH_LIMIT:,} rows</div>',
                unsafe_allow_html=True)

    st.markdown("**Search Options**")
    workers        = st.slider("Concurrent Workers",   2, 20, DEFAULT_WORKERS, 1)
    timeout        = st.slider("Per-URL Timeout (sec)",5, 60, DEFAULT_TIMEOUT, 5)
    case_sensitive = st.checkbox("Case-Sensitive Search", value=False)

    st.markdown("---")
    st.markdown("**Template**")
    st.download_button(
        "⬇️ Download Template (.xlsx)",
        data=_make_template(),
        file_name="Keyword_Search_Template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

    st.markdown("---")
    st.markdown("**Status Legend**")
    st.markdown(f"""
<span style='color:{t["found_fg"]}'>● Found</span> — keyword in document<br>
<span style='color:{t["nf_fg"]}'>● Not Found</span> — document read, keyword absent<br>
<span style='color:{t["scan_fg"]}'>● Scanned</span> — image PDF, no text layer<br>
<span style='color:{t["corrupt_fg"]}'>● Corrupted</span> — damaged / unreadable file<br>
<span style='color:{t["fail_fg"]}'>● Failed</span> — could not download / access
""", unsafe_allow_html=True)

    # ── Recovery panel ─────────────────────────────────────────────
    saved_df, saved_meta = _load_autosave()
    if saved_df is not None and saved_meta:
        st.markdown("---")
        st.markdown("**💾 Saved Progress**")
        st.caption(
            f"{saved_meta.get('rows',0):,} rows saved  \n"
            f"At: {saved_meta.get('saved_at','?')}  \n"
            f"Progress: {saved_meta.get('processed',0):,}/{saved_meta.get('total',0):,}"
        )
        col_r1, col_r2 = st.columns(2)
        with col_r1:
            if st.button("♻️ Restore", use_container_width=True):
                st.session_state.results_df = saved_df
                st.rerun()
        with col_r2:
            if st.button("🗑 Clear", use_container_width=True):
                _clear_autosave()
                st.rerun()
        st.download_button(
            "📥 Download Saved",
            data=df_to_csv_bytes(saved_df),
            file_name=f"partial_{saved_meta.get('saved_at','').replace(' ','_').replace(':','-')}.csv",
            mime="text/csv",
            use_container_width=True,
        )


# ══════════════════════════════════════════════════════════════════
# SECTION 12 — Header
# ══════════════════════════════════════════════════════════════════
t = _theme()
st.markdown(f"""
<div class="header-card">
    <div class="header-title">🔍 PDF Keyword Search</div>
    <div class="header-sub">
        Fast concurrent search across PDF &amp; HTML documents &nbsp;·&nbsp;
        Multi-keyword: <code style="background:{t['card2']};padding:1px 6px;border-radius:4px">KW1 | KW2 | KW3</code>
    </div>
</div>
""", unsafe_allow_html=True)

tab_search, tab_results, tab_guide = st.tabs(["🔍  Search", "📊  Results", "📖  Guide"])


# ══════════════════════════════════════════════════════════════════
# TAB 1 — SEARCH
# ══════════════════════════════════════════════════════════════════
with tab_search:
    col_up, col_info = st.columns([3, 2])

    with col_up:
        st.markdown("##### 📁 Upload Input File")
        st.caption("Excel or CSV with **URL** and **Keyword** columns.")
        uploaded = st.file_uploader("Drop file here",
                                    type=["xlsx","xls","csv"],
                                    label_visibility="collapsed")

    with col_info:
        t = _theme()
        st.markdown("##### 📌 Input Format")
        st.markdown(f"""
<div style="background:{t['card']};border:1px solid {t['border']};
     border-radius:10px;padding:14px 18px;font-size:0.85rem">
<b>Required columns:</b> <code>URL</code> and <code>Keyword</code><br><br>
<b>Multi-keyword</b> (pipe separator):<br>
<code>8536507000</code> — single<br>
<code>8536507000 | 8536.50</code> — two keywords<br>
<code>PartNo | HsCode | EAN</code> — three keywords<br><br>
Keyword is <b>Found</b> if <i>any</i> keyword matches.
</div>""", unsafe_allow_html=True)

    if uploaded:
        try:
            if uploaded.name.endswith(".csv"):
                input_df = pd.read_csv(uploaded, dtype={"Keyword": str, "URL": str})
            else:
                input_df = pd.read_excel(uploaded, dtype={"Keyword": str, "URL": str})

            input_df.columns = [c.strip() for c in input_df.columns]

            # Accept "Offline" as alias for URL
            if "URL" not in input_df.columns and "Offline" in input_df.columns:
                input_df.rename(columns={"Offline": "URL"}, inplace=True)

            missing = [c for c in ["URL","Keyword"] if c not in input_df.columns]
            if missing:
                st.error(f"❌ Missing columns: **{', '.join(missing)}**  |  Found: `{input_df.columns.tolist()}`")
            else:
                input_df = input_df.dropna(subset=["URL"]).reset_index(drop=True)
                n = len(input_df)

                if n > SEARCH_LIMIT:
                    st.warning(f"⚠️ {n:,} rows — trimmed to {SEARCH_LIMIT:,}")
                    input_df = input_df.head(SEARCH_LIMIT)
                    n = SEARCH_LIMIT

                st.success(f"✅ **{n:,} rows** loaded from `{uploaded.name}`")

                with st.expander("🔎 Preview input (first 10 rows)", expanded=False):
                    st.dataframe(input_df.head(10), use_container_width=True)

                st.markdown("---")
                c1, c2, c3 = st.columns([3, 1, 1])
                with c1:
                    start_btn = st.button("🚀 Start Search", use_container_width=True,
                                          type="primary", disabled=st.session_state.running)
                with c2:
                    stop_btn = st.button("⏹ Stop", use_container_width=True)
                with c3:
                    if st.session_state.results_df is not None:
                        if st.button("🗑 Clear", use_container_width=True):
                            st.session_state.results_df = None
                            st.rerun()

                if stop_btn:
                    st.session_state.running = False

                if start_btn and not st.session_state.running:
                    st.session_state.running = True
                    st.session_state.results_df = None
                    _clear_network_state()
                    _clear_text_cache()

                    rows  = input_df.to_dict("records")
                    total = len(rows)

                    prog_bar   = st.progress(0, text="Initialising…")
                    status_txt = st.empty()
                    log_box    = st.empty()

                    all_results: list = []
                    completed         = [0]
                    log_lines:  list  = []
                    start_ts          = time.time()

                    def _log(msg):
                        ts = datetime.now().strftime("%H:%M:%S")
                        log_lines.append(f"[{ts}] {msg}")
                        if len(log_lines) > 100: log_lines.pop(0)

                    def _refresh_ui(label):
                        pct     = min(completed[0] / total, 1.0)
                        elapsed = time.time() - start_ts
                        rate    = completed[0] / elapsed if elapsed > 0 else 0
                        eta     = (total - completed[0]) / rate if rate > 0 else 0
                        prog_bar.progress(pct,
                            text=f"[{label}] {completed[0]:,}/{total:,} · {rate:.1f} URLs/s · ETA {eta:.0f}s")
                        status_txt.markdown(
                            f"⏱ **{elapsed:.0f}s elapsed** &nbsp;|&nbsp; "
                            f"**{rate:.1f} URLs/s** &nbsp;|&nbsp; "
                            f"**{completed[0]:,}/{total:,}**")
                        log_box.markdown(
                            '<div class="progress-box">' +
                            "<br>".join(log_lines[-30:]) +
                            "</div>", unsafe_allow_html=True)

                    def _run_pass(work, label, extra_delay=0.0):
                        pass_results, pass_retry = [], []

                        with ThreadPoolExecutor(max_workers=workers) as ex:
                            fmap = {}
                            for r in work:
                                if extra_delay > 0: time.sleep(extra_delay)
                                f = ex.submit(
                                    process_one_url,
                                    str(r.get("URL", "")),
                                    str(r.get("Keyword", "")),
                                    case_sensitive,
                                    timeout,
                                )
                                fmap[f] = r

                            # FIX 8: future.result(timeout=) prevents Per-future hard deadline
                            for fut in as_completed(fmap):
                                if not st.session_state.running:
                                    _log("⏹ Stopped by user.")
                                    ex.shutdown(wait=False, cancel_futures=True)
                                    break

                                try:
                                    # FIX 8: cap worker time at timeout + 30 s grace
                                    res = fut.result(timeout=timeout + 30)
                                except Exception as e:
                                    src_row = fmap[fut]
                                    res = {
                                        "URL":                 str(src_row.get("URL", "")),
                                        "Keyword":             str(src_row.get("Keyword", "")),
                                        "Extraction Option":   "",
                                        "URL_Status":          0,
                                        "URL_Search_Status":   f"Exception: {e}",
                                        "Keyword_Status":      None,
                                        "feature_name":        str(src_row.get("Keyword", "")),
                                        "feature_value":       "",
                                        "Keyword_Search_Status": S.FAILED,
                                        "_retry":              True,
                                    }

                                completed[0] += 1
                                kss = res.get("Keyword_Search_Status", "")
                                _log(f"[{label}][{completed[0]}/{total}] "
                                     f"{kss[:18]:18s} …{str(res.get('URL',''))[-45:]}")

                                pass_results.append(res)

                                # FIX 9: retry flag set inside process_one_url
                                if res.get("_retry"):
                                    pass_retry.append(fmap[fut])

                                # Autosave every 100 rows (FIX 5)
                                if completed[0] % 100 == 0:
                                    _autosave(all_results + pass_results, completed[0], total)
                                    _log(f"💾 Saved {completed[0]:,} rows to disk")

                                # Refresh UI
                                every = max(1, min(20, total // 50))
                                if completed[0] % every == 0 or completed[0] == total:
                                    _refresh_ui(label)

                        return pass_results, pass_retry

                    # ── Pass 1 ─────────────────────────────────────────────
                    _log(f"🚀 Pass 1 — {total:,} URLs · {workers} workers")
                    p1_res, p1_retry = _run_pass(rows, "Pass1")
                    all_results.extend(p1_res)

                    # ── Pass 2 — retry transient failures ──────────────────
                    if p1_retry and st.session_state.running:
                        _log(f"♻️ Pass 2 — retrying {len(p1_retry):,} failed URLs…")
                        # Remove pass-1 error rows; replace with pass-2 results
                        retry_keys = {r.get("URL","") + "|" + r.get("Keyword","")
                                      for r in p1_retry}
                        all_results = [r for r in all_results
                                       if r.get("URL","") + "|" + r.get("Keyword","")
                                       not in retry_keys]
                        total += len(p1_retry)
                        _log("⏳ 12 s cooldown before Pass 2…")
                        time.sleep(12)
                        p2_res, still = _run_pass(p1_retry, "Pass2", extra_delay=0.5)
                        all_results.extend(p2_res)
                        msg = "✅ All resolved in Pass 2" if not still \
                              else f"⚠️ {len(still):,} still failed"
                        _log(msg)

                    st.session_state.running = False

                    if all_results:
                        final_df = _build_df(all_results)
                        st.session_state.results_df = final_df
                        _autosave(all_results, completed[0], total)
                        elapsed = time.time() - start_ts
                        prog_bar.progress(1.0, text="✅ Search Complete!")
                        st.success(
                            f"✅ **{completed[0]:,} URLs** · "
                            f"**{elapsed:.1f}s** · "
                            f"**{completed[0]/elapsed:.1f} URLs/s**"
                        )
                    else:
                        st.warning("No results collected.")

        except Exception as exc:
            st.error(f"❌ Failed to load file: {exc}")


# ══════════════════════════════════════════════════════════════════
# TAB 2 — RESULTS
# ══════════════════════════════════════════════════════════════════
with tab_results:
    rdf = st.session_state.results_df

    if rdf is None:
        st.info("🔍 Run a search first — results will appear here.")
    else:
        st.markdown("##### 📊 Summary")
        _stat_row(rdf)
        st.markdown("---")

        # Filters
        t = _theme()
        fc1, fc2, fc3 = st.columns([2, 2, 2])
        with fc1:
            all_statuses = rdf["Keyword_Search_Status"].unique().tolist()
            sel = st.multiselect("Filter by Status", all_statuses, default=all_statuses)
        with fc2:
            kw_flt = st.text_input("Keyword contains", "")
        with fc3:
            url_flt = st.text_input("URL contains", "")

        flt = rdf[rdf["Keyword_Search_Status"].isin(sel)]
        if kw_flt:
            flt = flt[flt["Keyword"].astype(str).str.contains(kw_flt, case=False, na=False)]
        if url_flt:
            flt = flt[flt["URL"].astype(str).str.contains(url_flt, case=False, na=False)]

        st.caption(f"Showing **{len(flt):,}** of {len(rdf):,} rows")

        if len(flt) <= 5000:
            fn = getattr(flt.style, "map", None) or flt.style.applymap
            st.dataframe(fn(_status_style, subset=["Keyword_Search_Status"]),
                         use_container_width=True, height=440)
        else:
            st.dataframe(flt, use_container_width=True, height=440)

        st.markdown("---")
        st.markdown("##### ⬇️ Download Results")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        dl1, dl2 = st.columns(2)
        with dl1:
            try:
                st.download_button(
                    "📥 Excel (.xlsx) — 6 sheets",
                    data=df_to_excel_bytes(flt),
                    file_name=f"keyword_search_{ts}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True, type="primary",
                )
            except Exception as exc:
                st.error(f"Excel export failed: {exc}. Use CSV.")
        with dl2:
            st.download_button(
                "📥 CSV (.csv)",
                data=df_to_csv_bytes(flt),
                file_name=f"keyword_search_{ts}.csv",
                mime="text/csv",
                use_container_width=True,
            )

        st.markdown("---")
        st.markdown("##### 📈 Result Distribution")
        chart = rdf["Keyword_Search_Status"].value_counts().reset_index()
        chart.columns = ["Status","Count"]
        st.bar_chart(chart.set_index("Status"))


# ══════════════════════════════════════════════════════════════════
# TAB 3 — GUIDE
# ══════════════════════════════════════════════════════════════════
with tab_guide:
    st.markdown(f"""
## 📖 User Guide

### How to Use
1. **Download the Template** from the sidebar
2. Fill in `URL` and `Keyword` columns
3. Upload on the **Search** tab
4. Adjust Workers / Timeout in the sidebar
5. Click **Start Search** — watch live progress
6. Go to **Results** tab → filter → download

---

### Multi-Keyword Format
Separate keywords with `|` (pipe):

| Keyword cell | Keywords searched |
|---|---|
| `8536507000` | one keyword |
| `8536507000 \| 8536.50` | two keywords |
| `PartNo \| HsCode \| EAN` | three keywords |

Row is **Found** if **any** keyword is present in the document.

---

### The 5 Output Statuses

| Status | Meaning |
|---|---|
| ✅ **Found** | Keyword located in document |
| ❌ **Not Found** | Document read successfully — keyword absent |
| 🟡 **PDF is Non searchable…** | Scanned / image PDF — no text layer |
| 🟣 **PDF Not mirrored / Corrupted** | Damaged, empty, or unreadable file |
| 🔴 **Failed to get PDF text** | Could not download or access the document |

> **Not Found ≠ Failed**: *Not Found* means the file was read but the keyword isn't there.
> *Failed* means the file could not be retrieved at all.

---

### Output Columns

| Column | Description |
|---|---|
| `URL` | Original URL |
| `Keyword` | Keyword(s) as entered |
| `Extraction Option` | PDF or HTML |
| `URL_Status` | 3 = OK · 0 = Error |
| `URL_Search_Status` | "Done" when processed |
| `Keyword_Status` | 3.0 when keyword was evaluated |
| `feature_name` | Keyword(s) that matched |
| `feature_value` | Context snippet around first match |
| `Keyword_Search_Status` | **Main result** |

### Excel Output — 6 Sheets
All Results · Found · Not Found · Scanned · Corrupted · Failed

---

### Retry Logic
| Pass | What happens |
|---|---|
| **Pass 1** | Every URL — up to 3 attempts + exponential back-off |
| **Mirror fallback** | 403/timeout → tries `source1.z2data.com` automatically |
| **12 s cooldown** | Pause before Pass 2 |
| **Pass 2** | Only failed URLs retried with 0.5 s stagger |

---

### Auto-Save & Recovery
- Results saved to disk every **100 rows**
- Survives page refresh, browser close, or Stop
- **Recovery panel** in sidebar: restore, download, or clear saved data

---

### Performance Tips
- Default **6 workers** — safe for z2data.com
- Raise to **10–12** for faster runs if no failures appear
- **Timeout 20 s** for most PDFs; raise to 40 s for large files
- Limit: **{SEARCH_LIMIT:,} rows** per run
""")
