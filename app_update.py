"""
PDF Keyword Search System — v3.0
Single • Multi • Table (HTS) Search Engine

Architecture:
  - Section 1  : Constants + Status enum
  - Section 2  : Network layer (session, rate-limiter, block-detector)
  - Section 3  : URL cache + alternate URL logic
  - Section 4  : Download with retry + error classification
  - Section 5  : Text extraction (PDF + HTML)
  - Section 6  : Text normalization + multi-mode keyword search
  - Section 7  : DataFrame / Excel / CSV helpers + autosave
  - Section 8  : Streamlit UI (sidebar, tabs: Search / Results / Logs / Guide)
"""

# ═══════════════════════════════════════════════════════════════════
# IMPORTS
# ═══════════════════════════════════════════════════════════════════
import streamlit as st
import pandas as pd
import io, os, re, time, random, threading, tempfile
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from html.parser import HTMLParser

import fitz                              # PyMuPDF
import urllib3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ═══════════════════════════════════════════════════════════════════
# SECTION 1 — Page config + CSS
# ═══════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="PDF Keyword Search System",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
/* ── Base ─────────────────────────────────────────────────────── */
.main { background-color: #0f1117; }
section[data-testid="stSidebar"] {
    background: #0d1117;
    border-right: 1px solid #1e2536;
}
/* ── Header ──────────────────────────────────────────────────── */
.hdr-wrap {
    background: linear-gradient(135deg,#0d1b2a 0%,#1b2838 60%,#0d2137 100%);
    border: 1px solid #1e3a5f; border-radius: 14px;
    padding: 22px 32px 18px; margin-bottom: 20px;
    display: flex; align-items: center; justify-content: space-between;
}
.hdr-left h1 { font-size:1.9rem; font-weight:800; color:#00d4ff; margin:0; }
.hdr-left p  { font-size:0.88rem; color:#6b7fa3; margin:4px 0 0; }
.hdr-badge {
    font-size:0.82rem; font-weight:700; padding:6px 14px;
    border-radius:20px; letter-spacing:0.4px;
}
.badge-ready   { background:#0d3d1e; color:#00e676; border:1px solid #00e676; }
.badge-running { background:#3d2e00; color:#ffd600; border:1px solid #ffd600; }
.badge-stopped { background:#3d0d0d; color:#ff5252; border:1px solid #ff5252; }
/* ── Stat cards ───────────────────────────────────────────────── */
.stat-card {
    background:#131925; border-radius:10px;
    padding:16px 18px; text-align:center;
    border:1px solid #1e2a3d;
}
.stat-num { font-size:1.9rem; font-weight:800; line-height:1; }
.stat-lbl { font-size:0.72rem; color:#6b7fa3; margin-top:5px;
            text-transform:uppercase; letter-spacing:0.6px; }
/* ── Limit warning ─────────────────────────────────────────────── */
.limit-warn {
    background:linear-gradient(90deg,#b34700,#e65c00);
    color:#fff; border-radius:8px; padding:9px 16px;
    font-weight:700; font-size:1rem; text-align:center;
}
/* ── Progress log box ──────────────────────────────────────────── */
.log-box {
    background:#0d1117; border:1px solid #1e2536;
    border-radius:10px; padding:16px 18px;
    font-family:"Courier New",monospace; font-size:0.8rem;
    color:#8892a4; max-height:260px; overflow-y:auto;
    line-height:1.55;
}
/* ── Tabs ──────────────────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] { gap:6px; border-bottom:1px solid #1e2536; }
.stTabs [data-baseweb="tab"] {
    background:#131925; border-radius:8px 8px 0 0;
    color:#6b7fa3; font-weight:600; padding:8px 18px;
    border:1px solid #1e2536; border-bottom:none;
}
.stTabs [aria-selected="true"] {
    background:#1e2a3d !important;
    color:#00d4ff !important;
    border-color:#00d4ff !important;
}
/* ── Buttons ───────────────────────────────────────────────────── */
.stButton > button {
    border-radius:8px; font-weight:700;
    transition:all 0.18s ease;
}
.stButton > button:hover { transform:translateY(-1px); }
/* ── Pre-processing summary box ─────────────────────────────────── */
.preproc-box {
    background:#131925; border:1px solid #1e2a3d;
    border-radius:10px; padding:16px 20px; margin:12px 0;
}
.preproc-row { display:flex; justify-content:space-between;
               padding:4px 0; font-size:0.88rem; }
.preproc-key { color:#6b7fa3; }
.preproc-val { color:#e0e6f0; font-weight:600; }
/* ── Section divider ────────────────────────────────────────────── */
.sec-div { border:none; border-top:1px solid #1e2536; margin:16px 0; }
</style>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════
# SECTION 2 — Constants + Status enum
# ═══════════════════════════════════════════════════════════════════
SEARCH_LIMIT       = 50_000
DEFAULT_WORKERS    = 6
DEFAULT_TIMEOUT    = 20
_CONNECT_TIMEOUT   = 12     # TCP connect (separate from read)
_MIN_DELAY_SECS    = 0.3    # min gap per host across all threads
_BLOCK_THRESHOLD   = 5      # consecutive failures → blocked
_BLOCK_COOLDOWN    = 30     # seconds to wait when blocked

_ILLEGAL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")

_AUTOSAVE_PATH = os.path.join(tempfile.gettempdir(), "pdf_search_autosave.csv")


class S:
    """Single source of truth for all Keyword_Search_Status values."""
    FOUND          = "Found"
    NOT_FOUND      = "Not Found"
    PARTIAL        = "Partial Match"          # multi-mode: some keywords found
    URL_NOT_FOUND  = "URL Not Found (404)"
    HTML_NOT_FOUND = "URL Not Found (HTML page returned)"
    SSL_ERROR      = "SSL Error"
    TIMEOUT        = "Timeout"
    BLOCKED        = "Blocked / Rate Limited"
    CORRUPTED      = "PDF Not mirrored / Corrupted"
    SCANNED        = "PDF is Non searchable, Advanced Scanned Extraction can make the PDF searchable."
    INVALID_URL    = "Invalid URL"
    CONNECTION_ERR = "Connection Error"


# Status groups used by error-pass detection
_ERROR_STATUSES = {
    S.URL_NOT_FOUND, S.SSL_ERROR, S.TIMEOUT,
    S.BLOCKED, S.CORRUPTED, S.INVALID_URL, S.CONNECTION_ERR,
}

# Output columns — canonical order
_OUT_COLS = [
    "URL", "Keyword", "Search Mode", "Extraction Option",
    "URL_Status", "URL_Search_Status",
    "Keyword_Status", "feature_name",
    "feature_value", "match_count",
    "Keyword_Search_Status",
]

# Phrases injected by servers into 200-OK "not found" HTML pages
_HTML_NFP = [
    "page not found", "404 not found", "404 error",
    "file not found", "resource not found", "does not exist",
    "error 404", "no results found", "page unavailable",
    "page cannot be found", "the page you requested",
    "sorry, we couldn", "could not be found",
    "this page doesn", "we couldn",
]

# ═══════════════════════════════════════════════════════════════════
# SECTION 3 — Network layer
# ═══════════════════════════════════════════════════════════════════
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
    "Referer":         "https://source.z2data.com/",
}


def _make_session() -> requests.Session:
    """
    Fresh session per thread.
    - Retry(total=0): urllib3 never retries; our loop controls every attempt.
    - verify=False: handles SSL cert mismatches on source1.z2data.com.
    - pool_connections=20: one slot per max worker thread.
    """
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


# ── Rate limiter ──────────────────────────────────────────────────
_rate_lock    = threading.Lock()
_last_req: dict[str, float] = {}

def _rate_limit(host: str) -> None:
    with _rate_lock:
        gap = time.time() - _last_req.get(host, 0)
        if gap < _MIN_DELAY_SECS:
            time.sleep(_MIN_DELAY_SECS - gap)
        _last_req[host] = time.time()


# ── Block detector ────────────────────────────────────────────────
_block_lock    = threading.Lock()
_consec_fail:  dict[str, int]   = {}
_blocked_until: dict[str, float] = {}

def _record_failure(host: str) -> None:
    with _block_lock:
        _consec_fail[host] = _consec_fail.get(host, 0) + 1
        if _consec_fail[host] >= _BLOCK_THRESHOLD:
            _blocked_until[host] = time.time() + _BLOCK_COOLDOWN
            _consec_fail[host]   = 0

def _record_success(host: str) -> None:
    with _block_lock:
        _consec_fail[host] = 0

def _wait_if_blocked(host: str) -> None:
    wait = _blocked_until.get(host, 0) - time.time()
    if wait > 0:
        time.sleep(wait)


def _host(url: str) -> str:
    try:    return url.split("/")[2]
    except: return url

# ═══════════════════════════════════════════════════════════════════
# SECTION 4 — URL cache + alternate URL logic
# ═══════════════════════════════════════════════════════════════════
_cache_lock = threading.Lock()
_url_cache:  dict[str, tuple[str, str]] = {}   # url → (text, extraction_status)

def _clear_cache() -> None:
    """Reset ALL per-run state: text cache, block detector, AND rate-limiter timing.
    Bug 2 fix: _last_req was not cleared, causing rate-limit delays to carry
    over from a previous run and throttle the start of a new one."""
    with _cache_lock:
        _url_cache.clear()
    with _block_lock:
        _consec_fail.clear()
        _blocked_until.clear()
    with _rate_lock:          # BUG 2 FIX: reset request timestamps
        _last_req.clear()


def _get_alternate_urls(url: str) -> list[str]:
    """Mirror swap (source ↔ source1) + /web/ path stripping for z2data.com."""
    alts: list[str] = []
    if "//source1.z2data.com" in url:
        alts.append(url.replace("//source1.z2data.com", "//source.z2data.com", 1))
    elif "//source.z2data.com" in url:
        alts.append(url.replace("//source.z2data.com", "//source1.z2data.com", 1))
    if "/web/" in url:
        s = url.replace("/web/", "/", 1)
        alts.append(s)
        if "//source1.z2data.com" in s:
            alts.append(s.replace("//source1.z2data.com", "//source.z2data.com", 1))
        elif "//source.z2data.com" in s:
            alts.append(s.replace("//source.z2data.com", "//source1.z2data.com", 1))
    return alts

# ═══════════════════════════════════════════════════════════════════
# SECTION 5 — Download with retry + precise error classification
# ═══════════════════════════════════════════════════════════════════
def _download_one(url: str, timeout: int, fresh_session: bool = False
                  ) -> tuple[bytes | None, str, str]:
    """
    One HTTP GET attempt.
    Returns (content_bytes | None, category, detail).
    category: None | 'http_404' | 'blocked' | 'ssl' | 'timeout' | 'connection'
    """
    host = _host(url)
    _wait_if_blocked(host)
    _rate_limit(host)
    session = _get_session(fresh=fresh_session)

    try:
        resp = session.get(url, timeout=(_CONNECT_TIMEOUT, timeout),
                           stream=False, allow_redirects=True)
        if resp.status_code == 200:
            _record_success(host)
            return resp.content, "", ""
        _record_failure(host)
        code = resp.status_code
        if code in (403, 429):
            if code == 429:
                time.sleep(5 + random.random() * 5)
            return None, "blocked", f"HTTP {code}"
        if code in (404, 410):
            return None, "http_404", f"HTTP {code}"
        return None, f"http_{code}", f"HTTP {code}"

    except Exception as e:
        _record_failure(host)
        s = str(e).lower()
        if any(w in s for w in ("ssl", "certificate", "handshake", "tls")):
            return None, "ssl", str(e)[:150]
        if any(w in s for w in ("timed out", "timeout", "read timed")):
            return None, "timeout", str(e)[:150]
        return None, "connection", str(e)[:150]


def _fetch(url: str, session_timeout: int,
           use_mirror: bool = True) -> tuple[bytes | None, str]:
    """
    Full retry: primary URL + alternates, up to 3 attempts each.
    Returns (content | None, S.* status string).
    Permanent errors (404, blocked) exit immediately — no waste.
    use_mirror: when False, skips mirror/alternate URLs (Bug 3 fix).
    """
    urls = [url] + (_get_alternate_urls(url) if use_mirror else [])
    last_status = S.TIMEOUT
    is_conn_err = False

    for try_url in urls:
        for attempt in range(1, 4):
            content, cat, _ = _download_one(
                try_url, session_timeout, fresh_session=(is_conn_err and attempt == 1)
            )
            is_conn_err = False

            if content is not None:
                # BUG 6 FIX: size < 64 was too aggressive — some valid short
                # responses were rejected. Now we only reject if the content
                # is tiny AND starts with no recognizable PDF/HTML signature.
                if len(content) < 32:
                    last_status = S.CORRUPTED
                    break
                # Accept any content that has a PDF header or HTML tag
                _snippet_check = content[:64].lstrip()
                _is_pdf  = _snippet_check.startswith(b"%PDF")
                _is_html = (b"<html" in _snippet_check.lower() or
                            b"<!doc" in _snippet_check.lower() or
                            b"<head" in _snippet_check.lower())
                if len(content) < 64 and not _is_pdf and not _is_html:
                    last_status = S.CORRUPTED
                    break
                return content, "ok"

            if cat == "http_404":
                return None, S.URL_NOT_FOUND      # permanent — no further tries
            if cat == "blocked":
                last_status = S.BLOCKED
                break
            if cat == "ssl":
                last_status = S.SSL_ERROR
                break
            if cat == "timeout":
                last_status, is_conn_err = S.TIMEOUT, True
            if cat and cat.startswith("connection"):
                last_status, is_conn_err = S.CONNECTION_ERR, True

            if attempt < 3:
                time.sleep((2 ** attempt) * (0.75 + 0.5 * random.random()))

    return None, last_status

# ═══════════════════════════════════════════════════════════════════
# SECTION 6 — Text extraction
# ═══════════════════════════════════════════════════════════════════
def _extract_pdf(data: bytes) -> tuple[str, str]:
    try:
        doc   = fitz.open(stream=data, filetype="pdf")
        pages = [p.get_text("text") for p in doc]
        doc.close()
        text  = "\n".join(pages)
        return (text, "searchable") if text.strip() else ("", "scanned")
    except Exception as e:
        return "", f"error:{e}"


class _TE(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts: list[str] = []
        self._skip = False
    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style", "noscript", "head"):
            self._skip = True
    def handle_endtag(self, tag):
        if tag in ("script", "style", "noscript", "head"):
            self._skip = False
    def handle_data(self, data):
        if not self._skip:
            s = data.strip()
            if s: self.parts.append(s)


def _extract_html(data: bytes) -> tuple[str, str]:
    try:
        html_str = None
        for enc in ("utf-8", "latin-1", "cp1252"):
            try:
                html_str = data.decode(enc); break
            except Exception:
                pass
        if not html_str:
            html_str = data.decode("utf-8", errors="replace")
        p = _TE()
        p.feed(html_str)
        text = "\n".join(p.parts)
        return (text, "searchable") if text.strip() else ("", "scanned")
    except Exception as e:
        return "", f"error:{e}"


def _get_text_cached(url: str, content: bytes, is_html: bool) -> tuple[str, str]:
    """
    Text extraction with URL-level cache.
    Same PDF downloaded for keyword A is reused for keyword B — no re-extraction.
    """
    with _cache_lock:
        if url in _url_cache:
            return _url_cache[url]

    if is_html:
        text, status = _extract_html(content)
        if not text and "error:" not in status:
            text, status = _extract_pdf(content)
    else:
        text, status = _extract_pdf(content)
        if "error:" in status:
            text, status = _extract_html(content)

    with _cache_lock:
        _url_cache[url] = (text, status)
    return text, status


def _is_html_not_found(text: str) -> bool:
    """
    Detect 200-OK responses that are actually 'not found' error pages.
    BUG 7 FIX: old code skipped pages longer than 2000 chars — many real
    'not found' pages include nav/footer boilerplate making them much larger.
    Now we always check the first 1000 chars of the visible text body,
    regardless of total length, and use a richer phrase set.
    """
    if not text:
        return False
    # Check first 1000 chars of extracted text (post-HTML-stripping)
    # This is fast and covers the <title> + main heading area.
    sample = text[:1000].lower()
    return any(p in sample for p in _HTML_NFP)

# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — Text normalization + multi-mode keyword search
# ═══════════════════════════════════════════════════════════════════
def _normalize(text: str) -> str:
    """
    Normalize extracted text before search:
    - Strip control chars
    - Join hyphenated line-breaks: 'key-\\nword' → 'keyword'
    - Collapse whitespace
    - Normalize non-breaking space, soft-hyphen
    """
    text = _ILLEGAL_RE.sub(" ", text)
    text = text.replace("\u00ad", "")    # soft hyphen
    text = text.replace("\u00a0", " ")   # non-breaking space
    text = re.sub(r"-\s*\n\s*", "", text)
    text = re.sub(r"\s+", " ", text)
    return text


def _parse_keywords(raw_keyword: str, mode: str) -> list[str]:
    """
    Split keyword field based on search mode.
    - single  : whole string is one keyword
    - multi   : split on '|'
    - table   : split on '|' then normalize (strip leading zeros etc.)
    - auto    : detect from content
    """
    raw = str(raw_keyword).strip()
    if mode == "single":
        return [raw]
    if mode in ("multi", "table"):
        parts = [k.strip() for k in raw.split("|") if k.strip()]
        return parts if parts else [raw]
    # auto-detect
    if "|" in raw:
        parts = [k.strip() for k in raw.split("|") if k.strip()]
        return parts
    return [raw]


def _search_one(text: str, keyword: str, case_sensitive: bool) -> tuple[bool, int]:
    """Search text for a single keyword. Returns (found, count)."""
    if not text or not keyword:
        return False, 0
    flags = 0 if case_sensitive else re.IGNORECASE
    try:
        matches = list(re.finditer(re.escape(keyword.strip()), text, flags))
        return (len(matches) > 0), len(matches)
    except re.error:
        return False, 0


def _search_multi(text: str, keywords: list[str], case_sensitive: bool,
                  match_all: bool) -> tuple[str, list[str], list[str], int]:
    """
    Multi-keyword search.
    Returns (status, found_keywords, missing_keywords, total_match_count).
    status: S.FOUND | S.NOT_FOUND | S.PARTIAL
    """
    found_kws:   list[str] = []
    missing_kws: list[str] = []
    total = 0

    for kw in keywords:
        ok, cnt = _search_one(text, kw, case_sensitive)
        if ok:
            found_kws.append(kw)
            total += cnt
        else:
            missing_kws.append(kw)

    if not keywords:
        return S.NOT_FOUND, [], [], 0

    if len(found_kws) == len(keywords):
        return S.FOUND, found_kws, [], total
    if found_kws:
        if match_all:
            return S.PARTIAL, found_kws, missing_kws, total
        else:
            return S.FOUND, found_kws, missing_kws, total   # ANY mode → found
    return S.NOT_FOUND, [], missing_kws, 0


def _snippet(text: str, keyword: str, ctx: int = 100) -> str:
    """Richest unique context window around any match of keyword in text."""
    if not text or not keyword:
        return ""
    kw = str(keyword).lower()
    tl = text.lower()
    pos, start, best, seen = [], 0, "", set()
    while True:
        i = tl.find(kw, start)
        if i == -1: break
        pos.append(i); start = i + 1
    for i in pos:
        s = max(0, i - ctx); e = min(len(text), i + len(kw) + ctx)
        raw = _ILLEGAL_RE.sub("", text[s:e].strip())
        if not raw or raw in seen: continue
        seen.add(raw)
        cand = ("…" if s > 0 else "") + raw + ("…" if e < len(text) else "")
        if len(raw) > len(best.replace("…", "")):
            best = cand
    return best

# ═══════════════════════════════════════════════════════════════════
# SECTION 8 — Main per-URL processor
# ═══════════════════════════════════════════════════════════════════
def process_one_url(url: str, raw_keyword: str, search_mode: str,
                    match_all: bool, case_sensitive: bool,
                    session_timeout: int, row_id: int = 0,
                    use_mirror: bool = True,
                    use_smart_detect: bool = True) -> dict:
    """
    Download + extract + search.  Always returns exactly ONE dict per call.
    Supports single / multi / table / auto modes.
    row_id          : stable per-input-row ID for retry tracking (Bug 1 fix).
    use_mirror      : when False, skips mirror URL fallback (Bug 3 fix).
    use_smart_detect: when False, skips HTML not-found page detection (Bug 3 fix).
    """
    base: dict = {
        "_row_id": row_id,
        "URL": url, "Keyword": raw_keyword,
        "Search Mode": search_mode.capitalize(),
        "Extraction_Option": "",
        "URL_Status": None,    "URL_Search_Status": "",
        "Keyword_Status": None, "feature_name": raw_keyword,
        "feature_value": None,  "match_count": 0,
        "Keyword_Search_Status": "",
        "matched_keywords": "",  "missing_keywords": "",
    }

    def row(**kw) -> dict:
        r = dict(base); r.update(kw); return r

    url = str(url).strip()
    if not url or not url.startswith("http"):
        return row(URL_Status=0, URL_Search_Status=S.INVALID_URL,
                   Keyword_Search_Status=S.INVALID_URL)

    is_html = url.lower().split("?")[0].endswith((".html", ".htm"))
    # BUG 4 FIX: set Extraction Option now so it appears in every row
    base["Extraction_Option"] = "HTML" if is_html else "PDF"

    # ── Download ──────────────────────────────────────────────────
    content, dl_status = _fetch(url, session_timeout, use_mirror=use_mirror)
    if content is None:
        return row(URL_Status=0, URL_Search_Status=dl_status,
                   Keyword_Search_Status=dl_status)

    # ── Extract (cached per URL) ───────────────────────────────────
    text, ext_status = _get_text_cached(url, content, is_html)

    # BUG 4 FIX: refine Extraction Option to reflect what was actually parsed
    # (_get_text_cached may fall back from PDF→HTML or HTML→PDF)
    if ext_status in ("searchable", "scanned"):
        base["Extraction_Option"] = "HTML" if is_html else "PDF"

    if "error:" in ext_status:
        return row(URL_Status=3, URL_Search_Status="Done",
                   Keyword_Search_Status=S.CORRUPTED)

    if ext_status == "scanned":
        return row(URL_Status=3, URL_Search_Status="Done",
                   Keyword_Status=None, Keyword_Search_Status=S.SCANNED)

    norm_text = _normalize(text)

    # BUG 3 FIX: enable_smart controls whether we detect HTML "not found" pages
    if use_smart_detect and _is_html_not_found(norm_text):
        return row(URL_Status=3, URL_Search_Status="Done",
                   Keyword_Search_Status=S.HTML_NOT_FOUND)

    # ── Multi-keyword search ───────────────────────────────────────
    keywords = _parse_keywords(raw_keyword, search_mode)
    status, found_kws, missing_kws, total_count = _search_multi(
        norm_text, keywords, case_sensitive, match_all
    )

    # Build context snippet from the first found keyword
    snippet = ""
    if found_kws:
        snippet = _snippet(norm_text, found_kws[0])

    # Build match details string for the results table
    match_details = ""
    if len(keywords) > 1:
        parts = []
        if found_kws:
            parts.append(f"✅ {len(found_kws)}/{len(keywords)}: {', '.join(found_kws)}")
        if missing_kws:
            parts.append(f"❌ Missing: {', '.join(missing_kws)}")
        match_details = " | ".join(parts)

    return row(
        URL_Status=3, URL_Search_Status="Done",
        Keyword_Status=3.0,
        feature_value=snippet,
        match_count=total_count,
        Keyword_Search_Status=status,
        matched_keywords=", ".join(found_kws),
        missing_keywords=", ".join(missing_kws),
        match_details=match_details,
    )

# ═══════════════════════════════════════════════════════════════════
# SECTION 9 — DataFrame / Excel / CSV helpers
# ═══════════════════════════════════════════════════════════════════
def _clean_cell(v):
    if isinstance(v, str):
        return _ILLEGAL_RE.sub("", v)
    return v


def _build_df(result_dicts: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(result_dicts)
    df.rename(columns={"Extraction_Option": "Extraction Option"}, inplace=True)
    for c in _OUT_COLS:
        if c not in df.columns:
            df[c] = None
    return df[_OUT_COLS]


def df_to_excel_bytes(df: pd.DataFrame) -> bytes:
    fn = getattr(df, "map", None) or df.applymap   # pandas ≥2.1 renamed applymap → map
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        fn(_clean_cell).to_excel(w, index=False, sheet_name="Results")
    return buf.getvalue()


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def apply_status_badge(val) -> str:
    v = str(val)
    if v == S.FOUND:          return "background-color:#0d3d1e;color:#00e676"
    if v == S.NOT_FOUND:      return "background-color:#3d0d0d;color:#ff5252"
    if v == S.PARTIAL:        return "background-color:#1a2d00;color:#b2ff59"
    if "Non searchable" in v: return "background-color:#2d2600;color:#ffd600"
    if v == S.SSL_ERROR:      return "background-color:#1a0d3d;color:#b39ddb"
    if v == S.BLOCKED:        return "background-color:#2d1a00;color:#ffb74d"
    if v == S.TIMEOUT:        return "background-color:#0d1a2d;color:#90caf9"
    if v in (S.URL_NOT_FOUND, S.HTML_NOT_FOUND):
        return "background-color:#1a0d0d;color:#ef9a9a"
    if "Error" in v or "HTTP" in v or v in (S.CORRUPTED, S.CONNECTION_ERR):
        return "background-color:#2d0024;color:#f48fb1"
    return ""

# ═══════════════════════════════════════════════════════════════════
# SECTION 10 — Autosave / Recovery
# ═══════════════════════════════════════════════════════════════════
def _autosave(result_dicts: list[dict]) -> None:
    try:
        if result_dicts:
            _build_df(result_dicts).to_csv(_AUTOSAVE_PATH, index=False)
    except Exception:
        pass

def _load_autosave() -> pd.DataFrame | None:
    try:
        if os.path.exists(_AUTOSAVE_PATH) and os.path.getsize(_AUTOSAVE_PATH) > 0:
            return pd.read_csv(_AUTOSAVE_PATH, dtype={"Keyword": str})
    except Exception:
        pass
    return None

def _clear_autosave() -> None:
    try: os.remove(_AUTOSAVE_PATH)
    except Exception: pass

# ═══════════════════════════════════════════════════════════════════
# SECTION 11 — Session state
# ═══════════════════════════════════════════════════════════════════
for _k, _v in [
    ("results_df",  None),
    ("running",     False),
    ("paused",      False),
    ("log_lines",   []),
    ("error_log",   []),
]:
    if _k not in st.session_state:
        st.session_state[_k] = _v

# ═══════════════════════════════════════════════════════════════════
# SECTION 12 — UI helpers
# ═══════════════════════════════════════════════════════════════════
def _render_header() -> None:
    if st.session_state.running:
        badge = '<span class="hdr-badge badge-running">🟡 Running</span>'
    elif st.session_state.results_df is not None:
        badge = '<span class="hdr-badge badge-stopped">🔴 Stopped</span>'
    else:
        badge = '<span class="hdr-badge badge-ready">🟢 Ready</span>'

    st.markdown(f"""
    <div class="hdr-wrap">
        <div class="hdr-left">
            <h1>🔍 PDF Keyword Search System</h1>
            <p>Single &nbsp;•&nbsp; Multi &nbsp;•&nbsp; Table (HTS) &nbsp;&nbsp;Search Engine</p>
        </div>
        {badge}
    </div>""", unsafe_allow_html=True)


def _stat_card(col, val: int, color: str, label: str) -> None:
    with col:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-num" style="color:{color}">{val:,}</div>
            <div class="stat-lbl">{label}</div>
        </div>""", unsafe_allow_html=True)


def _render_stat_cards(total, found, not_found, partial, scanned, errors) -> None:
    cols = st.columns(6)
    _stat_card(cols[0], total,    "#00d4ff", "Total")
    _stat_card(cols[1], found,    "#00e676", "Found")
    _stat_card(cols[2], not_found,"#ff5252", "Not Found")
    _stat_card(cols[3], partial,  "#b2ff59", "Partial")
    _stat_card(cols[4], scanned,  "#ffd600", "Scanned")
    _stat_card(cols[5], errors,   "#f48fb1", "Errors")

# ═══════════════════════════════════════════════════════════════════
# SECTION 13 — SIDEBAR
# ═══════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("## ⚙️ Settings")
    st.markdown(f'<div class="limit-warn">⚠️ Limit: {SEARCH_LIMIT:,}</div>',
                unsafe_allow_html=True)

    # ── Section 1: Search Configuration ───────────────────────────
    st.markdown("---")
    st.markdown("### ⚡ Search Configuration")

    search_mode = st.radio(
        "🔘 Search Mode",
        options=["Auto Detect (Recommended)", "Single Keyword",
                 "Multi Keyword (|)", "Table Search (HTS)"],
        index=0,
        help=(
            "**Auto Detect:** inspects your keyword field and picks the right mode.\n\n"
            "**Single:** the whole keyword field is one search term.\n\n"
            "**Multi:** split on `|` — e.g. `EAN123|UPC456` searches both terms.\n\n"
            "**Table (HTS):** split on `|`, normalized for numeric codes — "
            "best for HTS commodity codes."
        ),
    )
    _mode_key = {
        "Auto Detect (Recommended)": "auto",
        "Single Keyword":            "single",
        "Multi Keyword (|)":         "multi",
        "Table Search (HTS)":        "table",
    }[search_mode]

    show_match_mode = _mode_key in ("multi", "table", "auto")
    match_all = False
    if show_match_mode:
        match_mode_label = st.radio(
            "🔘 Match Mode",
            ["Match ANY keyword", "Match ALL keywords"],
            index=0,
            help=(
                "**Match ANY:** result is Found if at least one keyword matches.\n\n"
                "**Match ALL:** result is Found only when every keyword matches; "
                "if some match, status = Partial Match."
            ),
        )
        match_all = match_mode_label == "Match ALL keywords"

    case_sensitive = st.checkbox(
        "Case-Sensitive Search", value=False,
        help=(
            "**OFF (default):** `EAN123` matches `ean123`, `Ean123` — "
            "recommended for barcodes.\n\n"
            "**ON:** exact case required."
        ),
    )

    # ── Section 2: Performance ────────────────────────────────────
    st.markdown("---")
    st.markdown("### 🚀 Performance Settings")

    workers = st.slider(
        "Concurrent Workers", 2, 12, DEFAULT_WORKERS, 1,
        help=(
            "Parallel downloads.\n\n"
            "• **2–4** — safest; use for z2data.com to avoid rate-limiting\n"
            "• **6** — balanced default\n"
            "• **10–12** — fast CDNs only\n\n"
            "Reduce if you see Blocked or Rate Limited errors."
        ),
    )
    timeout = st.slider(
        "Timeout per URL (sec)", 5, 60, DEFAULT_TIMEOUT, 5,
        help=(
            "Max wait per URL.\n\n"
            "• **5–10** — skip slow URLs fast\n"
            "• **20** — default\n"
            "• **40–60** — for slow/large PDFs"
        ),
    )

    # ── Section 3: Advanced Options ───────────────────────────────
    st.markdown("---")
    st.markdown("### 🛡 Advanced Options")
    enable_retry   = st.checkbox("☑ Enable Retry System",         value=True)
    enable_mirror  = st.checkbox("☑ Enable Mirror Fallback",      value=True)
    enable_smart   = st.checkbox("☑ Enable Smart Error Detection", value=True)

    output_format  = st.radio("Output Format", ["Excel (.xlsx)", "CSV (.csv)"], index=0)

    # ── Section 4: Templates ──────────────────────────────────────
    st.markdown("---")
    st.markdown("### 📥 Templates")
    _tmpl_single = pd.DataFrame({
        "URL": ["https://source.z2data.com/example.pdf"],
        "Keyword": ["51712160148"],
    })
    _tmpl_multi = pd.DataFrame({
        "URL": ["https://source.z2data.com/example.pdf"],
        "Keyword": ["51712160148|4015080000000"],
    })
    _tmpl_table = pd.DataFrame({
        "URL": ["https://source.z2data.com/example.pdf"],
        "Keyword": ["8471.30|8471.41|8471.49"],
    })
    tc1, tc2, tc3 = st.columns(3)
    with tc1:
        st.download_button("📄 Single", data=df_to_excel_bytes(_tmpl_single),
            file_name="template_single.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True)
    with tc2:
        st.download_button("📄 Multi", data=df_to_excel_bytes(_tmpl_multi),
            file_name="template_multi.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True)
    with tc3:
        st.download_button("📄 HTS", data=df_to_excel_bytes(_tmpl_table),
            file_name="template_hts.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True)

    # ── Section 5: Status Guide ───────────────────────────────────
    st.markdown("---")
    st.markdown("### ℹ️ Status Guide")
    st.markdown(f"""
- 🟢 **{S.FOUND}**
- 🔴 **{S.NOT_FOUND}**
- 🟩 **{S.PARTIAL}** — multi-mode, some match
- 🟡 **Scanned** — image PDF, no text
- 🟣 **{S.SSL_ERROR}**
- 🟠 **{S.BLOCKED}**
- 🔵 **{S.TIMEOUT}**
- 🔻 **{S.URL_NOT_FOUND}**
- 🔻 **{S.HTML_NOT_FOUND}**
- ⚫ **{S.CORRUPTED}**
""")

    # ── Section 6: Auto-Save / Recovery ───────────────────────────
    st.markdown("---")
    st.markdown("### 💾 Auto-Save / Recovery")
    _saved = _load_autosave()
    if _saved is not None:
        _n = len(_saved)
        st.success(f"📂 **{_n:,} rows** saved on disk")
        _ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        st.download_button(
            f"📥 CSV ({_n:,} rows)",
            data=_saved.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"autosave_{_ts}.csv", mime="text/csv",
            use_container_width=True, key="sb_csv",
        )
        try:
            st.download_button(
                f"📥 Excel ({_n:,} rows)",
                data=df_to_excel_bytes(_saved),
                file_name=f"autosave_{_ts}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True, key="sb_xlsx",
            )
        except Exception:
            pass
        r1, r2 = st.columns(2)
        with r1:
            if st.button("♻️ Restore", use_container_width=True, key="restore"):
                st.session_state.results_df = _saved
                st.success("✅ Restored!")
        with r2:
            if st.button("🗑 Clear", use_container_width=True, key="clear"):
                _clear_autosave(); st.rerun()
    else:
        st.caption("No saved data. Results auto-save every 100 rows during a search.")

# ═══════════════════════════════════════════════════════════════════
# SECTION 14 — MAIN CONTENT
# ═══════════════════════════════════════════════════════════════════
_render_header()

tab_search, tab_results, tab_logs, tab_guide = st.tabs(
    ["🔍 Search", "📊 Results", "📜 Logs", "📖 Guide"]
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 1 — SEARCH
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_search:

    # ── Section A: File Upload ─────────────────────────────────────
    col_up, col_fmt = st.columns([2, 1])
    with col_up:
        st.markdown("### 📁 Upload Input File")
        st.markdown("Excel or CSV with **`URL`** and **`Keyword`** columns.")
        uploaded_file = st.file_uploader(
            "Drop file here", type=["xlsx", "xls", "csv"],
            label_visibility="collapsed",
        )
    with col_fmt:
        st.markdown("### 📌 Required Format")
        st.markdown("""
| Column | Example |
|--------|---------|
| `URL` | `https://…/file.pdf` |
| `Keyword` | `51712160148` |

For Multi/HTS separate with `|`  
e.g. `EAN123|UPC456`
""")

    if uploaded_file:
        try:
            if uploaded_file.name.endswith(".csv"):
                input_df = pd.read_csv(uploaded_file, dtype={"Keyword": str})
            else:
                input_df = pd.read_excel(uploaded_file, dtype={"Keyword": str})

            input_df.columns = [c.strip() for c in input_df.columns]
            if "URL" not in input_df.columns and "Offline" in input_df.columns:
                input_df.rename(columns={"Offline": "URL"}, inplace=True)

            missing_cols = [c for c in ["URL", "Keyword"] if c not in input_df.columns]
            if missing_cols:
                st.error(f"❌ Missing columns: **{', '.join(missing_cols)}**. "
                         f"Found: {input_df.columns.tolist()}")
            else:
                input_df = input_df.dropna(subset=["URL"]).reset_index(drop=True)
                total_rows  = len(input_df)
                unique_urls = input_df["URL"].nunique()
                dup_urls    = total_rows - unique_urls

                if total_rows > SEARCH_LIMIT:
                    st.warning(f"⚠️ Only first **{SEARCH_LIMIT:,}** rows will be processed.")
                    input_df = input_df.head(SEARCH_LIMIT)
                    total_rows = SEARCH_LIMIT

                st.success(f"✅ File loaded — **{total_rows:,}** rows")

                # ── Section B: Input Preview ───────────────────────
                with st.expander("🔎 Preview (first 10 rows)", expanded=False):
                    st.dataframe(input_df.head(10), use_container_width=True)

                st.markdown("<hr class='sec-div'>", unsafe_allow_html=True)

                # ── Section C: Pre-Processing Summary ─────────────
                st.markdown("### 📋 Pre-Processing Summary")

                # Auto-detect search mode for summary
                auto_mode_desc = _mode_key
                if _mode_key == "auto":
                    sample = str(input_df["Keyword"].iloc[0]) if len(input_df) else ""
                    if "|" in sample:
                        auto_mode_desc = "Multi Keyword (auto-detected)"
                    elif re.fullmatch(r"[\d.]+", sample.split("|")[0].strip()):
                        auto_mode_desc = "Table/Numeric (auto-detected)"
                    else:
                        auto_mode_desc = "Single Keyword (auto-detected)"

                st.markdown(f"""
<div class="preproc-box">
  <div class="preproc-row"><span class="preproc-key">Total Rows</span>
    <span class="preproc-val">{total_rows:,}</span></div>
  <div class="preproc-row"><span class="preproc-key">Unique URLs</span>
    <span class="preproc-val">{unique_urls:,}</span></div>
  <div class="preproc-row"><span class="preproc-key">Duplicate URLs (will use cached text)</span>
    <span class="preproc-val">{dup_urls:,}</span></div>
  <div class="preproc-row"><span class="preproc-key">Search Mode</span>
    <span class="preproc-val">{auto_mode_desc}</span></div>
  <div class="preproc-row"><span class="preproc-key">Match Mode</span>
    <span class="preproc-val">{"Match ALL" if match_all else "Match ANY"}</span></div>
  <div class="preproc-row"><span class="preproc-key">Case-Sensitive</span>
    <span class="preproc-val">{"Yes" if case_sensitive else "No"}</span></div>
  <div class="preproc-row"><span class="preproc-key">Workers / Timeout</span>
    <span class="preproc-val">{workers} / {timeout}s</span></div>
</div>
""", unsafe_allow_html=True)

                st.markdown("<hr class='sec-div'>", unsafe_allow_html=True)

                # ── Section D: Action Buttons ──────────────────────
                bc1, bc2, bc3, _ = st.columns([2, 1, 1, 1])
                with bc1:
                    start_btn = st.button(
                        "🚀 Start Search", use_container_width=True,
                        type="primary",
                        disabled=st.session_state.running,
                    )
                with bc2:
                    if st.button("⏸ Pause / Resume", use_container_width=True,
                                 disabled=not st.session_state.running):
                        st.session_state.paused = not st.session_state.paused
                with bc3:
                    if st.button("⏹ Stop", use_container_width=True,
                                 disabled=not st.session_state.running):
                        st.session_state.running = False
                        st.session_state.paused  = False

                # ── Section E: Progress Area ───────────────────────
                if start_btn and not st.session_state.running:
                    st.session_state.running = True
                    st.session_state.paused  = False
                    st.session_state.results_df = None
                    st.session_state.log_lines  = []
                    st.session_state.error_log  = []
                    _clear_cache()

                    total     = len(input_df)
                    # BUG 1 FIX: assign stable row ID so retry never drops
                    # valid duplicate rows when removing failed ones.
                    input_df = input_df.copy()
                    input_df["_row_id"] = range(len(input_df))
                    rows      = input_df.to_dict("records")
                    prog_bar  = st.progress(0, text="Initializing…")
                    metrics   = st.empty()
                    cur_url   = st.empty()
                    log_area  = st.empty()
                    results:  list[dict] = []
                    completed: list[int] = [0]
                    start_t   = time.time()

                    def _log(msg: str) -> None:
                        ts = datetime.now().strftime("%H:%M:%S")
                        entry = f"[{ts}] {msg}"
                        st.session_state.log_lines.append(entry)
                        if len(st.session_state.log_lines) > 500:
                            st.session_state.log_lines.pop(0)

                    def _log_error(url: str, status: str) -> None:
                        ts = datetime.now().strftime("%H:%M:%S")
                        st.session_state.error_log.append({
                            "Time": ts, "URL": url, "Status": status
                        })

                    def _save(dicts: list[dict]) -> None:
                        if dicts:
                            st.session_state.results_df = _build_df(dicts)
                            _autosave(dicts)

                    def _is_error(r: dict) -> bool:
                        s = str(r.get("Keyword_Search_Status", ""))
                        return s in _ERROR_STATUSES or (
                            "Error" in s or "HTTP" in s or "Exception" in s
                        )

                    def _run_pass(work_rows: list[dict], label: str
                                  ) -> tuple[list[dict], list[dict]]:
                        pass_res:  list[dict] = []
                        pass_err:  list[dict] = []

                        with ThreadPoolExecutor(max_workers=workers) as ex:
                            fmap: dict = {}
                            for r in work_rows:
                                # BUG 5 FIX: removed submission-time stagger (extra_delay).
                                # Staggering submissions serializes the thread pool and
                                # kills parallel efficiency. Back-off is already handled
                                # inside _download_one on actual failure.
                                # Pause support: block submission while paused
                                while st.session_state.paused and st.session_state.running:
                                    time.sleep(0.5)
                                if not st.session_state.running:
                                    break
                                f = ex.submit(
                                    process_one_url,
                                    str(r.get("URL", "")),
                                    str(r.get("Keyword", "")),
                                    _mode_key, match_all,
                                    case_sensitive, timeout,
                                    int(r.get("_row_id", 0)),
                                    enable_mirror,    # Bug 3 fix
                                    enable_smart,     # Bug 3 fix
                                )
                                fmap[f] = r

                            for future in as_completed(fmap):
                                if not st.session_state.running:
                                    _log("⏹ Stopped by user.")
                                    ex.shutdown(wait=False, cancel_futures=True)
                                    break

                                try:
                                    res = future.result()
                                except Exception as exc:
                                    src = fmap[future]
                                    res = {
                                        "_row_id": int(src.get("_row_id", 0)),
                                        "URL": str(src.get("URL", "")),
                                        "Keyword": str(src.get("Keyword", "")),
                                        "Search Mode": _mode_key.capitalize(),
                                        "Extraction_Option": "",
                                        "URL_Status": 0,
                                        "URL_Search_Status": f"Exception: {exc}",
                                        "Keyword_Status": None,
                                        "feature_name": str(src.get("Keyword", "")),
                                        "feature_value": None,
                                        "match_count": 0,
                                        "Keyword_Search_Status": f"Exception: {exc}",
                                        "matched_keywords": "",
                                        "missing_keywords": "",
                                    }

                                completed[0] += 1
                                status = res["Keyword_Search_Status"]
                                url_s  = res["URL"][-55:] if len(res["URL"]) > 55 else res["URL"]
                                icon   = ("✅" if status == S.FOUND
                                          else "⚠️" if status == S.PARTIAL
                                          else "❌" if status in (S.NOT_FOUND,)
                                          else "🔴")
                                _log(f"[{label}][{completed[0]}/{total}] "
                                     f"{icon} {status[:22]:22s} → …{url_s}")

                                if _is_error(res):
                                    pass_err.append(fmap[future])
                                    _log_error(res["URL"], status)

                                pass_res.append(res)

                                # Save every 100 rows
                                if completed[0] % 100 == 0:
                                    _save(results + pass_res)
                                    _log(f"💾 Auto-saved {completed[0]:,} rows to disk")

                                # Update UI
                                _n = max(1, min(25, total // 60))
                                if completed[0] % _n == 0 or completed[0] == total:
                                    pct     = min(completed[0] / total, 1.0)
                                    elapsed = time.time() - start_t
                                    rate    = completed[0] / elapsed if elapsed > 0 else 0
                                    eta     = (total - completed[0]) / rate if rate > 0 else 0
                                    succ    = sum(1 for r in pass_res if r.get("Keyword_Search_Status") == S.FOUND)
                                    succ_r  = succ / max(completed[0], 1) * 100

                                    prog_bar.progress(
                                        pct,
                                        text=f"[{label}] {completed[0]:,}/{total:,} "
                                             f"• {rate:.1f}/s • ETA {eta:.0f}s"
                                    )
                                    metrics.markdown(
                                        f"⏱ **{elapsed:.0f}s** elapsed &nbsp;|&nbsp; "
                                        f"⚡ **{rate:.1f}** URLs/s &nbsp;|&nbsp; "
                                        f"📊 **{completed[0]:,}/{total:,}** done &nbsp;|&nbsp; "
                                        f"✅ **{succ_r:.0f}%** success rate"
                                    )
                                    cur_url.markdown(
                                        f"**Processing:** `…{url_s}`"
                                    )
                                    log_area.markdown(
                                        '<div class="log-box">' +
                                        "<br>".join(st.session_state.log_lines[-40:]) +
                                        "</div>",
                                        unsafe_allow_html=True,
                                    )

                        return pass_res, pass_err

                    # ── PASS 1 ──────────────────────────────────────
                    _log(f"🚀 Pass 1 — {total:,} URLs, {workers} workers, mode={_mode_key}")
                    p1_res, p1_err = _run_pass(rows, "Pass1")
                    results.extend(p1_res)

                    # ── PASS 2 — retry errors ────────────────────────
                    if enable_retry and p1_err and st.session_state.running:
                        _log(f"♻️  Pass 2 — {len(p1_err):,} failed URLs…")
                        # BUG 1 FIX: use _row_id not (URL,Keyword) so duplicate
                        # rows with same URL+Keyword are handled independently.
                        err_ids = {r.get("_row_id") for r in p1_err}
                        results = [r for r in results if r.get("_row_id") not in err_ids]
                        total  += len(p1_err)
                        _log("⏳ 15 s cooldown before Pass 2…")
                        time.sleep(15)
                        p2_res, still_failed = _run_pass(p1_err, "Pass2")
                        results.extend(p2_res)
                        if still_failed:
                            _log(f"⚠️  {len(still_failed):,} URLs still failed after 2 passes.")
                        else:
                            _log("✅ All errors resolved in Pass 2!")

                    st.session_state.running = False
                    st.session_state.paused  = False

                    if results:
                        _save(results)
                        el = time.time() - start_t
                        prog_bar.progress(1.0, text="✅ Search Complete!")
                        st.success(
                            f"✅ **{completed[0]:,}** URLs in **{el:.1f}s** "
                            f"({completed[0]/el:.1f} URLs/s)"
                        )
                    else:
                        st.warning("No results collected.")

        except Exception as e:
            st.error(f"❌ Failed to load file: {e}")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 2 — RESULTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_results:
    df = st.session_state.results_df
    if df is None:
        st.info("🔍 Run a search on the Search tab to see results here.")
    else:
        total_r   = len(df)
        found_r   = (df["Keyword_Search_Status"] == S.FOUND).sum()
        nf_r      = (df["Keyword_Search_Status"] == S.NOT_FOUND).sum()
        partial_r = (df["Keyword_Search_Status"] == S.PARTIAL).sum()
        scanned_r = df["Keyword_Search_Status"].str.contains("Non searchable", na=False).sum()
        errors_r  = total_r - found_r - nf_r - partial_r - scanned_r

        st.markdown("### 📊 Summary")
        _render_stat_cards(total_r, found_r, nf_r, partial_r, scanned_r, errors_r)
        st.markdown("---")

        # ── Filters ───────────────────────────────────────────────
        fc1, fc2 = st.columns(2)
        with fc1:
            status_opts  = df["Keyword_Search_Status"].unique().tolist()
            status_filter = st.multiselect(
                "Filter by Status", options=status_opts, default=status_opts,
            )
        with fc2:
            kw_filter = st.text_input("Filter by Keyword (contains)", "")

        filtered = df[df["Keyword_Search_Status"].isin(status_filter)]
        if kw_filter:
            filtered = filtered[
                filtered["Keyword"].astype(str).str.contains(kw_filter, case=False, na=False)
            ]

        st.markdown(f"**Showing {len(filtered):,} rows**")

        # ── Results table ─────────────────────────────────────────
        display_df = filtered[_OUT_COLS].copy()
        if len(display_df) <= 5000:
            sfn = getattr(display_df.style, "map", None) or display_df.style.applymap
            st.dataframe(
                sfn(apply_status_badge, subset=["Keyword_Search_Status"]),
                use_container_width=True, height=460,
            )
        else:
            st.dataframe(display_df, use_container_width=True, height=460)

        # ── Download Section ──────────────────────────────────────
        # BUG 3 FIX: output_format sidebar choice now controls which
        # button is highlighted as primary and default file offered.
        st.markdown("---")
        st.markdown("### ⬇️ Download Results")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        _prefer_excel = (output_format == "Excel (.xlsx)")
        dc1, dc2 = st.columns(2)
        with dc1:
            try:
                st.download_button(
                    "📥 Download Excel (.xlsx)",
                    data=df_to_excel_bytes(filtered),
                    file_name=f"keyword_search_results_{ts}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    type="primary" if _prefer_excel else "secondary",
                )
            except Exception as exc:
                st.error(f"Excel export failed: {exc}. Use CSV instead.")
        with dc2:
            st.download_button(
                "📥 Download CSV (.csv)",
                data=df_to_csv_bytes(filtered),
                file_name=f"keyword_search_results_{ts}.csv",
                mime="text/csv",
                use_container_width=True,
                type="primary" if not _prefer_excel else "secondary",
            )

        # ── Chart ─────────────────────────────────────────────────
        st.markdown("---")
        st.markdown("### 📈 Result Distribution")
        chart = df["Keyword_Search_Status"].value_counts().reset_index()
        chart.columns = ["Status", "Count"]
        st.bar_chart(chart.set_index("Status"))

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 3 — LOGS (NEW)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_logs:
    st.markdown("### 📜 Full Execution Log")

    log_lines = st.session_state.log_lines
    error_log = st.session_state.error_log

    if not log_lines:
        st.info("No log entries yet. Logs appear here during and after a search.")
    else:
        # Export log
        log_text = "\n".join(log_lines)
        lc1, lc2 = st.columns([3, 1])
        with lc2:
            st.download_button(
                "📥 Export Log (.txt)",
                data=log_text.encode("utf-8"),
                file_name=f"search_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                mime="text/plain",
                use_container_width=True,
            )
        with lc1:
            st.markdown(f"**{len(log_lines):,} log entries**")

        st.markdown(
            '<div class="log-box" style="max-height:400px">' +
            "<br>".join(log_lines) +
            "</div>",
            unsafe_allow_html=True,
        )

    # ── Error Breakdown ───────────────────────────────────────────
    st.markdown("---")
    st.markdown("### ❌ Error Breakdown")

    if not error_log:
        st.success("✅ No errors recorded." if log_lines else "Run a search to see error breakdown.")
    else:
        err_df = pd.DataFrame(error_log)
        # Group by status
        breakdown = err_df["Status"].value_counts().reset_index()
        breakdown.columns = ["Error Type", "Count"]

        ec1, ec2 = st.columns([1, 2])
        with ec1:
            st.markdown(f"**{len(error_log):,} total errors**")
            for _, r in breakdown.iterrows():
                st.markdown(f"- **{r['Error Type']}**: {r['Count']:,}")

        with ec2:
            st.bar_chart(breakdown.set_index("Error Type"))

        with st.expander("📋 Error Detail Table", expanded=False):
            st.dataframe(err_df, use_container_width=True)

        # Export error log
        st.download_button(
            "📥 Export Error Log (.csv)",
            data=err_df.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"error_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
        )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 4 — GUIDE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_guide:
    st.markdown(f"""
## 📖 User Guide

### Step-by-Step

| Step | Action |
|------|--------|
| 1 | **Choose Search Mode** in sidebar (or leave Auto Detect) |
| 2 | **Download a Template** (Single / Multi / HTS) from sidebar |
| 3 | Fill: `URL` column + `Keyword` column |
| 4 | **Upload** the file on the Search tab |
| 5 | Review the **Pre-Processing Summary** |
| 6 | Click **🚀 Start Search** |
| 7 | Watch live progress — pause/stop any time |
| 8 | **Download results** from Results tab or sidebar (auto-saved) |

---

### 🔘 Search Modes Explained

| Mode | When to use | Keyword format |
|------|------------|---------------|
| **Auto Detect** | Default — app picks the right mode | Any |
| **Single Keyword** | One term per row | `51712160148` |
| **Multi Keyword** | Multiple terms, any match | `EAN123|UPC456|GTIN789` |
| **Table Search (HTS)** | Commodity / HTS codes | `8471.30|8471.41|8471.49` |

**Match ANY** (default): result is `Found` if at least one keyword matches.  
**Match ALL**: result is `Found` only when every keyword matches; otherwise `Partial Match`.

---

### ⚙️ Settings Reference

| Setting | Default | What it does |
|---------|---------|-------------|
| **Concurrent Workers** | {DEFAULT_WORKERS} | Parallel downloads — keep 4–6 for z2data.com |
| **Timeout per URL** | {DEFAULT_TIMEOUT}s | Max wait per URL — raise for slow/large PDFs |
| **Case-Sensitive** | OFF | OFF = `EAN123` matches `ean123` |
| **Output Format** | Excel | Use CSV for very large result sets |

---

### 🚦 All Status Values

| Status | Meaning | Action |
|--------|---------|--------|
| ✅ `{S.FOUND}` | Keyword found | Complete |
| ❌ `{S.NOT_FOUND}` | Readable, keyword absent | Complete |
| ⚠️ `{S.PARTIAL}` | Some keywords found (multi-mode, Match ALL) | Review missing keywords |
| 🟡 Scanned | Image PDF — no text layer | OCR needed externally |
| 🔻 `{S.URL_NOT_FOUND}` | HTTP 404 | Update URL |
| 🔻 `{S.HTML_NOT_FOUND}` | 200 OK but "not found" page | URL is stale/moved |
| 🟣 `{S.SSL_ERROR}` | TLS certificate failure | Auto-handled (verify=False) |
| 🟠 `{S.BLOCKED}` | Rate-limited (403/429) | Reduce workers, retry later |
| 🔵 `{S.TIMEOUT}` | No response in time | Increase timeout |
| ⚫ `{S.CORRUPTED}` | File unreadable or too small | Source file problem |
| 🔴 `{S.CONNECTION_ERR}` | TCP/DNS failure | Network issue |

---

### 📤 Output Columns

| Column | Layer | Description |
|--------|-------|-------------|
| `URL` | Input | Original URL |
| `Keyword` | Input | Raw keyword(s) as entered |
| `Search Mode` | System | Single / Multi / Table / Auto |
| `Extraction Option` | System | PDF or HTML |
| `URL_Status` | Network | `0` = failed · `3` = downloaded |
| `URL_Search_Status` | Network | `"Done"` or error category |
| `Keyword_Status` | Extraction | `3.0` = checked · `None` = not reached |
| `feature_name` | Search | Keyword searched |
| `feature_value` | Search | ~100-char context around first match |
| `match_count` | Search | Total occurrences found |
| `Keyword_Search_Status` | Search | **Main result** (see table above) |

---

### 🔄 Retry & Recovery

- **Pass 1:** every URL — 3 attempts with exponential back-off
- **Mirror fallback:** `source.z2data.com` ↔ `source1.z2data.com`
- **`/web/` path strip:** old archived URLs tried without `/web/` segment
- **15 s cooldown**, then **Pass 2** for failed URLs only (1 s stagger)
- **Auto-save every 100 rows** → survives Stop / refresh / internet drop
- **Sidebar Recovery panel** → restore or download saved data any time

---

### ⚡ Performance Tips

- **4–6 workers** for z2data.com (avoids blocking)
- **Timeout 20s** for typical PDFs; 40s+ for large files
- **CSV output** is faster than Excel for 10,000+ rows
- Duplicate URLs (same URL, different keywords) download only **once** (cached)
- Check the **Logs tab** for error breakdown after a run
    """)
