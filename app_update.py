"""
PDF Keyword Search System — v4.0
Single • Multi • Table Search Engine

Spec: Unified Online Platform Specification (v4)

Architecture
────────────
  Section 1   Constants + clean user-facing Status model
  Section 2   Network layer   (session, rate-limiter, block-detector)
  Section 3   URL cache + mirror fallback
  Section 4   Download + retry + precise error classification
  Section 5   Text extraction (PDF + HTML)
  Section 6   Text normalization + full-document multi-mode keyword search
  Section 7   Clean output builder (user-facing columns only)
  Section 8   Excel / CSV export helpers
  Section 9   Autosave / Recovery (disk-backed)
  Section 10  Session state
  Section 11  Streamlit UI (sidebar + 4 tabs)
"""

# ═══════════════════════════════════════════════════════════════════
# IMPORTS
# ═══════════════════════════════════════════════════════════════════
import streamlit as st
import pandas as pd
import io, os, re, time, random, threading, tempfile, hashlib, json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from html.parser import HTMLParser

import fitz          # PyMuPDF
import urllib3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ═══════════════════════════════════════════════════════════════════
# SECTION 1 — Page config + CSS
# ═══════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="PDF Keyword Search",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
.main { background-color: #0b0f1a; }
section[data-testid="stSidebar"] { background:#0b0f1a; border-right:1px solid #1c2333; }

/* ── Header ── */
.hdr {
    background: linear-gradient(135deg,#0a1628 0%,#112240 60%,#0a1a2e 100%);
    border:1px solid #1e3a5f; border-radius:14px;
    padding:22px 32px 18px; margin-bottom:20px;
    display:flex; align-items:center; justify-content:space-between;
}
.hdr h1  { font-size:1.85rem; font-weight:800; color:#00d4ff; margin:0; }
.hdr p   { font-size:0.85rem; color:#5a7fa3; margin:4px 0 0; }
.badge   { font-size:0.78rem; font-weight:700; padding:5px 13px;
           border-radius:20px; letter-spacing:0.3px; }
.b-ready   { background:#0a2e14; color:#00e676; border:1px solid #00e676; }
.b-running { background:#2e2200; color:#ffd600; border:1px solid #ffd600; }
.b-done    { background:#1a0d00; color:#ff9800; border:1px solid #ff9800; }

/* ── Stat cards ── */
.sc { background:#0f1623; border:1px solid #1c2d44; border-radius:10px;
      padding:15px 18px; text-align:center; }
.sc-n { font-size:1.85rem; font-weight:800; line-height:1; }
.sc-l { font-size:0.7rem; color:#5a7fa3; margin-top:4px;
        text-transform:uppercase; letter-spacing:0.5px; }

/* ── Limit banner ── */
.lim { background:linear-gradient(90deg,#8b2500,#c43d00);
       color:#fff; border-radius:8px; padding:8px 16px;
       font-weight:700; font-size:0.95rem; text-align:center; }

/* ── Log box ── */
.logbox {
    background:#080c14; border:1px solid #1c2333; border-radius:10px;
    padding:14px 18px; font-family:"Courier New",monospace; font-size:0.78rem;
    color:#7a8fa8; max-height:280px; overflow-y:auto; line-height:1.6;
}

/* ── Pre-process box ── */
.ppbox { background:#0f1623; border:1px solid #1c2d44; border-radius:10px;
         padding:15px 20px; margin:10px 0; }
.ppr   { display:flex; justify-content:space-between; padding:3px 0;
         font-size:0.86rem; }
.ppk   { color:#5a7fa3; }
.ppv   { color:#d0dae8; font-weight:600; }

/* ── Info note ── */
.info-note { background:#0a1a2e; border-left:3px solid #00d4ff;
             border-radius:0 8px 8px 0; padding:10px 16px;
             font-size:0.84rem; color:#7ab8d4; margin:10px 0; }

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] { gap:5px; border-bottom:1px solid #1c2333; }
.stTabs [data-baseweb="tab"] { background:#0f1623; border-radius:8px 8px 0 0;
    color:#5a7fa3; font-weight:600; padding:7px 18px;
    border:1px solid #1c2333; border-bottom:none; }
.stTabs [aria-selected="true"] { background:#1c2d44 !important;
    color:#00d4ff !important; border-color:#00d4ff !important; }

/* ── Buttons ── */
.stButton > button { border-radius:8px; font-weight:700; transition:all .15s; }
.stButton > button:hover { transform:translateY(-1px); }
hr.div { border:none; border-top:1px solid #1c2333; margin:14px 0; }
</style>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════
# SECTION 2 — Constants + Status model
# ═══════════════════════════════════════════════════════════════════
SEARCH_LIMIT     = 50_000
DEFAULT_WORKERS  = 6
DEFAULT_TIMEOUT  = 20
_CONNECT_TIMEOUT = 12      # TCP connect timeout (separate from read)
_MIN_DELAY_SECS  = 0.3     # min gap per host (all threads)
_BLOCK_THRESHOLD = 5       # consecutive failures → blocked
_BLOCK_COOLDOWN  = 30      # seconds to pause when blocked

_ILLEGAL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")

# ── Disk paths ────────────────────────────────────────────────────
_TMP            = tempfile.gettempdir()
_AUTOSAVE_CSV   = os.path.join(_TMP, "pdf_search_autosave.csv")
_JOB_STATE_FILE = os.path.join(_TMP, "pdf_search_job_state.json")


class S:
    """
    Single source of truth for all user-facing result values.
    Spec §10.5 / §11: clean public statuses only.
    Technical error detail lives in internal logs, not in the output file.
    """
    # ── User-facing (shown in output file) ────────────────────────
    FOUND          = "Found"
    NOT_FOUND      = "Not Found"
    PARTIAL        = "Partial Match"
    NON_SEARCHABLE = "Non searchable"   # spec §10.5 — replaces long scanned message

    # ── Internal only (logged, not in clean output) ───────────────
    URL_NOT_FOUND  = "URL Not Found"
    SSL_ERROR      = "SSL Error"
    TIMEOUT        = "Timeout"
    BLOCKED        = "Blocked / Rate Limited"
    CORRUPTED      = "File Not Available"
    INVALID_URL    = "Invalid URL"
    CONNECTION_ERR = "Connection Error"
    HTML_NOT_FOUND = "URL Not Found"    # merged with URL_NOT_FOUND for output


# Statuses that trigger the retry pass
_RETRY_STATUSES = {
    S.TIMEOUT, S.SSL_ERROR, S.BLOCKED,
    S.CONNECTION_ERR, S.CORRUPTED,
}

# Statuses that map to "Notes" in the clean output file
# (never shown in the Result column — spec §11 / §15)
_TECHNICAL_STATUSES = {
    S.URL_NOT_FOUND, S.SSL_ERROR, S.TIMEOUT, S.BLOCKED,
    S.CORRUPTED, S.INVALID_URL, S.CONNECTION_ERR, S.HTML_NOT_FOUND,
}

# ── Clean output columns (spec §11) ───────────────────────────────
# These are what the user sees in the downloaded file.
_CLEAN_COLS = [
    "URL", "Keyword", "Search Mode",
    "Result",          # user-facing: Found / Not Found / Partial Match / Non searchable
    "Match Count",     # total occurrences across document
    "Snippet",         # context around first match
    "Matched Keywords",
    "Missing Keywords",
    "Notes",           # non-empty only for errors — keeps main Result clean
]

# ── Internal columns (used during processing, stripped before export) ─
_INT_COLS = [
    "_row_id", "Extraction Option",
    "URL_Status", "URL_Search_Status", "Keyword_Status",
    "feature_name", "_raw_status",
]

# ── HTML "not found" phrases (Bug 7 fix — expanded) ──────────────
_HTML_NFP = [
    "page not found", "404 not found", "404 error",
    "file not found", "resource not found", "does not exist",
    "error 404", "no results found", "page unavailable",
    "page cannot be found", "the page you requested",
    "sorry, we couldn", "could not be found",
    "this page doesn", "we couldn", "not available",
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
    sess = requests.Session()
    sess.headers.update(_HEADERS)
    sess.verify = False   # handles SSL cert mismatch on source1.z2data.com
    no_retry = Retry(total=0, raise_on_status=False)
    adapter  = HTTPAdapter(max_retries=no_retry, pool_connections=20, pool_maxsize=20)
    sess.mount("https://", adapter)
    sess.mount("http://",  adapter)
    return sess


def _get_session(fresh: bool = False) -> requests.Session:
    if fresh or not hasattr(_thread_local, "session"):
        _thread_local.session = _make_session()
    return _thread_local.session


# ── Rate limiter (per-host, all threads) ─────────────────────────
_rate_lock = threading.Lock()
_last_req:  dict[str, float] = {}

def _rate_limit(host: str) -> None:
    with _rate_lock:
        gap = time.time() - _last_req.get(host, 0)
        if gap < _MIN_DELAY_SECS:
            time.sleep(_MIN_DELAY_SECS - gap)
        _last_req[host] = time.time()


# ── Block detector ────────────────────────────────────────────────
_block_lock    = threading.Lock()
_consec_fail:   dict[str, int]   = {}
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
# SECTION 4 — URL cache + alternate URLs
# ═══════════════════════════════════════════════════════════════════
_cache_lock = threading.Lock()
_url_cache: dict[str, tuple[str, str]] = {}   # url → (text, ext_status)


def _clear_all_state() -> None:
    """Reset every per-run cache and rate-limiter — must be called on new run."""
    with _cache_lock:   _url_cache.clear()
    with _block_lock:   _consec_fail.clear(); _blocked_until.clear()
    with _rate_lock:    _last_req.clear()


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
# SECTION 5 — Download + retry + error classification
# ═══════════════════════════════════════════════════════════════════
def _download_one(url: str, timeout: int, fresh: bool = False
                  ) -> tuple[bytes | None, str]:
    """Single HTTP GET. Returns (bytes | None, error_category)."""
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
        if code == 429:
            time.sleep(5 + random.random() * 5)
            return None, "blocked"
        if code == 403:
            return None, "blocked"
        if code in (404, 410):
            return None, "http_404"
        return None, f"http_{code}"

    except Exception as e:
        _record_failure(host)
        s = str(e).lower()
        if any(w in s for w in ("ssl", "certificate", "handshake", "tls")):
            return None, "ssl"
        if any(w in s for w in ("timed out", "timeout", "read timed")):
            return None, "timeout"
        return None, "connection"


def _fetch(url: str, session_timeout: int,
           use_mirror: bool = True) -> tuple[bytes | None, str]:
    """
    Full download with retry: primary + alternates, 3 attempts each.
    Returns (bytes | None, S.* status).
    Permanent failures (404) exit immediately without wasting retries.
    """
    candidates = [url] + (_get_alternate_urls(url) if use_mirror else [])
    last_status = S.TIMEOUT
    conn_err    = False

    for try_url in candidates:
        for attempt in range(1, 4):
            content, cat = _download_one(try_url, session_timeout, fresh=(conn_err and attempt == 1))
            conn_err = False

            if content is not None:
                # Validate content signature (bug-6 fix: not just size)
                if len(content) < 32:
                    last_status = S.CORRUPTED; break
                sig = content[:64].lstrip()
                is_pdf  = sig.startswith(b"%PDF")
                is_html = any(t in sig.lower() for t in (b"<html", b"<!doc", b"<head"))
                if len(content) < 64 and not is_pdf and not is_html:
                    last_status = S.CORRUPTED; break
                return content, "ok"

            if cat == "http_404":      return None, S.URL_NOT_FOUND
            if cat == "blocked":       last_status = S.BLOCKED;        break
            if cat == "ssl":           last_status = S.SSL_ERROR;       break
            if cat == "timeout":       last_status = S.TIMEOUT;         conn_err = True
            if cat == "connection":    last_status = S.CONNECTION_ERR;  conn_err = True

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


class _HtmlStripper(HTMLParser):
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
        html = None
        for enc in ("utf-8", "latin-1", "cp1252"):
            try: html = data.decode(enc); break
            except Exception: pass
        if not html:
            html = data.decode("utf-8", errors="replace")
        p = _HtmlStripper(); p.feed(html)
        text = "\n".join(p.parts)
        return (text, "searchable") if text.strip() else ("", "scanned")
    except Exception as e:
        return "", f"error:{e}"


def _get_text_cached(url: str, content: bytes, is_html: bool) -> tuple[str, str]:
    """
    Extract text with URL-level cache.
    Same document downloaded for keyword A is reused for keyword B.
    Spec §10.4: Duplicate URL Optimization.
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


def _is_not_found_page(text: str) -> bool:
    """Detect 200-OK responses that are actually 'not found' HTML pages."""
    if not text: return False
    sample = text[:1000].lower()
    return any(p in sample for p in _HTML_NFP)

# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — Text normalization + full-document keyword search
# ═══════════════════════════════════════════════════════════════════
def _normalize(text: str) -> str:
    """Clean extracted text before search — spec §10.1 full document search."""
    text = _ILLEGAL_RE.sub(" ", text)
    text = text.replace("\u00ad", "")    # soft hyphen
    text = text.replace("\u00a0", " ")   # non-breaking space
    text = re.sub(r"-\s*\n\s*", "", text)
    text = re.sub(r"\s+", " ", text)
    return text


def _parse_keywords(raw: str, mode: str) -> list[str]:
    raw = str(raw).strip()
    if mode == "single":
        return [raw]
    if mode in ("multi", "table"):
        parts = [k.strip() for k in raw.split("|") if k.strip()]
        return parts if parts else [raw]
    # auto
    if "|" in raw:
        return [k.strip() for k in raw.split("|") if k.strip()]
    return [raw]


def _search_keyword(text: str, keyword: str, case_sensitive: bool) -> int:
    """
    Count ALL occurrences of keyword in text.
    Spec §10.2: must count repeated occurrences, not just first.
    Returns 0 if not found.
    """
    if not text or not keyword: return 0
    flags = 0 if case_sensitive else re.IGNORECASE
    try:
        return len(re.findall(re.escape(keyword.strip()), text, flags))
    except re.error:
        return 0


def _search_all(text: str, keywords: list[str], case_sensitive: bool,
                match_all: bool) -> tuple[str, list[str], list[str], int]:
    """
    Multi-keyword full-document search.
    Returns (result_status, found_kws, missing_kws, total_count).
    """
    found:   list[str] = []
    missing: list[str] = []
    total = 0

    for kw in keywords:
        cnt = _search_keyword(text, kw, case_sensitive)
        if cnt > 0:
            found.append(kw)
            total += cnt
        else:
            missing.append(kw)

    if not keywords:
        return S.NOT_FOUND, [], [], 0
    if len(found) == len(keywords):
        return S.FOUND, found, [], total
    if found:
        return (S.PARTIAL if match_all else S.FOUND), found, missing, total
    return S.NOT_FOUND, [], missing, 0


def _best_snippet(text: str, keyword: str, ctx: int = 100) -> str:
    """
    Richest unique context window around any match.
    Spec §10.3: context snippets.
    """
    if not text or not keyword: return ""
    kw = str(keyword).lower()
    tl = text.lower()
    positions, start, best, seen = [], 0, "", set()
    while True:
        i = tl.find(kw, start)
        if i == -1: break
        positions.append(i); start = i + 1
    for i in positions:
        s = max(0, i - ctx); e = min(len(text), i + len(kw) + ctx)
        raw = _ILLEGAL_RE.sub("", text[s:e].strip())
        if not raw or raw in seen: continue
        seen.add(raw)
        cand = ("…" if s > 0 else "") + raw + ("…" if e < len(text) else "")
        if len(raw) > len(best.replace("…", "")):
            best = cand
    return best

# ═══════════════════════════════════════════════════════════════════
# SECTION 8 — Main per-row processor
# ═══════════════════════════════════════════════════════════════════
def process_one(url: str, raw_keyword: str, search_mode: str,
                match_all: bool, case_sensitive: bool,
                session_timeout: int, row_id: int = 0,
                use_mirror: bool = True,
                use_smart: bool = True) -> dict:
    """
    Download + extract + search.  Returns ONE result dict per call.
    row_id ensures retry never drops duplicate (URL, Keyword) rows.
    """
    out = {
        "_row_id":         row_id,
        "URL":             url,
        "Keyword":         raw_keyword,
        "Search Mode":     search_mode.capitalize(),
        "Extraction Option": "",
        "URL_Status":      None,
        "URL_Search_Status": "",
        "Keyword_Status":  None,
        "feature_name":    raw_keyword,
        "Result":          "",
        "Match Count":     0,
        "Snippet":         "",
        "Matched Keywords": "",
        "Missing Keywords": "",
        "Notes":           "",
        "_raw_status":     "",
    }

    def done(**kw) -> dict:
        r = dict(out); r.update(kw); return r

    url = str(url).strip()
    if not url or not url.startswith("http"):
        return done(URL_Status=0, URL_Search_Status=S.INVALID_URL,
                    Result=S.NOT_FOUND, Notes=S.INVALID_URL,
                    _raw_status=S.INVALID_URL)

    is_html = url.lower().split("?")[0].endswith((".html", ".htm"))
    out["Extraction Option"] = "HTML" if is_html else "PDF"

    # ── Download ──────────────────────────────────────────────────
    content, dl_status = _fetch(url, session_timeout, use_mirror=use_mirror)

    if content is None:
        # Spec §11 / §15: technical errors go to Notes, not Result
        user_result = S.NOT_FOUND
        return done(URL_Status=0, URL_Search_Status=dl_status,
                    Result=user_result, Notes=dl_status,
                    _raw_status=dl_status)

    # ── Extract ───────────────────────────────────────────────────
    text, ext_status = _get_text_cached(url, content, is_html)

    if "error:" in ext_status:
        return done(URL_Status=3, URL_Search_Status="Done",
                    Result=S.NOT_FOUND, Notes=S.CORRUPTED,
                    _raw_status=S.CORRUPTED)

    if ext_status == "scanned":
        return done(URL_Status=3, URL_Search_Status="Done",
                    Keyword_Status=None,
                    Result=S.NON_SEARCHABLE,        # spec §10.5
                    _raw_status=S.NON_SEARCHABLE)

    norm = _normalize(text)

    if use_smart and _is_not_found_page(norm):
        return done(URL_Status=3, URL_Search_Status="Done",
                    Result=S.NOT_FOUND, Notes=S.HTML_NOT_FOUND,
                    _raw_status=S.HTML_NOT_FOUND)

    # ── Full-document keyword search ──────────────────────────────
    keywords = _parse_keywords(raw_keyword, search_mode)
    result, found_kws, missing_kws, total_count = _search_all(
        norm, keywords, case_sensitive, match_all
    )

    # Best snippet from first matched keyword
    snippet = _best_snippet(norm, found_kws[0]) if found_kws else ""

    return done(
        URL_Status=3, URL_Search_Status="Done", Keyword_Status=3.0,
        Result=result,
        Match_Count=total_count,
        Snippet=snippet,
        Matched_Keywords=", ".join(found_kws),
        Missing_Keywords=", ".join(missing_kws),
        Notes="" if result in (S.FOUND, S.NOT_FOUND, S.PARTIAL) else result,
        _raw_status=result,
        # Normalize column names to match _CLEAN_COLS
        **{"Match Count": total_count,
           "Matched Keywords": ", ".join(found_kws),
           "Missing Keywords": ", ".join(missing_kws)},
    )

# ═══════════════════════════════════════════════════════════════════
# SECTION 9 — Clean output builder + Excel/CSV helpers
# ═══════════════════════════════════════════════════════════════════
def _clean_cell(v):
    if isinstance(v, str):
        return _ILLEGAL_RE.sub("", v)
    return v


def _build_clean_df(result_dicts: list[dict]) -> pd.DataFrame:
    """
    Build the user-facing output DataFrame.
    Spec §11: only clean columns visible; technical fields stripped.
    """
    df = pd.DataFrame(result_dicts)
    # Ensure all required columns exist
    for c in _CLEAN_COLS:
        if c not in df.columns:
            df[c] = ""
    # Normalize match count
    if "match_count" in df.columns and "Match Count" not in df.columns:
        df["Match Count"] = df["match_count"]
    df["Match Count"] = pd.to_numeric(df.get("Match Count", 0), errors="coerce").fillna(0).astype(int)
    return df[_CLEAN_COLS].copy()


def _build_internal_df(result_dicts: list[dict]) -> pd.DataFrame:
    """Internal DataFrame for autosave — includes all fields for resumability."""
    return pd.DataFrame(result_dicts)


def _to_excel(df: pd.DataFrame) -> bytes:
    """Export to xlsx with multiple informative sheets. Spec §12: descriptive sheet names."""
    fn = getattr(df, "map", None) or df.applymap
    clean = fn(_clean_cell)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        # Sheet 1: All Results
        clean.to_excel(w, index=False, sheet_name="All Results")
        # Sheet 2: Found only
        found = clean[clean["Result"] == S.FOUND]
        if not found.empty:
            found.to_excel(w, index=False, sheet_name="Found")
        # Sheet 3: Not Found
        nf = clean[clean["Result"].isin([S.NOT_FOUND, S.PARTIAL, S.NON_SEARCHABLE])]
        if not nf.empty:
            nf.to_excel(w, index=False, sheet_name="Not Found and Partial")
        # Sheet 4: Errors (notes column non-empty)
        errs = clean[clean["Notes"].astype(str).str.strip() != ""]
        if not errs.empty:
            errs.to_excel(w, index=False, sheet_name="Errors and Issues")
    return buf.getvalue()


def _to_csv(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def _result_badge(val) -> str:
    v = str(val)
    if v == S.FOUND:          return "background-color:#0a2e14;color:#00e676"
    if v == S.NOT_FOUND:      return "background-color:#2e0a0a;color:#ff5252"
    if v == S.PARTIAL:        return "background-color:#162600;color:#b2ff59"
    if v == S.NON_SEARCHABLE: return "background-color:#2e2200;color:#ffd600"
    return "background-color:#1c1c1c;color:#888"

# ═══════════════════════════════════════════════════════════════════
# SECTION 10 — Autosave / Job state persistence
# ═══════════════════════════════════════════════════════════════════
def _autosave(result_dicts: list[dict], processed: int, total: int) -> None:
    """
    Save partial results to disk every N rows.
    Spec §14: autosave + job state so results survive browser refresh.
    """
    try:
        if result_dicts:
            _build_internal_df(result_dicts).to_csv(_AUTOSAVE_CSV, index=False)
        state = {"processed": processed, "total": total,
                 "saved_at": datetime.now().isoformat()}
        with open(_JOB_STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception:
        pass


def _load_autosave() -> tuple[pd.DataFrame | None, dict | None]:
    df, state = None, None
    try:
        if os.path.exists(_AUTOSAVE_CSV) and os.path.getsize(_AUTOSAVE_CSV) > 0:
            df = pd.read_csv(_AUTOSAVE_CSV, dtype={"Keyword": str, "URL": str})
    except Exception:
        pass
    try:
        if os.path.exists(_JOB_STATE_FILE):
            with open(_JOB_STATE_FILE) as f:
                state = json.load(f)
    except Exception:
        pass
    return df, state


def _clear_autosave() -> None:
    for p in (_AUTOSAVE_CSV, _JOB_STATE_FILE):
        try: os.remove(p)
        except Exception: pass

# ═══════════════════════════════════════════════════════════════════
# SECTION 11 — Template builder
# ═══════════════════════════════════════════════════════════════════
def _make_template() -> bytes:
    """
    Spec §12: descriptive sheet names, clear examples, all 3 modes.
    """
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        pd.DataFrame({
            "URL": ["https://source.z2data.com/example.pdf",
                    "https://source.z2data.com/example2.pdf"],
            "Keyword": ["51712160148", "4015080000000"],
        }).to_excel(w, index=False, sheet_name="Single Search")

        pd.DataFrame({
            "URL": ["https://source.z2data.com/example.pdf"],
            "Keyword": ["51712160148|4015080000000|EAN5413131"],
        }).to_excel(w, index=False, sheet_name="Multi Search")

        pd.DataFrame({
            "URL": ["https://source.z2data.com/example.pdf"],
            "Keyword": ["8471.30|8471.41|8471.49"],
        }).to_excel(w, index=False, sheet_name="Table Search")
    return buf.getvalue()

# ═══════════════════════════════════════════════════════════════════
# SECTION 12 — Session state
# ═══════════════════════════════════════════════════════════════════
for _k, _v in [
    ("results_df",   None),
    ("running",      False),
    ("paused",       False),
    ("log_lines",    []),
    ("error_log",    []),
    ("job_stats",    {}),
]:
    if _k not in st.session_state:
        st.session_state[_k] = _v

# ═══════════════════════════════════════════════════════════════════
# SECTION 13 — UI Helpers
# ═══════════════════════════════════════════════════════════════════
def _header() -> None:
    if st.session_state.running:
        badge = '<span class="badge b-running">🟡 Running</span>'
    elif st.session_state.results_df is not None:
        badge = '<span class="badge b-done">🟠 Complete</span>'
    else:
        badge = '<span class="badge b-ready">🟢 Ready</span>'
    st.markdown(f"""
    <div class="hdr">
        <div>
            <h1>🔍 PDF Keyword Search</h1>
            <p>Single &nbsp;•&nbsp; Multi &nbsp;•&nbsp; Table &nbsp; Search Engine</p>
        </div>
        {badge}
    </div>""", unsafe_allow_html=True)


def _stat(col, n: int, color: str, label: str) -> None:
    with col:
        st.markdown(f"""
        <div class="sc">
            <div class="sc-n" style="color:{color}">{n:,}</div>
            <div class="sc-l">{label}</div>
        </div>""", unsafe_allow_html=True)


def _stats_row(total, found, nf, partial, scanned, notes) -> None:
    cols = st.columns(6)
    _stat(cols[0], total,   "#00d4ff", "Total")
    _stat(cols[1], found,   "#00e676", "Found")
    _stat(cols[2], nf,      "#ff5252", "Not Found")
    _stat(cols[3], partial, "#b2ff59", "Partial")
    _stat(cols[4], scanned, "#ffd600", "Non Searchable")
    _stat(cols[5], notes,   "#f48fb1", "Issues")

# ═══════════════════════════════════════════════════════════════════
# SECTION 14 — SIDEBAR
# ═══════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("## ⚙️ Settings")
    st.markdown(f'<div class="lim">⚠️ Limit: {SEARCH_LIMIT:,}</div>',
                unsafe_allow_html=True)

    # ── Search Configuration ───────────────────────────────────────
    st.markdown("---")
    st.markdown("### ⚡ Search Configuration")

    mode_label = st.radio(
        "Search Mode",
        ["Auto Detect (Recommended)", "Single Search",
         "Multi Search (|)", "Table Search"],
        index=0,
        help=(
            "**Auto Detect:** app picks the right mode based on your keyword.\n\n"
            "**Single Search:** one keyword per row.\n\n"
            "**Multi Search:** separate keywords with `|` — e.g. `EAN123|UPC456`.\n\n"
            "**Table Search:** numeric / code searches, also split on `|`."
        ),
    )
    _MODE = {
        "Auto Detect (Recommended)": "auto",
        "Single Search":             "single",
        "Multi Search (|)":          "multi",
        "Table Search":              "table",
    }[mode_label]

    match_all = False
    if _MODE in ("multi", "table", "auto"):
        match_all = st.radio(
            "Match Logic",
            ["Match ANY keyword", "Match ALL keywords"],
            index=0,
            help=(
                "**Match ANY:** Found if at least one keyword is present.\n\n"
                "**Match ALL:** Found only when every keyword matches. "
                "Partial Match when some match."
            ),
        ) == "Match ALL keywords"

    case_sensitive = st.checkbox("Case-Sensitive Search", value=False,
        help="OFF: `EAN123` matches `ean123`. ON: exact case only.")

    # ── Performance ────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### 🚀 Performance")

    workers = st.slider("Concurrent Workers", 2, 12, DEFAULT_WORKERS, 1,
        help=(
            "• **2–4** — safe for z2data.com\n"
            "• **6** — balanced default\n"
            "• **10–12** — fast CDNs only"
        ))
    timeout = st.slider("Timeout per URL (sec)", 5, 60, DEFAULT_TIMEOUT, 5,
        help="Increase for large PDFs or slow servers.")

    # ── Advanced ───────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### 🛡 Advanced")
    enable_retry  = st.checkbox("Enable Retry System",          value=True)
    enable_mirror = st.checkbox("Enable Mirror Fallback",       value=True)
    enable_smart  = st.checkbox("Enable Smart Error Detection", value=True)
    output_format = st.radio("Output Format", ["Excel (.xlsx)", "CSV (.csv)"], index=0)

    # ── Templates ──────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### 📥 Download Template")
    st.download_button(
        "⬇️ Download All Templates",
        data=_make_template(),
        file_name="PDF_Search_Templates.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

    # ── Status Guide ───────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### ℹ️ Result Guide")
    st.markdown(f"""
- 🟢 **{S.FOUND}** — keyword located in document
- 🔴 **{S.NOT_FOUND}** — document searched, keyword absent
- 🟩 **{S.PARTIAL}** — some keywords found (Multi / Match ALL)
- 🟡 **{S.NON_SEARCHABLE}** — image PDF, no text layer
- 📝 **Issues** — see Notes column in output file
""")

    # ── Auto-Save / Recovery ───────────────────────────────────────
    st.markdown("---")
    st.markdown("### 💾 Auto-Save / Recovery")
    _saved_df, _job_state = _load_autosave()

    if _saved_df is not None:
        _n = len(_saved_df)
        _prog = _job_state.get("processed", _n) if _job_state else _n
        _tot  = _job_state.get("total", _n)     if _job_state else _n
        _ts   = _job_state.get("saved_at", "")  if _job_state else ""
        st.success(f"📂 **{_n:,} rows** saved ({_prog:,}/{_tot:,} done)")
        if _ts:
            st.caption(f"Last saved: {_ts[:19]}")

        _now = datetime.now().strftime("%Y%m%d_%H%M%S")
        _clean = _build_clean_df(_saved_df.to_dict("records")) if "Result" not in _saved_df.columns else _saved_df[_CLEAN_COLS] if all(c in _saved_df.columns for c in _CLEAN_COLS) else _saved_df
        st.download_button(
            f"📥 CSV ({_n:,} rows)",
            data=_clean.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"autosave_{_now}.csv", mime="text/csv",
            use_container_width=True, key="sb_csv",
        )
        try:
            st.download_button(
                f"📥 Excel ({_n:,} rows)",
                data=_to_excel(_clean),
                file_name=f"autosave_{_now}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True, key="sb_xlsx",
            )
        except Exception:
            pass
        r1, r2 = st.columns(2)
        with r1:
            if st.button("♻️ Restore", use_container_width=True, key="restore"):
                st.session_state.results_df = _clean
                st.success("✅ Restored!")
        with r2:
            if st.button("🗑 Clear", use_container_width=True, key="clr"):
                _clear_autosave(); st.rerun()
    else:
        st.caption("Results auto-save every 100 rows during a search.")

# ═══════════════════════════════════════════════════════════════════
# SECTION 15 — MAIN CONTENT
# ═══════════════════════════════════════════════════════════════════
_header()

tab_search, tab_results, tab_logs, tab_guide = st.tabs(
    ["🔍 Search", "📊 Results", "📜 Logs", "📖 Guide"]
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 1 — SEARCH
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_search:
    c_up, c_fmt = st.columns([2, 1])

    with c_up:
        st.markdown("### 📁 Upload Input File")
        st.markdown("Excel (.xlsx) or CSV with **`URL`** and **`Keyword`** columns.")
        uploaded = st.file_uploader("Drop file here", type=["xlsx", "xls", "csv"],
                                    label_visibility="collapsed")
    with c_fmt:
        st.markdown("### 📌 Required Columns")
        st.markdown("""
| Column | Example |
|--------|---------|
| `URL` | `https://…/file.pdf` |
| `Keyword` | `51712160148` |

Multi: `EAN123|UPC456`
""")

    if uploaded:
        try:
            if uploaded.name.endswith(".csv"):
                idf = pd.read_csv(uploaded, dtype={"Keyword": str})
            else:
                idf = pd.read_excel(uploaded, dtype={"Keyword": str})

            idf.columns = [c.strip() for c in idf.columns]
            if "URL" not in idf.columns and "Offline" in idf.columns:
                idf.rename(columns={"Offline": "URL"}, inplace=True)

            bad_cols = [c for c in ["URL", "Keyword"] if c not in idf.columns]
            if bad_cols:
                st.error(f"❌ Missing columns: **{', '.join(bad_cols)}**. "
                         f"Found: {idf.columns.tolist()}")
            else:
                idf = idf.dropna(subset=["URL"]).reset_index(drop=True)
                if len(idf) > SEARCH_LIMIT:
                    st.warning(f"⚠️ Only first {SEARCH_LIMIT:,} rows will be processed.")
                    idf = idf.head(SEARCH_LIMIT)

                total_rows  = len(idf)
                unique_urls = idf["URL"].nunique()
                dup_urls    = total_rows - unique_urls

                st.success(f"✅ **{total_rows:,}** rows loaded")

                with st.expander("🔎 Preview (first 10 rows)", expanded=False):
                    st.dataframe(idf.head(10), use_container_width=True)

                st.markdown("<hr class='div'>", unsafe_allow_html=True)

                # ── Pre-Processing Summary ─────────────────────────
                st.markdown("### 📋 Pre-Processing Summary")

                _auto_desc = _MODE
                if _MODE == "auto":
                    _s0 = str(idf["Keyword"].iloc[0]) if total_rows else ""
                    if "|" in _s0:
                        _auto_desc = "Multi Search (auto-detected)"
                    elif re.fullmatch(r"[\d.]+", _s0.split("|")[0].strip()):
                        _auto_desc = "Table Search (auto-detected)"
                    else:
                        _auto_desc = "Single Search (auto-detected)"

                _cache_note = (f"✅ {dup_urls:,} duplicate URLs will reuse cached text"
                               if dup_urls > 0 else "No duplicate URLs")

                st.markdown(f"""
<div class="ppbox">
  <div class="ppr"><span class="ppk">Total Rows</span><span class="ppv">{total_rows:,}</span></div>
  <div class="ppr"><span class="ppk">Unique URLs</span><span class="ppv">{unique_urls:,}</span></div>
  <div class="ppr"><span class="ppk">URL Cache Savings</span><span class="ppv">{_cache_note}</span></div>
  <div class="ppr"><span class="ppk">Search Mode</span><span class="ppv">{_auto_desc}</span></div>
  <div class="ppr"><span class="ppk">Match Logic</span><span class="ppv">{"Match ALL" if match_all else "Match ANY"}</span></div>
  <div class="ppr"><span class="ppk">Case-Sensitive</span><span class="ppv">{"Yes" if case_sensitive else "No"}</span></div>
  <div class="ppr"><span class="ppk">Workers / Timeout</span><span class="ppv">{workers} workers / {timeout}s</span></div>
</div>
""", unsafe_allow_html=True)

                st.markdown("""
<div class="info-note">
ℹ️ Results are auto-saved every 100 rows to disk.
If the page refreshes or your connection drops, use the
<strong>Auto-Save / Recovery</strong> panel in the sidebar to download
or restore partial results at any time.
</div>
""", unsafe_allow_html=True)

                st.markdown("<hr class='div'>", unsafe_allow_html=True)

                # ── Action Buttons ─────────────────────────────────
                b1, b2, b3, _ = st.columns([2, 1, 1, 1])
                with b1:
                    start = st.button("🚀 Start Search", use_container_width=True,
                                      type="primary",
                                      disabled=st.session_state.running)
                with b2:
                    if st.button("⏸ Pause / Resume", use_container_width=True,
                                 disabled=not st.session_state.running):
                        st.session_state.paused = not st.session_state.paused
                with b3:
                    if st.button("⏹ Stop", use_container_width=True,
                                 disabled=not st.session_state.running):
                        st.session_state.running = False
                        st.session_state.paused  = False

                # ── Run ────────────────────────────────────────────
                if start and not st.session_state.running:
                    st.session_state.running    = True
                    st.session_state.paused     = False
                    st.session_state.results_df = None
                    st.session_state.log_lines  = []
                    st.session_state.error_log  = []
                    st.session_state.job_stats  = {}
                    _clear_all_state()

                    # Assign stable row IDs (Bug 1 fix)
                    idf = idf.copy()
                    idf["_row_id"] = range(len(idf))
                    rows = idf.to_dict("records")
                    total = len(rows)

                    prog  = st.progress(0, text="Initializing…")
                    mtrs  = st.empty()
                    curl  = st.empty()
                    lbox  = st.empty()
                    results:   list[dict] = []
                    done_n:    list[int]  = [0]
                    start_t    = time.time()

                    def _log(msg: str) -> None:
                        ts = datetime.now().strftime("%H:%M:%S")
                        st.session_state.log_lines.append(f"[{ts}] {msg}")
                        if len(st.session_state.log_lines) > 500:
                            st.session_state.log_lines.pop(0)

                    def _log_err(url_: str, status_: str) -> None:
                        st.session_state.error_log.append({
                            "Time": datetime.now().strftime("%H:%M:%S"),
                            "URL": url_, "Issue": status_,
                        })

                    def _save(dicts: list[dict]) -> None:
                        if dicts:
                            clean = _build_clean_df(dicts)
                            st.session_state.results_df = clean
                            _autosave(dicts, done_n[0], total)

                    def _is_retry(r: dict) -> bool:
                        return r.get("_raw_status", "") in _RETRY_STATUSES

                    def _run_pass(work: list[dict], label: str) -> tuple[list, list]:
                        pass_res: list[dict] = []
                        pass_err: list[dict] = []

                        with ThreadPoolExecutor(max_workers=workers) as ex:
                            fmap: dict = {}
                            for r in work:
                                while st.session_state.paused and st.session_state.running:
                                    time.sleep(0.5)
                                if not st.session_state.running:
                                    break
                                f = ex.submit(
                                    process_one,
                                    str(r.get("URL", "")),
                                    str(r.get("Keyword", "")),
                                    _MODE, match_all, case_sensitive, timeout,
                                    int(r.get("_row_id", 0)),
                                    enable_mirror, enable_smart,
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
                                        "Search Mode": _MODE.capitalize(),
                                        "Extraction Option": "",
                                        "Result": S.NOT_FOUND,
                                        "Match Count": 0, "Snippet": "",
                                        "Matched Keywords": "",
                                        "Missing Keywords": "",
                                        "Notes": f"Exception: {exc}",
                                        "_raw_status": S.CONNECTION_ERR,
                                    }

                                done_n[0] += 1
                                raw_s = res.get("_raw_status", "")
                                icon  = ("✅" if raw_s == S.FOUND else
                                         "⚠️" if raw_s == S.PARTIAL else
                                         "❌" if raw_s == S.NOT_FOUND else "🔴")
                                url_s = res["URL"][-55:] if len(res["URL"]) > 55 else res["URL"]
                                _log(f"[{label}][{done_n[0]}/{total}] {icon} {raw_s[:20]:20s} …{url_s}")

                                if raw_s in _TECHNICAL_STATUSES:
                                    pass_err.append(fmap[future])
                                    _log_err(res["URL"], raw_s)

                                pass_res.append(res)

                                if done_n[0] % 100 == 0:
                                    _save(results + pass_res)
                                    _log(f"💾 Auto-saved {done_n[0]:,} rows")

                                _n = max(1, min(25, total // 60))
                                if done_n[0] % _n == 0 or done_n[0] == total:
                                    pct  = min(done_n[0] / total, 1.0)
                                    el   = time.time() - start_t
                                    rate = done_n[0] / el if el else 0
                                    eta  = (total - done_n[0]) / rate if rate else 0
                                    succ = sum(1 for r in pass_res if r.get("_raw_status") == S.FOUND)
                                    prog.progress(pct,
                                        text=f"[{label}] {done_n[0]:,}/{total:,} • {rate:.1f}/s • ETA {eta:.0f}s")
                                    mtrs.markdown(
                                        f"⏱ **{el:.0f}s** &nbsp;|&nbsp; "
                                        f"⚡ **{rate:.1f}** URLs/s &nbsp;|&nbsp; "
                                        f"✅ **{succ}** found &nbsp;|&nbsp; "
                                        f"📊 **{done_n[0]:,}/{total:,}** done"
                                    )
                                    curl.markdown(f"**Processing:** `…{url_s}`")
                                    lbox.markdown(
                                        '<div class="logbox">' +
                                        "<br>".join(st.session_state.log_lines[-40:]) +
                                        "</div>", unsafe_allow_html=True)

                        return pass_res, pass_err

                    # ── Pass 1 ─────────────────────────────────────
                    _log(f"🚀 Pass 1 — {total:,} rows, {workers} workers, mode={_MODE}")
                    p1, p1_err = _run_pass(rows, "Pass 1")
                    results.extend(p1)

                    # ── Pass 2 — retry failed rows ──────────────────
                    if enable_retry and p1_err and st.session_state.running:
                        _log(f"♻️ Pass 2 — retrying {len(p1_err):,} failed rows…")
                        # Use row_id to safely remove only the specific failed rows
                        err_ids = {r.get("_row_id") for r in p1_err}
                        results = [r for r in results if r.get("_row_id") not in err_ids]
                        total  += len(p1_err)
                        _log("⏳ 15 s cooldown…")
                        time.sleep(15)
                        p2, still = _run_pass(p1_err, "Pass 2")
                        results.extend(p2)
                        _log(f"{'✅ All resolved' if not still else f'⚠️ {len(still):,} still failed'} after Pass 2")

                    st.session_state.running = False
                    st.session_state.paused  = False

                    if results:
                        _save(results)
                        el = time.time() - start_t
                        # Store stats for Logs tab
                        df_r = _build_clean_df(results)
                        st.session_state.job_stats = {
                            "total": len(df_r),
                            "found": (df_r["Result"] == S.FOUND).sum(),
                            "not_found": (df_r["Result"] == S.NOT_FOUND).sum(),
                            "partial": (df_r["Result"] == S.PARTIAL).sum(),
                            "scanned": (df_r["Result"] == S.NON_SEARCHABLE).sum(),
                            "elapsed": el,
                        }
                        prog.progress(1.0, text="✅ Search Complete!")
                        st.success(
                            f"✅ **{done_n[0]:,}** rows in **{el:.1f}s** "
                            f"({done_n[0]/el:.1f} rows/s)"
                        )
                    else:
                        st.warning("No results collected.")

        except Exception as e:
            st.error(f"❌ Failed to load file: {e}")
            import traceback
            st.session_state.log_lines.append(f"[ERROR] {traceback.format_exc()}")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 2 — RESULTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_results:
    df = st.session_state.results_df
    if df is None:
        st.info("🔍 Run a search to see results here.")
    else:
        # Ensure clean columns present
        for _c in _CLEAN_COLS:
            if _c not in df.columns: df[_c] = ""

        t_r  = len(df)
        f_r  = (df["Result"] == S.FOUND).sum()
        nf_r = (df["Result"] == S.NOT_FOUND).sum()
        p_r  = (df["Result"] == S.PARTIAL).sum()
        sc_r = (df["Result"] == S.NON_SEARCHABLE).sum()
        nt_r = df["Notes"].astype(str).str.strip().ne("").sum()

        st.markdown("### 📊 Summary")
        _stats_row(t_r, f_r, nf_r, p_r, sc_r, nt_r)
        st.markdown("---")

        # ── Filters ───────────────────────────────────────────────
        fc1, fc2 = st.columns(2)
        with fc1:
            s_opts = sorted(df["Result"].unique().tolist())
            s_filt = st.multiselect("Filter by Result", s_opts, default=s_opts)
        with fc2:
            kw_filt = st.text_input("Filter by Keyword (contains)", "")

        fdf = df[df["Result"].isin(s_filt)]
        if kw_filt:
            fdf = fdf[fdf["Keyword"].astype(str).str.contains(kw_filt, case=False, na=False)]

        st.markdown(f"**Showing {len(fdf):,} of {t_r:,} rows**")

        disp = fdf[_CLEAN_COLS].copy()
        if len(disp) <= 5000:
            sfn = getattr(disp.style, "map", None) or disp.style.applymap
            st.dataframe(sfn(_result_badge, subset=["Result"]),
                         use_container_width=True, height=460)
        else:
            st.dataframe(disp, use_container_width=True, height=460)

        # ── Download ──────────────────────────────────────────────
        st.markdown("---")
        st.markdown("### ⬇️ Download Results")
        _ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
        _pex = (output_format == "Excel (.xlsx)")
        dc1, dc2 = st.columns(2)
        with dc1:
            try:
                st.download_button(
                    "📥 Excel (.xlsx) — 4 sheets",
                    data=_to_excel(fdf[_CLEAN_COLS]),
                    file_name=f"keyword_search_{_ts}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    type="primary" if _pex else "secondary",
                )
            except Exception as ex:
                st.error(f"Excel failed: {ex}. Use CSV.")
        with dc2:
            st.download_button(
                "📥 CSV",
                data=_to_csv(fdf[_CLEAN_COLS]),
                file_name=f"keyword_search_{_ts}.csv",
                mime="text/csv",
                use_container_width=True,
                type="primary" if not _pex else "secondary",
            )

        # ── Distribution chart ─────────────────────────────────────
        st.markdown("---")
        st.markdown("### 📈 Result Distribution")
        cht = df["Result"].value_counts().reset_index()
        cht.columns = ["Result", "Count"]
        st.bar_chart(cht.set_index("Result"))

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 3 — LOGS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_logs:
    st.markdown("### 📜 Execution Log")
    ll = st.session_state.log_lines
    el = st.session_state.error_log

    if not ll:
        st.info("No log entries yet.")
    else:
        lc1, lc2 = st.columns([3, 1])
        with lc1:
            st.markdown(f"**{len(ll):,} entries**")
        with lc2:
            st.download_button("📥 Export Log",
                data="\n".join(ll).encode("utf-8"),
                file_name=f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                mime="text/plain", use_container_width=True)
        st.markdown(
            '<div class="logbox" style="max-height:420px">' +
            "<br>".join(ll) + "</div>",
            unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("### ❌ Issue Breakdown")

    if not el:
        st.success("✅ No issues." if ll else "Run a search first.")
    else:
        edf = pd.DataFrame(el)
        bd  = edf["Issue"].value_counts().reset_index()
        bd.columns = ["Issue Type", "Count"]
        ec1, ec2 = st.columns([1, 2])
        with ec1:
            st.markdown(f"**{len(el):,} issues total**")
            for _, r in bd.iterrows():
                st.markdown(f"- **{r['Issue Type']}**: {r['Count']:,}")
        with ec2:
            st.bar_chart(bd.set_index("Issue Type"))
        with st.expander("📋 Issue Detail", expanded=False):
            st.dataframe(edf, use_container_width=True)
        st.download_button("📥 Export Issues (.csv)",
            data=edf.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"issues_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 4 — GUIDE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_guide:
    st.markdown(f"""
## 📖 User Guide

### How to Use

| Step | Action |
|------|--------|
| 1 | Choose **Search Mode** in the sidebar |
| 2 | Download a **Template** from the sidebar |
| 3 | Fill in `URL` and `Keyword` columns |
| 4 | Upload the file on the **Search** tab |
| 5 | Review the **Pre-Processing Summary** |
| 6 | Click **🚀 Start Search** |
| 7 | Watch live progress — Pause or Stop at any time |
| 8 | Download results from the **Results** tab |

> **If the page refreshes or your connection drops**, your progress is saved automatically.
> Use the **Auto-Save / Recovery** panel in the sidebar to restore or download partial results.

---

### 🔘 Search Modes

| Mode | Keyword Format | When to Use |
|------|---------------|-------------|
| **Single Search** | `51712160148` | One keyword per row |
| **Multi Search** | `EAN123|UPC456|GTIN789` | Multiple keywords per row |
| **Table Search** | `8471.30|8471.41` | Numeric / code lookups |
| **Auto Detect** | Any | App picks the right mode |

**Match ANY** (default): Found if at least one keyword is present.
**Match ALL**: Found only if every keyword matches; otherwise Partial Match.

---

### 🚦 Result Values

| Result | Meaning |
|--------|---------|
| ✅ **{S.FOUND}** | Keyword found in the document |
| ❌ **{S.NOT_FOUND}** | Document searched, keyword absent |
| ⚠️ **{S.PARTIAL}** | Some keywords found (Multi + Match ALL) |
| 🟡 **{S.NON_SEARCHABLE}** | Image PDF — no text layer to search |

> Technical issues (connection errors, timeouts, SSL problems) are recorded in the **Notes** column
> and are never shown in the main **Result** column. This keeps the output clean for non-technical users.

---

### 📤 Output File Columns

| Column | Description |
|--------|-------------|
| `URL` | Original URL |
| `Keyword` | Keyword(s) as entered |
| `Search Mode` | Single / Multi / Table / Auto |
| `Result` | **Main result** — Found / Not Found / Partial Match / Non searchable |
| `Match Count` | Total number of times the keyword appears in the document |
| `Snippet` | ~100-character context around the first match |
| `Matched Keywords` | Which keywords were found (Multi mode) |
| `Missing Keywords` | Which keywords were not found (Multi mode) |
| `Notes` | Technical details for any issues — empty for normal results |

> The Excel output has **4 sheets**: All Results · Found · Not Found and Partial · Errors and Issues.

---

### ⚙️ Settings

| Setting | Default | What it does |
|---------|---------|-------------|
| **Concurrent Workers** | {DEFAULT_WORKERS} | Parallel downloads — keep 4–6 for z2data.com |
| **Timeout per URL** | {DEFAULT_TIMEOUT}s | Max wait — increase for large/slow PDFs |
| **Case-Sensitive** | OFF | OFF: `EAN123` matches `ean123` |
| **Enable Retry System** | ON | Automatically retries failed URLs in a second pass |
| **Enable Mirror Fallback** | ON | Tries `source1.z2data.com` if `source.z2data.com` fails |
| **Enable Smart Error Detection** | ON | Detects "not found" HTML pages returned as 200 OK |

---

### 🔄 Retry Logic

- **Pass 1**: every URL — 3 attempts with exponential back-off
- **Mirror fallback**: `source.z2data.com` ↔ `source1.z2data.com` (automatic)
- **Path fallback**: `/web/` paths tried without the `/web/` segment
- **15 s cooldown** before Pass 2
- **Pass 2**: failed rows only — no re-processing of already-successful rows

---

### ⚡ Performance Tips

- **4–6 workers** for z2data.com to avoid blocking
- **Timeout 20s** default; raise to 40s+ for large PDFs
- **CSV** is faster than Excel for 10,000+ rows
- Duplicate URLs (same URL, different keywords) are downloaded **only once** (cached)
- Check the **Logs tab** for a breakdown of any issues after a run
""")
