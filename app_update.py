"""
PDF Keyword Search System — v5.0
Fast • 5-Status • Light & Dark Adaptive
"""

import streamlit as st
import pandas as pd
import io, os, re, time, random, threading, tempfile, json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
import gc
from html.parser import HTMLParser

import fitz
import urllib3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ═══════════════════════════════════════════════════════════════════
# PAGE CONFIG
# ═══════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="PDF Keyword Search",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ═══════════════════════════════════════════════════════════════════
# CSS — Adaptive light + dark using CSS variables
# Colors chosen to be clear and readable in BOTH modes
# ═══════════════════════════════════════════════════════════════════
st.markdown("""
<style>
/* ── Adaptive color tokens ── */
:root {
    /* Improved light theme — stronger contrast, real depth */
    --bg-card:      #fcfcfd;
    --bg-sidebar:   #f3f6fb;
    --bg-log:       #eef2f9;
    --bg-info:      #e8f0fe;
    --border:       #bcc7d9;
    --text-primary: #111827;
    --text-muted:   #5b6475;
    --accent:       #005fcc;
    --accent-soft:  #dceeff;
    --success:      #1a6e2e;
    --success-bg:   #c6f0d2;
    --warn-bg:      #fef3c7;
    --warn-txt:     #7c5300;
    --err-bg:       #fde8e8;
    --err-txt:      #7b1a1a;
    --scan-bg:      #fef9e7;
    --scan-txt:     #6b4226;
    --fail-bg:      #fce4f0;
    --fail-txt:     #7a0a4a;
    --corrupt-bg:   #ede7f6;
    --corrupt-txt:  #3d1a8a;
    /* Extra depth tokens for light mode */
    --shadow-sm:    0 1px 3px rgba(0,0,0,.10), 0 1px 2px rgba(0,0,0,.06);
    --shadow-md:    0 4px 6px rgba(0,0,0,.08), 0 2px 4px rgba(0,0,0,.05);
}
@media (prefers-color-scheme: dark) {
    :root {
        --bg-card:      #1a1f2e;
        --bg-sidebar:   #111827;
        --bg-log:       #0f1117;
        --bg-info:      #0d1a2e;
        --border:       #2a3448;
        --text-primary: #e2e8f0;
        --text-muted:   #6b7fa3;
        --accent:       #4da3ff;
        --accent-soft:  #0d1a2e;
        --success:      #4caf50;
        --success-bg:   #0d2e14;
        --warn-bg:      #2e2200;
        --warn-txt:     #ffd54f;
        --err-bg:       #2e0a0a;
        --err-txt:      #ff5252;
        --scan-bg:      #2e2200;
        --scan-txt:     #ffcc02;
        --fail-bg:      #2e0022;
        --fail-txt:     #ff80ab;
        --corrupt-bg:   #1a0e2e;
        --corrupt-txt:  #b39ddb;
    }
}

/* ── Base ── */
.main { background: var(--bg-card); }
section[data-testid="stSidebar"] {
    background: var(--bg-sidebar) !important;
    border-right: 1px solid var(--border) !important;
}

/* ── Header ── */
.hdr {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-left: 4px solid var(--accent);
    border-radius: 10px;
    padding: 18px 26px;
    margin-bottom: 18px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    box-shadow: var(--shadow-md);
}
.hdr h1 {
    font-size: 1.65rem; font-weight: 800;
    color: var(--accent); margin: 0;
}
.hdr p { font-size: 0.82rem; color: var(--text-muted); margin: 3px 0 0; }

/* ── Status badges ── */
.badge { font-size: 0.76rem; font-weight: 700; padding: 4px 12px;
         border-radius: 16px; letter-spacing: .3px; border: 1px solid; }
.b-ready   { background: var(--success-bg); color: var(--success);
             border-color: var(--success); }
.b-running { background: var(--warn-bg);    color: var(--warn-txt);
             border-color: var(--warn-txt); }
.b-done    { background: var(--accent-soft); color: var(--accent);
             border-color: var(--accent); }

/* ── Stat cards ── */
.sc {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 14px 16px;
    text-align: center;
    box-shadow: var(--shadow-sm);
    transition: box-shadow .15s;
}
.sc:hover { box-shadow: var(--shadow-md); }
.sc-n { font-size: 1.75rem; font-weight: 800; line-height: 1; }
.sc-l { font-size: 0.68rem; color: var(--text-muted); margin-top: 4px;
        text-transform: uppercase; letter-spacing: .5px; }

/* ── Limit banner ── */
.lim {
    background: linear-gradient(90deg, #c0392b, #e74c3c);
    color: #fff; border-radius: 8px;
    padding: 8px 14px; font-weight: 700;
    font-size: .9rem; text-align: center; margin: 6px 0;
}

/* ── Log box ── */
.logbox {
    background: var(--bg-log);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 12px 16px;
    font-family: "Courier New", monospace;
    font-size: 0.76rem;
    color: var(--text-muted);
    max-height: 260px;
    overflow-y: auto;
    line-height: 1.65;
}

/* ── Pre-process summary box ── */
.ppbox {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-left: 3px solid var(--accent);
    border-radius: 0 10px 10px 0;
    padding: 15px 20px;
    margin: 10px 0;
    box-shadow: var(--shadow-sm);
}
.ppr { display: flex; justify-content: space-between;
       padding: 3px 0; font-size: 0.86rem; }
.ppk { color: var(--text-muted); }
.ppv { color: var(--text-primary); font-weight: 600; }

/* ── Info note ── */
.info-note {
    background: var(--bg-info);
    border-left: 3px solid var(--accent);
    border-radius: 0 8px 8px 0;
    padding: 10px 16px;
    font-size: 0.83rem;
    color: var(--text-muted);
    margin: 10px 0;
}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    gap: 4px; border-bottom: 2px solid var(--border); }
.stTabs [data-baseweb="tab"] {
    background: var(--bg-card);
    border-radius: 8px 8px 0 0;
    color: var(--text-muted);
    font-weight: 600; padding: 7px 18px;
    border: 1px solid var(--border); border-bottom: none; }
.stTabs [aria-selected="true"] {
    background: var(--accent-soft) !important;
    color: var(--accent) !important;
    border-color: var(--accent) !important; }

/* ── Buttons ── */
.stButton > button {
    border-radius: 7px; font-weight: 700; transition: all .12s; }
.stButton > button:hover {
    transform: translateY(-1px);
    box-shadow: 0 3px 8px rgba(0,0,0,.15); }

/* ── Section divider ── */
hr.d { border: none; border-top: 1px solid var(--border); margin: 12px 0; }
</style>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════
# SECTION 1 — Constants + exact 5-status model
# ═══════════════════════════════════════════════════════════════════
SEARCH_LIMIT     = 50_000
DEFAULT_WORKERS  = 10     # faster default
DEFAULT_TIMEOUT  = 15     # tighter timeout for speed
_CONNECT_TIMEOUT = 8      # fast TCP fail
_MIN_DELAY_SECS  = 0.1    # light rate limiting
_BLOCK_THRESHOLD = 6
_BLOCK_COOLDOWN  = 25

_ILLEGAL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")

_TMP            = tempfile.gettempdir()
_AUTOSAVE_CSV   = os.path.join(_TMP, "pdf_search_autosave.csv")
_JOB_STATE_FILE = os.path.join(_TMP, "pdf_search_job_state.json")


class S:
    """Exact 5 user-facing status values — nothing else in output."""
    FOUND      = "Found"
    NOT_FOUND  = "Not Found"
    SCANNED    = "PDF is Non searchable,Advanced Scanned Extraction can make the PDF searchable."
    CORRUPTED  = "PDF Not mirrored / Corrupted"
    FAILED     = "Failed to get PDF text"   # network / download / parse failures


# Internal categories → mapped to one of the 5 public statuses
_FAIL_CATS   = {"ssl", "timeout", "blocked", "connection", "404", "corrupted"}
_RETRY_CATS  = {"ssl", "timeout", "connection"}   # retry these; not 404/blocked

# Output columns — exact order
_OUT_COLS = [
    "URL", "Keyword", "Search Mode",
    "Keyword_Search_Status",   # main result — one of the 5 above
    "Match Count",
    "Snippet",
    "Matched Keywords",
    "Missing Keywords",
    "Notes",                   # non-empty only for FAILED rows
]

_HTML_NFP = [
    "page not found", "404 not found", "404 error", "file not found",
    "resource not found", "does not exist", "error 404", "no results found",
    "page unavailable", "could not be found", "not available",
    "page cannot be found", "this page doesn", "sorry, we couldn",
]

# ── PDF size + extraction limits ──────────────────────────────────
MAX_PDF_MB        = 40
MAX_PDF_PAGES     = 500
MAX_TEXT_CHARS    = 3_000_000   # truncate extraction at 3M chars
MIN_USEFUL_CHARS  = 150         # below this = corrupted stub

# ── In-progress dedup cache ───────────────────────────────────────
# Prevents the same URL being downloaded by multiple workers simultaneously.
# Worker that arrives second WAITS for the first worker's future result.
_inprog_lock    = threading.Lock()
_inprog_futures: dict[str, object] = {}   # normalized_url → Future

# ── URL normalizer ────────────────────────────────────────────────
def _norm_url(url: str) -> str:
    """Lowercase host, strip fragment, strip trailing spaces — for cache keys."""
    try:
        url = url.strip()
        if "://" in url:
            scheme, rest = url.split("://", 1)
            if "/" in rest:
                host, path = rest.split("/", 1)
                url = f"{scheme}://{host.lower()}/{path}"
            else:
                url = f"{scheme}://{rest.lower()}"
        return url.split("#")[0].rstrip("/")
    except Exception:
        return url.strip()

# ═══════════════════════════════════════════════════════════════════
# SECTION 2 — Network layer
# ═══════════════════════════════════════════════════════════════════
_thread_local = threading.local()

_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Accept":          "text/html,application/pdf,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "Referer":         "https://www.google.com/",
}

_rate_lock  = threading.Lock()
_last_req:  dict[str, float] = {}
_block_lock = threading.Lock()
_consec_fail: dict[str, int]   = {}
_blocked_til: dict[str, float] = {}
_cache_lock  = threading.Lock()
_url_cache:  dict[str, tuple[str, str]] = {}


def _make_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update(_HEADERS)
    sess.verify = False
    a = HTTPAdapter(max_retries=Retry(total=0, raise_on_status=False),
                    pool_connections=25, pool_maxsize=25)
    sess.mount("https://", a)
    sess.mount("http://", a)
    return sess


def _get_session(fresh: bool = False) -> requests.Session:
    if fresh or not hasattr(_thread_local, "session"):
        _thread_local.session = _make_session()
    return _thread_local.session


def _host(url: str) -> str:
    try:    return url.split("/")[2]
    except: return url


def _rate_limit(host: str) -> None:
    with _rate_lock:
        gap = time.time() - _last_req.get(host, 0)
        if gap < _MIN_DELAY_SECS:
            time.sleep(_MIN_DELAY_SECS - gap)
        _last_req[host] = time.time()


def _record_failure(host: str) -> None:
    with _block_lock:
        _consec_fail[host] = _consec_fail.get(host, 0) + 1
        if _consec_fail[host] >= _BLOCK_THRESHOLD:
            _blocked_til[host] = time.time() + _BLOCK_COOLDOWN
            _consec_fail[host] = 0


def _record_success(host: str) -> None:
    with _block_lock:
        _consec_fail[host] = 0


def _wait_if_blocked(host: str) -> None:
    wait = _blocked_til.get(host, 0) - time.time()
    if wait > 0:
        time.sleep(wait)


def _clear_all_state() -> None:
    with _cache_lock:   _url_cache.clear()
    with _block_lock:   _consec_fail.clear(); _blocked_til.clear()
    with _rate_lock:    _last_req.clear()
    with _inprog_lock:  _inprog_futures.clear()


def _get_alternate_urls(url: str) -> list[str]:
    """Swap between mirror hosts + strip /web/ path segment."""
    alts: list[str] = []
    parts = url.split("//", 1)
    if len(parts) == 2:
        host_path = parts[1]
        host      = host_path.split("/")[0]
        # Generic mirror swap: host ↔ host with "1" appended/removed
        if host.endswith("1") and not host.endswith("11"):
            alt_host = host[:-1]
        else:
            alt_host = host + "1"
        alts.append(url.replace(f"//{host}", f"//{alt_host}", 1))
    if "/web/" in url:
        stripped = url.replace("/web/", "/", 1)
        alts.append(stripped)
        for a in list(alts[:-1]):
            if "/web/" in a:
                alts.append(a.replace("/web/", "/", 1))
    return alts

# ═══════════════════════════════════════════════════════════════════
# SECTION 3 — Download + retry
# ═══════════════════════════════════════════════════════════════════
def _download_one(url: str, timeout: int,
                  fresh: bool = False) -> tuple[bytes | None, str]:
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
            time.sleep(4 + random.random() * 4)
            return None, "blocked"
        if code == 403:
            return None, "blocked"
        if code in (404, 410):
            return None, "404"
        return None, f"http_{code}"
    except Exception as e:
        _record_failure(host)
        s = str(e).lower()
        if any(w in s for w in ("ssl", "cert", "tls", "handshake")):
            return None, "ssl"
        if any(w in s for w in ("timed out", "timeout", "read timed")):
            return None, "timeout"
        return None, "connection"


def _fetch(url: str, session_timeout: int,
           use_mirror: bool = True) -> tuple[bytes | None, str]:
    """Primary + alternates, 3 attempts each. Permanent 404 exits immediately."""
    candidates = [url] + (_get_alternate_urls(url) if use_mirror else [])
    last_cat = "timeout"
    conn_err = False

    for try_url in candidates:
        for attempt in range(1, 4):
            content, cat = _download_one(
                try_url, session_timeout, fresh=(conn_err and attempt == 1)
            )
            conn_err = False

            if content is not None:
                if len(content) < 32:
                    last_cat = "corrupted"; break
                sig = content[:64].lstrip()
                if len(content) < 64 and \
                   not sig.startswith(b"%PDF") and \
                   not any(t in sig.lower() for t in (b"<html", b"<!doc", b"<head")):
                    last_cat = "corrupted"; break
                return content, "ok"

            last_cat = cat
            if cat == "404":     return None, "404"
            if cat == "blocked": break              # no retry on rate-limit
            if cat == "ssl":     break              # SSL won't self-fix on retry
            if cat in ("timeout", "connection"):
                conn_err = True

            if attempt < 3:
                time.sleep((2 ** attempt) * (0.5 + 0.5 * random.random()))

    return None, last_cat

# ═══════════════════════════════════════════════════════════════════
# SECTION 4 — Text extraction
# ═══════════════════════════════════════════════════════════════════
def _extract_pdf(data: bytes) -> tuple[str, str]:
    """Extract text with size limits, explicit memory cleanup."""
    # Size check before parsing
    if len(data) > MAX_PDF_MB * 1024 * 1024:
        return "", f"error:PDF too large ({len(data)//1024//1024}MB > {MAX_PDF_MB}MB)"
    try:
        doc = fitz.open(stream=data, filetype="pdf")
        page_count = doc.page_count
        if page_count > MAX_PDF_PAGES:
            doc.close(); del doc
            return "", f"error:Too many pages ({page_count} > {MAX_PDF_PAGES})"
        parts = []
        total_chars = 0
        for page in doc:
            t = page.get_text("text")
            total_chars += len(t)
            parts.append(t)
            if total_chars >= MAX_TEXT_CHARS:
                break   # truncate — we have enough to search
        doc.close()
        del doc
        text = "\n".join(parts)[:MAX_TEXT_CHARS]
        return (text, "searchable") if text.strip() else ("", "scanned")
    except Exception as e:
        return "", f"error:{e}"


class _HtmlStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts: list[str] = []
        self._skip = False
    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style", "noscript", "head"): self._skip = True
    def handle_endtag(self, tag):
        if tag in ("script", "style", "noscript", "head"): self._skip = False
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


def _get_text_cached(url: str, content: bytes,
                     is_html: bool) -> tuple[str, str]:
    """
    URL-level text cache — same document never extracted twice.
    Also deduplicates concurrent extraction of the same URL:
    if worker A is already extracting url, worker B waits for A's result
    instead of extracting again (in-progress dedup).
    """
    nurl = _norm_url(url)

    # Fast path: already extracted
    with _cache_lock:
        if nurl in _url_cache:
            return _url_cache[nurl]

    # In-progress dedup: register or wait
    my_event = threading.Event()
    with _inprog_lock:
        if nurl in _inprog_futures:
            waiter = _inprog_futures[nurl]
        else:
            _inprog_futures[nurl] = my_event
            waiter = None

    if waiter is not None:
        # Another thread is extracting — wait up to 60 s then fall back
        waiter.wait(timeout=60)
        with _cache_lock:
            if nurl in _url_cache:
                return _url_cache[nurl]
        # Fall through to extract ourselves if waiter never set

    # We are the first — extract
    try:
        if is_html:
            text, status = _extract_html(content)
            if not text and "error:" not in status:
                text, status = _extract_pdf(content)
        else:
            text, status = _extract_pdf(content)
            if "error:" in status:
                text, status = _extract_html(content)
    finally:
        with _cache_lock:
            _url_cache[nurl] = (text, status)
        with _inprog_lock:
            _inprog_futures.pop(nurl, None)
        my_event.set()   # wake any waiters

    return text, status


def _is_not_found_page(text: str) -> bool:
    if not text: return False
    return any(p in text[:1000].lower() for p in _HTML_NFP)

# ═══════════════════════════════════════════════════════════════════
# SECTION 5 — Normalization + keyword search
# ═══════════════════════════════════════════════════════════════════
def _normalize(text: str) -> str:
    text = _ILLEGAL_RE.sub(" ", text)
    text = text.replace("\u00ad", "").replace("\u00a0", " ")
    text = re.sub(r"-\s*\n\s*", "", text)
    text = re.sub(r"\s+", " ", text)
    return text


def _parse_keywords(raw: str, mode: str) -> list[str]:
    raw = str(raw).strip()
    if mode == "single": return [raw]
    if mode in ("multi", "table"):
        parts = [k.strip() for k in raw.split("|") if k.strip()]
        return parts if parts else [raw]
    if "|" in raw:
        return [k.strip() for k in raw.split("|") if k.strip()]
    return [raw]


def _count_keyword(text: str, keyword: str, case_sensitive: bool) -> int:
    """Fast count — uses str.count for simple single-keyword, regex only when needed."""
    if not text or not keyword: return 0
    kw = keyword.strip()
    if not kw: return 0
    # Fast path: plain substring count (much faster than re.findall on large text)
    if not case_sensitive:
        return text.lower().count(kw.lower())
    else:
        return text.count(kw)


def _search_all(text: str, keywords: list[str], case_sensitive: bool,
                match_all: bool) -> tuple[str, list[str], list[str], int]:
    found, missing, total = [], [], 0
    for kw in keywords:
        cnt = _count_keyword(text, kw, case_sensitive)
        (found if cnt > 0 else missing).append(kw)
        total += cnt
    if not keywords: return S.NOT_FOUND, [], [], 0
    if len(found) == len(keywords): return S.FOUND, found, [], total
    if found:
        # match_all → partial; match_any → still Found
        status = S.NOT_FOUND if match_all else S.FOUND
        # (we keep partial logic but map to NOT_FOUND when match_all
        #  and not all found — stays within 5 statuses)
        return (S.FOUND if not match_all else S.NOT_FOUND), found, missing, total
    return S.NOT_FOUND, [], missing, 0


def _best_snippet(text: str, keyword: str, ctx: int = 120) -> str:
    """Build context snippet. Skips expensive scan on very large text."""
    if not text or not keyword: return ""
    # For very large texts only scan the first 500k chars — keyword will be found there
    scan_text = text[:500_000] if len(text) > 500_000 else text
    kw = str(keyword).lower(); tl = scan_text.lower()
    positions, start, best, seen = [], 0, "", set()
    while True:
        i = tl.find(kw, start)
        if i == -1: break
        positions.append(i); start = i + 1
        if len(positions) >= 10: break   # only need a few candidates
    for i in positions:
        s = max(0, i - ctx); e = min(len(scan_text), i + len(kw) + ctx)
        raw = _ILLEGAL_RE.sub("", scan_text[s:e].strip())
        if not raw or raw in seen: continue
        seen.add(raw)
        cand = ("…" if s > 0 else "") + raw + ("…" if e < len(scan_text) else "")
        if len(raw) > len(best.replace("…", "")): best = cand
    return best

# ═══════════════════════════════════════════════════════════════════
# SECTION 6 — Main per-row processor
# Returns ONE dict. Status is always one of the exact 5 values.
# ═══════════════════════════════════════════════════════════════════
def process_one(url: str, raw_keyword: str, search_mode: str,
                match_all: bool, case_sensitive: bool,
                session_timeout: int, row_id: int = 0,
                use_mirror: bool = True,
                use_smart: bool  = True) -> dict:

    base = {
        "_row_id":       row_id,
        "URL":           url,
        "Keyword":       raw_keyword,
        "Search Mode":   search_mode.capitalize(),
        "Keyword_Search_Status": "",
        "Match Count":   0,
        "Snippet":       "",
        "Matched Keywords": "",
        "Missing Keywords": "",
        "Notes":         "",
        "_cat":          "",   # internal download category for retry logic
    }

    def done(**kw) -> dict:
        r = dict(base); r.update(kw); return r

    url = str(url).strip()
    if not url or not url.startswith("http"):
        return done(Keyword_Search_Status=S.FAILED, Notes="Invalid URL", _cat="404")

    is_html = url.lower().split("?")[0].endswith((".html", ".htm"))

    # ── Download ──────────────────────────────────────────────────
    content, dl_cat = _fetch(url, session_timeout, use_mirror=use_mirror)

    if content is None:
        # Map all download failures → S.FAILED
        return done(Keyword_Search_Status=S.FAILED,
                    Notes=dl_cat, _cat=dl_cat)

    # ── Extract ──────────────────────────────────────────────────
    text, ext_status = _get_text_cached(url, content, is_html)

    if "error:" in ext_status or ext_status == "":
        return done(Keyword_Search_Status=S.CORRUPTED, _cat="corrupted")

    if ext_status == "scanned":
        return done(Keyword_Search_Status=S.SCANNED)

    norm = _normalize(text)

    # Short extracted text = corrupted/garbage file stub
    if len(norm.strip()) < MIN_USEFUL_CHARS:
        return done(Keyword_Search_Status=S.CORRUPTED, _cat="corrupted")

    if use_smart and _is_not_found_page(norm):
        # Server returned a 200 OK "not found" HTML page → treat as failed
        return done(Keyword_Search_Status=S.FAILED,
                    Notes="URL returned a not-found page")

    # ── Search ────────────────────────────────────────────────────
    keywords = _parse_keywords(raw_keyword, search_mode)
    result, found_kws, missing_kws, total_cnt = _search_all(
        norm, keywords, case_sensitive, match_all
    )
    snippet = _best_snippet(norm, found_kws[0]) if found_kws else ""

    return done(
        Keyword_Search_Status=result,
        **{"Match Count":      total_cnt,
           "Snippet":          snippet,
           "Matched Keywords": ", ".join(found_kws),
           "Missing Keywords": ", ".join(missing_kws)},
    )

# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — Output DataFrame + export helpers
# ═══════════════════════════════════════════════════════════════════
def _clean_cell(v):
    return _ILLEGAL_RE.sub("", v) if isinstance(v, str) else v


def _build_df(result_dicts: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(result_dicts)
    for c in _OUT_COLS:
        if c not in df.columns: df[c] = ""
    df["Match Count"] = pd.to_numeric(
        df.get("Match Count", 0), errors="coerce"
    ).fillna(0).astype(int)
    return df[_OUT_COLS].copy()


def _to_excel(df: pd.DataFrame) -> bytes:
    fn = getattr(df, "map", None) or df.applymap
    clean = fn(_clean_cell)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        clean.to_excel(w, index=False, sheet_name="All Results")
        for status, sheet in [
            (S.FOUND,     "Found"),
            (S.NOT_FOUND, "Not Found"),
            (S.SCANNED,   "Scanned"),
            (S.CORRUPTED, "Corrupted"),
            (S.FAILED,    "Failed"),
        ]:
            sub = clean[clean["Keyword_Search_Status"] == status]
            if not sub.empty:
                sub.to_excel(w, index=False, sheet_name=sheet)
    return buf.getvalue()


def _to_csv(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def _status_badge(val) -> str:
    """Color styles that read well in BOTH light and dark mode."""
    v = str(val)
    if v == S.FOUND:     return "background-color:#d4edda;color:#1e7e34;font-weight:700"
    if v == S.NOT_FOUND: return "background-color:#f8d7da;color:#721c24"
    if v == S.SCANNED:   return "background-color:#fff3cd;color:#795548"
    if v == S.CORRUPTED: return "background-color:#ede7f6;color:#4527a0"
    if v == S.FAILED:    return "background-color:#fce4ec;color:#880e4f"
    return ""

# ═══════════════════════════════════════════════════════════════════
# SECTION 8 — Autosave / Recovery
# ═══════════════════════════════════════════════════════════════════
# Append-only autosave: never rewrite entire file — just append new rows.
# Avoids O(n²) IO as the batch grows.
_autosave_initialized = [False]

def _autosave(result_dicts: list[dict], processed: int, total: int) -> None:
    """Append new rows to autosave CSV. Much faster than full rewrite each time."""
    try:
        if result_dicts:
            df_batch = pd.DataFrame(result_dicts[-100:])  # only the latest 100
            for c in _OUT_COLS:
                if c not in df_batch.columns: df_batch[c] = ""
            write_header = not _autosave_initialized[0] or not os.path.exists(_AUTOSAVE_CSV)
            df_batch[_OUT_COLS].to_csv(
                _AUTOSAVE_CSV,
                mode="a",
                header=write_header,
                index=False,
            )
            _autosave_initialized[0] = True
        with open(_JOB_STATE_FILE, "w") as f:
            json.dump({"processed": processed, "total": total,
                       "saved_at": datetime.now().isoformat()}, f)
    except Exception:
        pass

def _reset_autosave_state() -> None:
    _autosave_initialized[0] = False


def _load_autosave() -> tuple[pd.DataFrame | None, dict | None]:
    df, state = None, None
    try:
        if os.path.exists(_AUTOSAVE_CSV) and os.path.getsize(_AUTOSAVE_CSV) > 0:
            df = pd.read_csv(_AUTOSAVE_CSV, dtype={"Keyword": str, "URL": str})
    except Exception: pass
    try:
        if os.path.exists(_JOB_STATE_FILE):
            with open(_JOB_STATE_FILE) as f: state = json.load(f)
    except Exception: pass
    return df, state


def _clear_autosave() -> None:
    for p in (_AUTOSAVE_CSV, _JOB_STATE_FILE):
        try: os.remove(p)
        except Exception: pass

# ═══════════════════════════════════════════════════════════════════
# SECTION 9 — Template
# ═══════════════════════════════════════════════════════════════════
def _make_template() -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        pd.DataFrame({
            "URL":     ["https://example.com/document.pdf",
                        "https://example.com/page.html"],
            "Keyword": ["your keyword here", "another keyword"],
        }).to_excel(w, index=False, sheet_name="Single Search")
        pd.DataFrame({
            "URL":     ["https://example.com/document.pdf"],
            "Keyword": ["keyword1|keyword2|keyword3"],
        }).to_excel(w, index=False, sheet_name="Multi Search")
    return buf.getvalue()

# ═══════════════════════════════════════════════════════════════════
# SECTION 10 — Session state
# ═══════════════════════════════════════════════════════════════════
for _k, _v in [
    ("results_df", None), ("running", False), ("paused", False),
    ("log_lines", []),    ("error_log", []),
]:
    if _k not in st.session_state:
        st.session_state[_k] = _v

# ═══════════════════════════════════════════════════════════════════
# SECTION 11 — UI helpers
# ═══════════════════════════════════════════════════════════════════
def _header() -> None:
    if st.session_state.running:
        badge = '<span class="badge b-running">⏳ Running</span>'
    elif st.session_state.results_df is not None:
        badge = '<span class="badge b-done">✅ Complete</span>'
    else:
        badge = '<span class="badge b-ready">🟢 Ready</span>'
    st.markdown(f"""
    <div class="hdr">
        <div>
            <h1>🔍 PDF Keyword Search</h1>
            <p>Fast • Reliable • 5-Status Output</p>
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


def _stats_row(total, found, not_found, scanned, corrupted, failed) -> None:
    cols = st.columns(6)
    _stat(cols[0], total,     "#1a73e8", "Total")
    _stat(cols[1], found,     "#1e7e34", "Found")
    _stat(cols[2], not_found, "#c62828", "Not Found")
    _stat(cols[3], scanned,   "#795548", "Scanned")
    _stat(cols[4], corrupted, "#4527a0", "Corrupted")
    _stat(cols[5], failed,    "#880e4f", "Failed")

# ═══════════════════════════════════════════════════════════════════
# SECTION 12 — SIDEBAR
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
            "**Auto Detect:** picks mode based on keyword content.\n\n"
            "**Single Search:** one keyword per row.\n\n"
            "**Multi Search:** separate with `|` e.g. `keyword1|keyword2`.\n\n"
            "**Table Search:** numeric/code values, also split on `|`."
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
                "**ANY:** Found if at least one keyword matches.\n\n"
                "**ALL:** Found only when every keyword matches."
            ),
        ) == "Match ALL keywords"

    case_sensitive = st.checkbox("Case-Sensitive Search", value=False,
        help="OFF: `ABC` matches `abc`. ON: exact case required.")

    # ── Performance ────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### 🚀 Performance")

    workers = st.slider("Concurrent Workers", 2, 20, DEFAULT_WORKERS, 1,
        help=(
            "• **2–4** → safest, low error rate\n"
            "• **8–10** → fast default\n"
            "• **16–20** → fast servers only\n\n"
            "Reduce if you see many Failed results."
        ))
    timeout = st.slider("Timeout per URL (sec)", 5, 60, DEFAULT_TIMEOUT, 5,
        help="Max wait per URL. Raise for large/slow documents.")

    # ── Advanced ───────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### 🛡 Advanced")
    enable_retry  = st.checkbox("Retry Failed URLs",         value=True)
    enable_mirror = st.checkbox("Mirror Fallback",           value=True)
    enable_smart  = st.checkbox("Smart Error Detection",     value=True)
    output_format = st.radio("Output Format",
                             ["Excel (.xlsx)", "CSV (.csv)"], index=0)

    # ── Template ──────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### 📥 Template")
    st.download_button(
        "⬇️ Download Template",
        data=_make_template(),
        file_name="PDF_Search_Template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

    # ── Status Guide ──────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### 📊 Status Guide")
    st.markdown(f"""
- 🟢 **{S.FOUND}**
- 🔴 **{S.NOT_FOUND}**
- 🟡 **Scanned** (non-searchable PDF)
- 🟣 **{S.CORRUPTED}**
- 🔺 **{S.FAILED}**
""")

    # ── Recovery ──────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### 💾 Recovery")
    _saved_df, _job_state = _load_autosave()
    if _saved_df is not None:
        _n    = len(_saved_df)
        _prog = (_job_state or {}).get("processed", _n)
        _tot  = (_job_state or {}).get("total", _n)
        _ts   = (_job_state or {}).get("saved_at", "")
        st.success(f"📂 **{_n:,} rows** saved ({_prog:,}/{_tot:,})")
        if _ts: st.caption(f"Saved: {_ts[:19]}")
        _now = datetime.now().strftime("%Y%m%d_%H%M%S")
        _sv_clean = _saved_df[_OUT_COLS] if all(
            c in _saved_df.columns for c in _OUT_COLS) else _saved_df
        st.download_button(f"📥 CSV ({_n:,})",
            data=_sv_clean.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"autosave_{_now}.csv", mime="text/csv",
            use_container_width=True, key="sb_csv")
        try:
            st.download_button(f"📥 Excel ({_n:,})",
                data=_to_excel(_sv_clean),
                file_name=f"autosave_{_now}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True, key="sb_xlsx")
        except Exception: pass
        r1, r2 = st.columns(2)
        with r1:
            if st.button("♻️ Restore", use_container_width=True, key="rst"):
                st.session_state.results_df = _sv_clean; st.success("✅")
        with r2:
            if st.button("🗑 Clear", use_container_width=True, key="clr"):
                _clear_autosave(); st.rerun()
    else:
        st.caption("Auto-saves every 100 rows during a search.")

# ═══════════════════════════════════════════════════════════════════
# SECTION 13 — MAIN CONTENT
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
        uploaded = st.file_uploader("",
            type=["xlsx", "xls", "csv"], label_visibility="collapsed")
    with c_fmt:
        st.markdown("### 📌 Required Format")
        st.markdown("""
| Column | Example |
|--------|---------|
| `URL` | `https://…/file.pdf` |
| `Keyword` | `search term` |

Multi: `term1|term2|term3`
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
                st.error(f"❌ Missing columns: **{', '.join(bad_cols)}**")
            else:
                idf = idf.dropna(subset=["URL"]).reset_index(drop=True)
                if len(idf) > SEARCH_LIMIT:
                    st.warning(f"⚠️ Capped at {SEARCH_LIMIT:,} rows.")
                    idf = idf.head(SEARCH_LIMIT)

                total_rows  = len(idf)
                unique_urls = idf["URL"].nunique()
                dup_urls    = total_rows - unique_urls

                st.success(f"✅ **{total_rows:,}** rows loaded")

                with st.expander("🔎 Preview (first 10 rows)", expanded=False):
                    st.dataframe(idf.head(10), use_container_width=True)

                st.markdown("<hr class='d'>", unsafe_allow_html=True)

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

                _cache_note = (
                    f"✅ {dup_urls:,} duplicate URLs — text cached, downloaded once"
                    if dup_urls > 0 else "No duplicate URLs detected"
                )

                st.markdown(f"""
<div class="ppbox">
  <div class="ppr"><span class="ppk">Total Rows</span>
    <span class="ppv">{total_rows:,}</span></div>
  <div class="ppr"><span class="ppk">Unique URLs</span>
    <span class="ppv">{unique_urls:,}</span></div>
  <div class="ppr"><span class="ppk">URL Cache</span>
    <span class="ppv">{_cache_note}</span></div>
  <div class="ppr"><span class="ppk">Search Mode</span>
    <span class="ppv">{_auto_desc}</span></div>
  <div class="ppr"><span class="ppk">Match Logic</span>
    <span class="ppv">{"Match ALL" if match_all else "Match ANY"}</span></div>
  <div class="ppr"><span class="ppk">Case-Sensitive</span>
    <span class="ppv">{"Yes" if case_sensitive else "No"}</span></div>
  <div class="ppr"><span class="ppk">Workers / Timeout</span>
    <span class="ppv">{workers} / {timeout}s</span></div>
</div>
""", unsafe_allow_html=True)

                st.markdown("""
<div class="info-note">
💾 Results are auto-saved every 100 rows.
If the page refreshes or your connection drops, use
<strong>Recovery</strong> in the sidebar to restore partial results.
</div>
""", unsafe_allow_html=True)

                st.markdown("<hr class='d'>", unsafe_allow_html=True)

                # ── Action Buttons ─────────────────────────────────
                b1, b2, b3, _ = st.columns([2, 1, 1, 1])
                with b1:
                    start = st.button("🚀 Start Search",
                                      use_container_width=True, type="primary",
                                      disabled=st.session_state.running)
                with b2:
                    if st.button("⏸ Pause/Resume", use_container_width=True,
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
                    _clear_all_state()
                    _reset_autosave_state()

                    idf = idf.copy()
                    idf["_row_id"] = range(len(idf))
                    rows    = idf.to_dict("records")
                    total   = len(rows)
                    prog    = st.progress(0, text="Starting…")
                    mtrs    = st.empty()
                    curl    = st.empty()
                    lbox    = st.empty()
                    results: list[dict] = []
                    done_n:  list[int]  = [0]
                    start_t = time.time()

                    def _log(msg: str) -> None:
                        ts = datetime.now().strftime("%H:%M:%S")
                        st.session_state.log_lines.append(f"[{ts}] {msg}")
                        if len(st.session_state.log_lines) > 500:
                            st.session_state.log_lines.pop(0)

                    def _log_err(url_: str, status_: str) -> None:
                        st.session_state.error_log.append({
                            "Time": datetime.now().strftime("%H:%M:%S"),
                            "URL": url_, "Status": status_,
                        })

                    def _save(dicts: list[dict]) -> None:
                        """Build and assign results_df on main thread. Safe for Streamlit."""
                        if dicts:
                            st.session_state.results_df = _build_df(dicts)

                    def _needs_retry(r: dict) -> bool:
                        return r.get("_cat", "") in _RETRY_CATS

                    def _run_pass(work: list[dict],
                                  label: str) -> tuple[list[dict], list[dict]]:
                        pass_res:  list[dict] = []
                        pass_err:  list[dict] = []
                        found_ctr: list[int]  = [0]

                        _in_flight:    dict[object, dict]  = {}   # future → row
                        _submit_times: dict[object, float] = {}   # future → submit time
                        _work_iter = iter(work)
                        _stopped   = False
                        # Throttle UI updates — max once per 0.75s
                        _last_ui   = [0.0]
                        # Per-future hard deadline = submit_time + timeout + 10s buffer
                        _HARD_DEADLINE = timeout + 10

                        def _submit_next():
                            try:
                                r = next(_work_iter)
                            except StopIteration:
                                return None
                            while (st.session_state.paused
                                   and st.session_state.running):
                                time.sleep(0.4)
                            if not st.session_state.running:
                                return None
                            f = ex.submit(
                                process_one,
                                str(r.get("URL", "")),
                                str(r.get("Keyword", "")),
                                _MODE, match_all, case_sensitive,
                                timeout,
                                int(r.get("_row_id", 0)),
                                enable_mirror, enable_smart,
                            )
                            _in_flight[f]    = r
                            _submit_times[f] = time.time()
                            return f

                        def _collect_one(future, src_row) -> None:
                            """Process one completed future into pass_res."""
                            try:
                                res = future.result(timeout=0)
                            except Exception as exc:
                                res = {
                                    "_row_id":  int(src_row.get("_row_id", 0)),
                                    "URL":      str(src_row.get("URL", "")),
                                    "Keyword":  str(src_row.get("Keyword", "")),
                                    "Search Mode": _MODE.capitalize(),
                                    "Keyword_Search_Status": S.FAILED,
                                    "Match Count": 0, "Snippet": "",
                                    "Matched Keywords": "",
                                    "Missing Keywords": "",
                                    "Notes": f"Exception: {exc}",
                                    "_cat": "connection",
                                }
                            done_n[0] += 1
                            ks    = res.get("Keyword_Search_Status", "")
                            icon  = ("✅" if ks == S.FOUND else
                                     "❌" if ks == S.NOT_FOUND else
                                     "🟡" if ks == S.SCANNED else
                                     "🟣" if ks == S.CORRUPTED else "🔺")
                            url_s = res["URL"][-55:] if len(res["URL"]) > 55 else res["URL"]
                            _log(f"[{label}][{done_n[0]}/{total}] "
                                 f"{icon} {ks[:26]:26s} …{url_s}")
                            if _needs_retry(res):
                                pass_err.append(src_row)
                                _log_err(res["URL"], ks)
                            pass_res.append(res)
                            if ks == S.FOUND:
                                found_ctr[0] += 1
                            if done_n[0] % 100 == 0:
                                _autosave(pass_res, done_n[0], total)
                                _log(f"💾 Auto-saved checkpoint at {done_n[0]:,} rows")
                            # Throttled UI update — max once per 0.75s
                            now = time.time()
                            if now - _last_ui[0] >= 0.75 or done_n[0] == total:
                                _last_ui[0] = now
                                pct  = min(done_n[0] / total, 1.0)
                                el   = now - start_t
                                rate = done_n[0] / el if el else 0
                                eta  = (total - done_n[0]) / rate if rate else 0
                                prog.progress(pct,
                                    text=f"[{label}] {done_n[0]:,}/{total:,} "
                                         f"• {rate:.1f}/s • ETA {eta:.0f}s")
                                mtrs.markdown(
                                    f"⏱ **{el:.0f}s** &nbsp;|&nbsp; "
                                    f"⚡ **{rate:.1f}** URLs/s &nbsp;|&nbsp; "
                                    f"✅ **{found_ctr[0]:,}** found &nbsp;|&nbsp; "
                                    f"📊 **{done_n[0]:,}/{total:,}**"
                                )
                                curl.markdown(f"`…{url_s}`")
                                lbox.markdown(
                                    '<div class="logbox">' +
                                    "<br>".join(st.session_state.log_lines[-35:]) +
                                    "</div>", unsafe_allow_html=True)

                        with ThreadPoolExecutor(max_workers=workers) as ex:
                            # Pre-fill pool
                            for _ in range(workers * 2):
                                if _submit_next() is None:
                                    break

                            while _in_flight:
                                if not st.session_state.running:
                                    _log("⏹ Stopped by user.")
                                    ex.shutdown(wait=False, cancel_futures=True)
                                    _stopped = True
                                    break

                                # ── Expire futures that have hit their hard deadline
                                now = time.time()
                                expired = [f for f, t in _submit_times.items()
                                           if now - t > _HARD_DEADLINE
                                           and f in _in_flight]
                                for f in expired:
                                    src_row = _in_flight.pop(f, None)
                                    _submit_times.pop(f, None)
                                    if src_row is None: continue
                                    _log(f"⚠️ Hard deadline hit — {str(src_row.get('URL',''))[-45:]}")
                                    pass_res.append({
                                        "_row_id": int(src_row.get("_row_id", 0)),
                                        "URL":     str(src_row.get("URL", "")),
                                        "Keyword": str(src_row.get("Keyword", "")),
                                        "Search Mode": _MODE.capitalize(),
                                        "Keyword_Search_Status": S.FAILED,
                                        "Match Count": 0, "Snippet": "",
                                        "Matched Keywords": "", "Missing Keywords": "",
                                        "Notes": "Per-future hard deadline exceeded",
                                        "_cat": "timeout",
                                    })
                                    done_n[0] += 1
                                    pass_err.append(src_row)
                                    _submit_next()   # fill freed slot

                                if not _in_flight:
                                    break

                                # ── wait() never raises TimeoutError — safe replacement for as_completed(timeout=)
                                try:
                                    done_set, _ = wait(
                                        list(_in_flight.keys()),
                                        timeout=0.5,             # poll every 500ms
                                        return_when=FIRST_COMPLETED,
                                    )
                                except Exception:
                                    done_set = set()

                                for future in done_set:
                                    src_row = _in_flight.pop(future, None)
                                    _submit_times.pop(future, None)
                                    if src_row is None: continue
                                    _collect_one(future, src_row)
                                    _submit_next()   # keep pool full

                        return pass_res, pass_err

                    # ── Pass 1 ─────────────────────────────────────
                    _log(f"🚀 Pass 1 — {total:,} rows, {workers} workers, mode={_MODE}")
                    p1, p1_err = _run_pass(rows, "Pass 1")
                    results.extend(p1)

                    # ── Pass 2 — retry network failures ────────────
                    if enable_retry and p1_err and st.session_state.running:
                        _log(f"♻️ Pass 2 — {len(p1_err):,} failed rows…")
                        err_ids = {r.get("_row_id") for r in p1_err}
                        results = [r for r in results
                                   if r.get("_row_id") not in err_ids]
                        total  += len(p1_err)
                        _log("⏳ 12s cooldown…")
                        time.sleep(12)
                        p2, still = _run_pass(p1_err, "Pass 2")
                        results.extend(p2)
                        _log(f"{'✅ All resolved' if not still else f'⚠️ {len(still):,} still failed'} after Pass 2")

                    st.session_state.running = False
                    st.session_state.paused  = False

                    if results:
                        el = time.time() - start_t
                        # Build final DataFrame on the main thread (safe for Streamlit).
                        # _build_df is fast — it only reshapes dicts already in memory.
                        # The incremental autosave already wrote every 100 rows to disk.
                        st.session_state.results_df = _build_df(results)
                        # Final state snapshot
                        with open(_JOB_STATE_FILE, "w") as _f:
                            import json as _j
                            _j.dump({"processed": done_n[0], "total": total,
                                     "saved_at": datetime.now().isoformat(),
                                     "complete": True}, _f)
                        prog.progress(1.0, text="✅ Search Complete!")
                        st.success(
                            f"✅ **{done_n[0]:,}** rows in **{el:.1f}s** "
                            f"({done_n[0]/el:.1f} rows/s)"
                        )
                        # Trigger periodic gc to release PyMuPDF memory
                        gc.collect()
                    else:
                        st.warning("No results collected.")

        except Exception as e:
            import traceback
            st.error(f"❌ {e}")
            st.session_state.log_lines.append(f"[ERROR] {traceback.format_exc()}")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 2 — RESULTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_results:
    df = st.session_state.results_df
    if df is None:
        st.info("🔍 Run a search to see results here.")
    else:
        for c in _OUT_COLS:
            if c not in df.columns: df[c] = ""

        t_r  = len(df)
        f_r  = (df["Keyword_Search_Status"] == S.FOUND).sum()
        nf_r = (df["Keyword_Search_Status"] == S.NOT_FOUND).sum()
        sc_r = (df["Keyword_Search_Status"] == S.SCANNED).sum()
        co_r = (df["Keyword_Search_Status"] == S.CORRUPTED).sum()
        fa_r = (df["Keyword_Search_Status"] == S.FAILED).sum()

        st.markdown("### 📊 Summary")
        _stats_row(t_r, f_r, nf_r, sc_r, co_r, fa_r)
        st.markdown("---")

        fc1, fc2 = st.columns(2)
        with fc1:
            s_opts = sorted(df["Keyword_Search_Status"].dropna().unique().tolist())
            s_filt = st.multiselect("Filter by Status", s_opts, default=s_opts)
        with fc2:
            kw_filt = st.text_input("Filter by Keyword", "")

        fdf = df[df["Keyword_Search_Status"].isin(s_filt)]
        if kw_filt:
            fdf = fdf[fdf["Keyword"].astype(str).str.contains(
                kw_filt, case=False, na=False)]

        st.markdown(f"**{len(fdf):,} of {t_r:,} rows**")

        disp = fdf[_OUT_COLS].copy()
        if len(disp) <= 5000:
            sfn = getattr(disp.style, "map", None) or disp.style.applymap
            st.dataframe(
                sfn(_status_badge, subset=["Keyword_Search_Status"]),
                use_container_width=True, height=460,
            )
        else:
            st.dataframe(disp, use_container_width=True, height=460)

        st.markdown("---")
        st.markdown("### ⬇️ Download")
        _ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
        _pex = (output_format == "Excel (.xlsx)")
        dc1, dc2 = st.columns(2)
        with dc1:
            try:
                st.download_button(
                    "📥 Excel (.xlsx) — 6 sheets",
                    data=_to_excel(fdf[_OUT_COLS]),
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
                data=_to_csv(fdf[_OUT_COLS]),
                file_name=f"keyword_search_{_ts}.csv",
                mime="text/csv",
                use_container_width=True,
                type="primary" if not _pex else "secondary",
            )

        st.markdown("---")
        st.markdown("### 📈 Distribution")
        cht = df["Keyword_Search_Status"].value_counts().reset_index()
        cht.columns = ["Status", "Count"]
        st.bar_chart(cht.set_index("Status"))

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 3 — LOGS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_logs:
    st.markdown("### 📜 Execution Log")
    ll = st.session_state.log_lines
    el = st.session_state.error_log

    if not ll:
        st.info("No logs yet. Run a search first.")
    else:
        lc1, lc2 = st.columns([3, 1])
        with lc1: st.markdown(f"**{len(ll):,} entries**")
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
        st.success("✅ No issues recorded." if ll else "Run a search first.")
    else:
        edf = pd.DataFrame(el)
        bd  = edf["Status"].value_counts().reset_index()
        bd.columns = ["Status", "Count"]
        ec1, ec2 = st.columns([1, 2])
        with ec1:
            st.markdown(f"**{len(el):,} issues**")
            for _, r in bd.iterrows():
                st.markdown(f"- **{r['Status']}**: {r['Count']:,}")
        with ec2:
            st.bar_chart(bd.set_index("Status"))
        with st.expander("📋 Detail", expanded=False):
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
| 2 | Download the **Template** from the sidebar |
| 3 | Fill in `URL` and `Keyword` columns |
| 4 | Upload the file on the **Search** tab |
| 5 | Review the **Pre-Processing Summary** |
| 6 | Click **🚀 Start Search** |
| 7 | Watch live progress — Pause or Stop any time |
| 8 | Download results from the **Results** tab |

> If the page refreshes or your connection drops, use **Recovery** in the sidebar.

---

### 🔘 Search Modes

| Mode | Keyword Format | Logic |
|------|---------------|-------|
| **Single Search** | `keyword` | One keyword per row |
| **Multi Search** | `kw1|kw2|kw3` | ANY or ALL matching |
| **Table Search** | `code1|code2` | Numeric/code values |
| **Auto Detect** | Any | App picks the right mode |

---

### 🚦 The 5 Result Statuses

| Status | Meaning |
|--------|---------|
| ✅ **{S.FOUND}** | Keyword was found in the document |
| ❌ **{S.NOT_FOUND}** | Document was read, keyword is not present |
| 🟡 **{S.SCANNED}** | Image/scanned PDF — no text layer |
| 🟣 **{S.CORRUPTED}** | File is damaged or unreadable |
| 🔺 **{S.FAILED}** | Could not download or access the document |

---

### 📤 Output Columns

| Column | Description |
|--------|-------------|
| `URL` | Original URL |
| `Keyword` | Keyword(s) as entered |
| `Search Mode` | Single / Multi / Table / Auto |
| `Keyword_Search_Status` | **Main result** — one of the 5 statuses above |
| `Match Count` | Total occurrences found in the document |
| `Snippet` | Context around the first match |
| `Matched Keywords` | Which keywords were found (Multi mode) |
| `Missing Keywords` | Which keywords were not found (Multi mode) |
| `Notes` | Extra detail for Failed rows only |

> Excel output has **6 sheets**: All Results + one sheet per status.

---

### ⚙️ Settings

| Setting | Default | Description |
|---------|---------|-------------|
| **Workers** | {DEFAULT_WORKERS} | Parallel downloads. Reduce to 4 if many failures. |
| **Timeout** | {DEFAULT_TIMEOUT}s | Max wait per URL. Raise for slow/large documents. |
| **Case-Sensitive** | OFF | OFF: `ABC` matches `abc` |
| **Retry Failed URLs** | ON | Re-tries failed downloads in a second pass |
| **Mirror Fallback** | ON | Tries an alternate server path on failure |
| **Smart Error Detection** | ON | Detects "not found" pages returned as 200 OK |

---

### ⚡ Performance Tips

- **10 workers** is the default — good for most servers
- Reduce to **4–6** if you see many Failed results
- **CSV** is faster to export than Excel for large result sets
- Duplicate URLs (same URL, different keywords) are downloaded **only once**
- Check the **Logs tab** for a full breakdown after a run
""")
