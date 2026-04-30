import streamlit as st
import pandas as pd
import io
import time
import re
import fitz  # PyMuPDF
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─── Page Config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="PDF Keyword Search",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Custom CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    /* Main theme */
    .main { background-color: #0f1117; }
    
    /* Header card */
    .header-card {
        background: linear-gradient(135deg, #1a1f2e 0%, #16213e 100%);
        border: 1px solid #2d3561;
        border-radius: 12px;
        padding: 24px 32px;
        margin-bottom: 24px;
    }
    .header-title {
        font-size: 2rem;
        font-weight: 800;
        color: #00d4ff;
        margin: 0;
    }
    .header-sub {
        font-size: 0.95rem;
        color: #8892a4;
        margin-top: 4px;
    }

    /* Stat cards */
    .stat-card {
        background: #1a1f2e;
        border-radius: 10px;
        padding: 18px 20px;
        text-align: center;
        border: 1px solid #2d3561;
    }
    .stat-number { font-size: 2rem; font-weight: 800; }
    .stat-label  { font-size: 0.78rem; color: #8892a4; margin-top: 4px; text-transform: uppercase; letter-spacing: 0.5px; }

    /* Status badges */
    .badge-found     { background:#0d4c2b; color:#00e676; padding:3px 10px; border-radius:20px; font-size:0.78rem; font-weight:600; }
    .badge-notfound  { background:#4c1a0d; color:#ff6b6b; padding:3px 10px; border-radius:20px; font-size:0.78rem; font-weight:600; }
    .badge-scanned   { background:#2d2600; color:#ffd600; padding:3px 10px; border-radius:20px; font-size:0.78rem; font-weight:600; }
    .badge-error     { background:#2d0024; color:#f48fb1; padding:3px 10px; border-radius:20px; font-size:0.78rem; font-weight:600; }

    /* Progress area */
    .progress-box {
        background: #1a1f2e;
        border: 1px solid #2d3561;
        border-radius: 10px;
        padding: 20px;
        font-family: monospace;
        font-size: 0.82rem;
        color: #b0bec5;
        max-height: 220px;
        overflow-y: auto;
    }

    /* Limit warning */
    .limit-warning {
        background: linear-gradient(90deg, #ff6d00, #ff9800);
        color: white;
        border-radius: 8px;
        padding: 10px 20px;
        font-weight: 700;
        font-size: 1.1rem;
        text-align: center;
        margin: 8px 0;
    }

    /* Sidebar style */
    section[data-testid="stSidebar"] {
        background: #0f1117;
        border-right: 1px solid #2d3561;
    }

    /* Button override */
    .stButton > button {
        border-radius: 8px;
        font-weight: 700;
        transition: all 0.2s;
    }
    .stButton > button:hover { transform: translateY(-1px); }

    /* Tab style */
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        background: #1a1f2e;
        border-radius: 8px 8px 0 0;
        color: #8892a4;
        font-weight: 600;
    }
    .stTabs [aria-selected="true"] {
        background: #2d3561 !important;
        color: #00d4ff !important;
    }
</style>
""", unsafe_allow_html=True)


# ─── Constants ───────────────────────────────────────────────────────────────────
SEARCH_LIMIT = 50_000
CONCURRENT_DOWNLOADS = 6  # Lowered: fewer workers = less likely to trigger z2data rate limits
TIMEOUT_SECONDS = 20


# ─── Core Search Logic ───────────────────────────────────────────────────────────

import threading
import random

# ─── Thread-local requests Session (connection pooling per worker thread) ──────
_thread_local = threading.local()

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/pdf,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    # Referer is required — z2data.com silently drops connections without it
    "Referer": "https://source.z2data.com/",
}

# Separate connect timeout to avoid blocking workers on slow TCP handshakes
_CONNECT_TIMEOUT = 15

# ─── Global rate-limiter: enforces minimum delay between requests ─────────────
# Prevents z2data.com from triggering rate-limiting / connection bans.
# All worker threads share this lock so the delay is per-server, not per-thread.
import threading as _threading
_rate_lock = _threading.Lock()
_last_request_time: dict[str, float] = {}   # host → last request timestamp
_MIN_DELAY_SECS = 0.25   # 250 ms min between any two requests to the same host

def _rate_limit(url: str):
    """Sleep if needed so we don't hammer the same host too fast."""
    try:
        host = url.split("/")[2]
    except IndexError:
        return
    with _rate_lock:
        last = _last_request_time.get(host, 0)
        gap = time.time() - last
        if gap < _MIN_DELAY_SECS:
            time.sleep(_MIN_DELAY_SECS - gap)
        _last_request_time[host] = time.time()

# ─── Blocked-server detector ──────────────────────────────────────────────────
# Tracks consecutive failures per host. When a host fails >= threshold times
# in a row, all threads pause for a cooling-off period before retrying.
_block_lock = _threading.Lock()
_consecutive_failures: dict[str, int] = {}   # host → count
_host_blocked_until: dict[str, float] = {}   # host → unblock timestamp
_BLOCK_THRESHOLD = 5       # consecutive failures before declaring "blocked"
_BLOCK_COOLDOWN_SECS = 30  # seconds to wait when blocked is detected

def _record_failure(host: str):
    with _block_lock:
        _consecutive_failures[host] = _consecutive_failures.get(host, 0) + 1
        if _consecutive_failures[host] >= _BLOCK_THRESHOLD:
            _host_blocked_until[host] = time.time() + _BLOCK_COOLDOWN_SECS
            _consecutive_failures[host] = 0  # reset counter after triggering cooldown

def _record_success(host: str):
    with _block_lock:
        _consecutive_failures[host] = 0

def _wait_if_blocked(host: str):
    """Block the calling thread until the host's cooldown expires."""
    until = _host_blocked_until.get(host, 0)
    wait = until - time.time()
    if wait > 0:
        time.sleep(wait)


def _make_session():
    """
    Create a brand-new requests.Session.

    KEY DESIGN DECISIONS:
    ─────────────────────
    1. urllib3 Retry(total=0)  — we disable urllib3 internal retries entirely.
       When urllib3 exhausts its own counter it raises MaxRetriesError which
       our except-clause catches as ONE failure, so our app-level backoff loop
       never runs.  With total=0 every single TCP attempt surfaces immediately
       and our loop stays in full control.

    2. raise_on_status=False — HTTP 4xx/5xx arrive as a response object, not
       an exception, so we can decide per-status whether to retry or give up.

    3. pool_connections/pool_maxsize=20 — matches max worker count so workers
       never queue waiting for a socket slot.
    """
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    session = requests.Session()
    session.headers.update(_HEADERS)
    no_retry = Retry(total=0, raise_on_status=False)
    adapter = HTTPAdapter(
        max_retries=no_retry,
        pool_connections=20,
        pool_maxsize=20,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _get_session(fresh: bool = False):
    """
    Return the per-thread Session, creating a new one when requested.
    Pass fresh=True after a connection error to discard a poisoned pool.
    """
    if fresh or not hasattr(_thread_local, "session"):
        _thread_local.session = _make_session()
    return _thread_local.session


def extract_text_from_pdf_bytes(pdf_bytes: bytes) -> tuple[str, str]:
    """
    Extract text from PDF bytes.
    Returns (text, extraction_status)
    extraction_status: 'searchable' | 'scanned' | 'error:<msg>'
    """
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        full_text = []
        has_text = False
        for page in doc:
            txt = page.get_text("text")
            if txt.strip():
                has_text = True
            full_text.append(txt)
        doc.close()
        if has_text:
            return "\n".join(full_text), "searchable"
        else:
            return "", "scanned"
    except Exception as e:
        return "", f"error:{e}"


def _extract_text_from_html_bytes(html_bytes: bytes) -> tuple[str, str]:
    """
    Extract plain text from HTML bytes using html.parser (stdlib, no extra deps).
    Falls back gracefully if decoding fails.
    Returns (text, 'searchable') or ('', 'error:<msg>')
    """
    try:
        from html.parser import HTMLParser

        class _TextExtractor(HTMLParser):
            def __init__(self):
                super().__init__()
                self.parts = []
                self._skip = False

            def handle_starttag(self, tag, attrs):
                if tag in ("script", "style", "noscript"):
                    self._skip = True

            def handle_endtag(self, tag):
                if tag in ("script", "style", "noscript"):
                    self._skip = False

            def handle_data(self, data):
                if not self._skip:
                    stripped = data.strip()
                    if stripped:
                        self.parts.append(stripped)

        # Detect encoding
        for enc in ("utf-8", "latin-1", "cp1252"):
            try:
                html_str = html_bytes.decode(enc)
                break
            except Exception:
                html_str = None
        if not html_str:
            html_str = html_bytes.decode("utf-8", errors="replace")

        parser = _TextExtractor()
        parser.feed(html_str)
        text = "\n".join(parser.parts)
        return (text, "searchable") if text.strip() else ("", "scanned")
    except Exception as e:
        return "", f"error:{e}"


def search_keyword_in_text(text: str, keyword: str, case_sensitive: bool = False) -> tuple[bool, int, list[str]]:
    """
    Search keyword in text.
    Returns (found, count, matched_line_snippets)
    Duplicate lines are deduplicated while preserving order.
    """
    if not text or not keyword:
        return False, 0, []

    flags = 0 if case_sensitive else re.IGNORECASE
    pattern = re.escape(str(keyword).strip())

    try:
        matches = list(re.finditer(pattern, text, flags))
    except re.error:
        matches = []

    if not matches:
        return False, 0, []

    snippets = []
    seen = set()
    for m in matches:
        line_start = text.rfind("\n", 0, m.start())
        line_start = line_start + 1 if line_start != -1 else 0
        line_end = text.find("\n", m.end())
        line_end = line_end if line_end != -1 else len(text)
        line = text[line_start:line_end].strip()
        if line and line not in seen:
            seen.add(line)
            snippets.append(line)

    return True, len(matches), snippets


def _get_alternate_urls(url: str) -> list[str]:
    """
    Return all alternate mirror URLs to try when the primary URL fails.

    z2data.com has two known CDN hosts: source.z2data.com and source1.z2data.com.
    We try both regardless of which was primary.

    Additionally, for old /web/ paths (archived pages from 2017-2019) that often
    return connection errors, we also try stripping /web/ from the path, which
    matches the newer URL structure that the servers still serve.
    Example:
      source.z2data.com/web/2019/9/11/.../0484584530.html
      → source.z2data.com/2019/9/11/.../0484584530.html  (alternate path)
    """
    alts = []
    # Mirror swap
    if "//source1.z2data.com" in url:
        alts.append(url.replace("//source1.z2data.com", "//source.z2data.com", 1))
    elif "//source.z2data.com" in url:
        alts.append(url.replace("//source.z2data.com", "//source1.z2data.com", 1))

    # /web/ path stripping — try both hosts
    if "/web/" in url:
        stripped = url.replace("/web/", "/", 1)
        alts.append(stripped)
        # Also stripped + mirror swap
        if "//source1.z2data.com" in stripped:
            alts.append(stripped.replace("//source1.z2data.com", "//source.z2data.com", 1))
        elif "//source.z2data.com" in stripped:
            alts.append(stripped.replace("//source.z2data.com", "//source1.z2data.com", 1))

    return alts


def _download_with_retry(url: str, session_timeout: int, max_attempts: int = 4) -> tuple[bytes | None, str | None]:
    """
    Download URL with app-level exponential back-off retry.

    urllib3-internal retries are DISABLED (Retry(total=0)) so this function
    has full control over every attempt.  On a connection-level error the
    thread-local session is discarded and recreated so a poisoned connection
    pool does not carry over to the next attempt.

    Retry schedule (base 2, ±25% jitter):
      attempt 1 → immediate
      attempt 2 → wait ~2 s
      attempt 3 → wait ~4 s
      attempt 4 → wait ~8 s

    No-retry statuses: 403, 404, 410  (permanent client errors)
    Retried statuses:  429, 500, 502, 503, 504  (transient server errors)
    """
    last_err = None
    is_connection_error = False

    try:
        host = url.split("/")[2]
    except IndexError:
        host = url

    for attempt in range(1, max_attempts + 1):
        # Wait if this host has been detected as blocked/rate-limiting
        _wait_if_blocked(host)
        # Enforce minimum inter-request delay (shared across all threads)
        _rate_limit(url)

        # Refresh session if previous attempt had a connection-level error
        session = _get_session(fresh=is_connection_error)
        is_connection_error = False

        try:
            resp = session.get(
                url,
                timeout=(_CONNECT_TIMEOUT, session_timeout),  # (connect, read)
                stream=False,
                allow_redirects=True,
            )
            if resp.status_code == 200:
                _record_success(host)
                return resp.content, None

            last_err = f"HTTP {resp.status_code}"
            _record_failure(host)

            # No retry on permanent failures
            if resp.status_code in (403, 404, 410):
                break
            # 429 = rate limited: extra pause before next attempt
            if resp.status_code == 429:
                time.sleep(5 + random.random() * 5)
            # 5xx: retryable — fall through to back-off below

        except Exception as e:
            last_err = str(e)[:200]
            _record_failure(host)
            # Connection-level errors (SSL, TCP reset, MaxRetries from pool) —
            # discard the session so the next attempt opens a fresh socket.
            is_connection_error = True

        if attempt < max_attempts:
            # Exponential back-off: 2^attempt seconds ± 25% jitter
            wait = (2 ** attempt) * (0.75 + 0.5 * random.random())
            time.sleep(wait)

    return None, last_err


def _download_pdf(url: str, session_timeout: int):
    return _download_with_retry(url, session_timeout, max_attempts=4)



def _build_context_snippet(text: str, keyword: str, context_chars: int = 100) -> str:
    """
    Build a context snippet around keyword occurrences in text.

    Strategy: collect a context window around EVERY match, deduplicate them,
    then return the one with the most surrounding text (richest context).
    This prevents the feature_value from containing duplicate snippets when
    the same barcode appears multiple times in the PDF (e.g. TOC + product page).

    Format: '…<up-to context_chars chars before match>…<match>…<up-to context_chars after>…'
    """
    if not text or not keyword:
        return ""

    kw_lower = str(keyword).lower()
    kw_len = len(kw_lower)
    text_lower = text.lower()

    # Collect all match positions
    positions = []
    start = 0
    while True:
        idx = text_lower.find(kw_lower, start)
        if idx == -1:
            break
        positions.append(idx)
        start = idx + 1  # allow overlapping scan

    if not positions:
        return ""

    # Build unique context windows, keep the richest (most non-whitespace chars)
    seen_snippets: set[str] = set()
    best = ""
    for idx in positions:
        s = max(0, idx - context_chars)
        e = min(len(text), idx + kw_len + context_chars)
        raw = text[s:e].strip()
        # Sanitise control chars so the snippet is always xlsx-safe too
        raw = _ILLEGAL_CHARS_RE.sub("", raw)
        if not raw or raw in seen_snippets:
            continue
        seen_snippets.add(raw)
        prefix = "…" if s > 0 else ""
        suffix = "…" if e < len(text) else ""
        candidate = f"{prefix}{raw}{suffix}"
        # Prefer the snippet with the most surrounding context (richest information)
        if len(raw) > len(best.replace("…", "")):
            best = candidate

    return best


def process_one_url(url: str, keyword: str, case_sensitive: bool, session_timeout: int) -> list:
    """
    Download content from URL and search for keyword.

    Handles both PDF and HTML URLs:
      - .html / .htm  → extract text via HTML parser
      - everything else (or unknown) → try PDF parser, fallback to HTML parser

    Retry strategy:
      1. Try primary URL with up to 3 attempts + exponential back-off
      2. If all fail, swap mirror host (source ↔ source1) and retry 3 more times
      3. Only then report the actual error

    Always returns exactly ONE row per URL+Keyword pair.
    """
    base = {
        "URL": url,
        "Keyword": keyword,
        "Extraction_Option": "",
        "URL_Status": None,
        "URL_Search_Status": "",
        "Keyword_Status": None,
        "feature_name": keyword,
        "feature_value": None,
        "Keyword_Search_Status": "",
        "match_count": 0,
        "context": "",
    }

    def make_row(**overrides):
        r = dict(base)
        r.update(overrides)
        return r

    url = str(url).strip()
    if not url or not url.startswith("http"):
        return [make_row(URL_Status=0, URL_Search_Status="Invalid URL",
                         Keyword_Search_Status="Invalid URL")]

    # ── Determine content type from URL extension ────────────────────
    url_lower = url.lower().split("?")[0]
    is_html_url = url_lower.endswith(".html") or url_lower.endswith(".htm")

    # ── Download: primary URL first, then all mirror/path alternates ──
    #
    # Attempt order:
    #   1. Primary URL         — 4 attempts with exponential back-off
    #   2. Mirror swap         — source ↔ source1  (4 attempts)
    #   3. /web/ path stripped — same URL without /web/ segment (4 attempts)
    #   4. Stripped + mirror   — stripped URL on the other host (4 attempts)
    #
    # Each attempt uses a fresh session after any connection-level error
    # (see _download_with_retry / _get_session(fresh=True)).
    content_bytes, err = _download_with_retry(url, session_timeout, max_attempts=4)

    if content_bytes is None:
        for alt_url in _get_alternate_urls(url):
            content_bytes, err = _download_with_retry(alt_url, session_timeout, max_attempts=4)
            if content_bytes is not None:
                break

    if content_bytes is None:
        # Surface the full error message so the user knows exactly what failed
        if err and not err.startswith(("Download Error:", "HTTP ", "Timeout")):
            error_msg = f"Download Error: {err}"
        else:
            error_msg = err or "Download Error: Unknown"
        return [make_row(URL_Status=0, URL_Search_Status=error_msg,
                         Keyword_Search_Status=error_msg)]

    # ── Text extraction ───────────────────────────────────────────────
    if is_html_url:
        # HTML product pages: parse directly as HTML
        text, extraction_status = _extract_text_from_html_bytes(content_bytes)
        if not text and "error:" not in extraction_status:
            # Could be a PDF served with .html extension — try PDF parser too
            text2, status2 = extract_text_from_pdf_bytes(content_bytes)
            if text2:
                text, extraction_status = text2, status2
    else:
        # Try PDF first
        text, extraction_status = extract_text_from_pdf_bytes(content_bytes)
        if "error:" in extraction_status:
            # Could be HTML served with .pdf extension or wrong content-type
            text2, status2 = _extract_text_from_html_bytes(content_bytes)
            if text2:
                text, extraction_status = text2, status2

    if "error:" in extraction_status:
        return [make_row(URL_Status=0, URL_Search_Status="PDF Not mirrored / Corrupted",
                         Keyword_Search_Status="PDF Not mirrored / Corrupted")]

    if extraction_status == "scanned":
        msg = (
            "PDF is Non searchable,"
            "Advanced Scanned Extraction can make the PDF searchable."
        )
        return [make_row(URL_Status=3, URL_Search_Status="Done",
                         Keyword_Search_Status=msg, Keyword_Status=None)]

    # ── Keyword search ────────────────────────────────────────────────
    found, count, snippets = search_keyword_in_text(text, keyword, case_sensitive)

    if found:
        context_snippet = _build_context_snippet(text, keyword, context_chars=100)
        return [make_row(
            URL_Status=3,
            URL_Search_Status="Done",
            Keyword_Status=3.0,
            match_count=count,
            feature_value=context_snippet,
            Keyword_Search_Status="Found",
            context=context_snippet,
        )]
    else:
        return [make_row(URL_Status=3, URL_Search_Status="Done", Keyword_Status=3.0,
                         Keyword_Search_Status="Not Found")]


# ─── Streamlit App ────────────────────────────────────────────────────────────────

def render_header():
    st.markdown("""
    <div class="header-card">
        <div class="header-title">🔍 PDF Keyword Search</div>
        <div class="header-sub">Fast, concurrent keyword search across thousands of PDF URLs</div>
    </div>
    """, unsafe_allow_html=True)


def render_stat_cards(total, found, not_found, scanned, errors):
    cols = st.columns(5)
    stats = [
        (total,     "#00d4ff", "Total"),
        (found,     "#00e676", "Found"),
        (not_found, "#ff6b6b", "Not Found"),
        (scanned,   "#ffd600", "Scanned"),
        (errors,    "#f48fb1", "Errors"),
    ]
    for col, (val, color, label) in zip(cols, stats):
        with col:
            st.markdown(f"""
            <div class="stat-card">
                <div class="stat-number" style="color:{color}">{val:,}</div>
                <div class="stat-label">{label}</div>
            </div>
            """, unsafe_allow_html=True)


_ILLEGAL_CHARS_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def _sanitize_cell(val):
    """Strip control chars that openpyxl cannot write to xlsx cells.
    PDFs often embed NUL (\x00), form-feed (\x0c), vertical-tab (\x0b),
    ESC (\x1b) etc. which are valid Python strings but crash openpyxl."""
    if isinstance(val, str):
        return _ILLEGAL_CHARS_RE.sub("", val)
    return val


def df_to_excel_bytes(df: pd.DataFrame) -> bytes:
    # Sanitise every cell before handing to openpyxl
    _map_fn = getattr(df, "map", None) or df.applymap   # pandas >= 2.1 renamed applymap -> map
    clean = _map_fn(_sanitize_cell)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        clean.to_excel(writer, index=False, sheet_name="Results")
    return output.getvalue()


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def apply_status_badge(val):
    if val == "Found":
        return "background-color:#0d4c2b; color:#00e676"
    elif val == "Not Found":
        return "background-color:#4c1a0d; color:#ff6b6b"
    elif "Non searchable" in str(val):
        return "background-color:#2d2600; color:#ffd600"
    elif val in ("Timeout", "Invalid URL") or "Error" in str(val) or "HTTP" in str(val):
        return "background-color:#2d0024; color:#f48fb1"
    return ""


# ─── Session State ────────────────────────────────────────────────────────────────
if "results_df" not in st.session_state:
    st.session_state.results_df = None
if "running" not in st.session_state:
    st.session_state.running = False


# ─── Sidebar ─────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Settings")

    st.markdown(f"""
    <div class="limit-warning">⚠️ Limit: {SEARCH_LIMIT:,}</div>
    """, unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("### 🔧 Search Options")

    workers = st.slider("Concurrent Workers", 2, 20, CONCURRENT_DOWNLOADS, 1,
                        help="More workers = faster, but uses more memory")
    timeout = st.slider("Per-URL Timeout (sec)", 5, 60, TIMEOUT_SECONDS, 5)
    case_sensitive = st.checkbox("Case-Sensitive Search", value=False)
    output_format = st.radio("Output Format", ["Excel (.xlsx)", "CSV (.csv)"], index=0)

    st.markdown("---")
    st.markdown("### 📋 Template")
    template_df = pd.DataFrame({"URL": ["https://example.com/file.pdf"], "Keyword": ["your keyword"]})
    st.download_button(
        "⬇️ Download Template",
        data=df_to_excel_bytes(template_df),
        file_name="PDF_Keyword_Search_Template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

    st.markdown("---")
    st.markdown("### ℹ️ Status Guide")
    st.markdown("""
    - 🟢 **Found** — Keyword located in PDF  
    - 🔴 **Not Found** — PDF searchable, keyword absent  
    - 🟡 **Scanned** — Non-searchable / image PDF  
    - 🔵 **Error** — Download or parse failure  
    """)


# ─── Main Content ─────────────────────────────────────────────────────────────────
render_header()

tab_search, tab_results, tab_guide = st.tabs(["🔍 Search", "📊 Results", "📖 Guide"])

# ══════════════════════════════════════════════════════════════════
# TAB 1 — SEARCH
# ══════════════════════════════════════════════════════════════════
with tab_search:
    col_upload, col_info = st.columns([2, 1])

    with col_upload:
        st.markdown("### 📁 Upload Input File")
        st.markdown("Upload an Excel or CSV with **`URL`** and **`Keyword`** columns.")
        uploaded_file = st.file_uploader(
            "Drop your file here",
            type=["xlsx", "xls", "csv"],
            label_visibility="collapsed",
        )

    with col_info:
        st.markdown("### 📌 Required Columns")
        st.markdown("""
        | Column | Description |
        |--------|-------------|
        | `URL` | Direct link to PDF file |
        | `Keyword` | Term to search for |
        """)

    if uploaded_file:
        try:
            if uploaded_file.name.endswith(".csv"):
                input_df = pd.read_csv(uploaded_file, dtype={"Keyword": str})
            else:
                input_df = pd.read_excel(uploaded_file, dtype={"Keyword": str})

            # Normalize column names
            input_df.columns = [c.strip() for c in input_df.columns]
            # Accept 'Offline' as URL column (matches the uploaded sample)
            if "URL" not in input_df.columns and "Offline" in input_df.columns:
                input_df.rename(columns={"Offline": "URL"}, inplace=True)

            missing = [c for c in ["URL", "Keyword"] if c not in input_df.columns]
            if missing:
                st.error(f"❌ Missing columns: **{', '.join(missing)}**. Found: {input_df.columns.tolist()}")
            else:
                input_df = input_df.dropna(subset=["URL"]).reset_index(drop=True)
                total_rows = len(input_df)

                st.success(f"✅ File loaded — **{total_rows:,}** rows detected")

                if total_rows > SEARCH_LIMIT:
                    st.warning(f"⚠️ File has {total_rows:,} rows. Only the first **{SEARCH_LIMIT:,}** will be processed.")
                    input_df = input_df.head(SEARCH_LIMIT)

                with st.expander("🔎 Preview Input (first 10 rows)", expanded=False):
                    st.dataframe(input_df.head(10), use_container_width=True)

                st.markdown("---")
                col_btn1, col_btn2, col_btn3 = st.columns([2, 1, 1])

                with col_btn1:
                    start_btn = st.button(
                        "🚀 Start Search",
                        use_container_width=True,
                        type="primary",
                        disabled=st.session_state.running,
                    )

                with col_btn2:
                    stop_btn = st.button("⏹ Stop", use_container_width=True)

                if stop_btn:
                    st.session_state.running = False

                if start_btn and not st.session_state.running:
                    st.session_state.running = True
                    st.session_state.results_df = None

                    total = len(input_df)
                    rows = input_df.to_dict("records")

                    # ── Progress UI ──────────────────────────────────────────
                    prog_bar = st.progress(0, text="Initializing…")
                    status_text = st.empty()
                    log_area = st.empty()
                    metrics_area = st.empty()

                    results = []
                    failed_rows = []   # rows whose first pass returned an error — retried in second pass
                    completed = [0]    # mutable counter — lets _run_pass increment without nonlocal
                    log_lines = []

                    # ── helper: build output DataFrame from raw result dicts ──
                    _OUT_COLS = [
                        "URL", "Keyword", "Extraction Option",
                        "URL_Status", "URL_Search_Status",
                        "Keyword_Status", "feature_name",
                        "feature_value", "Keyword_Search_Status",
                    ]

                    def _build_df(result_dicts):
                        df_tmp = pd.DataFrame(result_dicts)
                        df_tmp.rename(columns={"Extraction_Option": "Extraction Option"}, inplace=True)
                        for c in _OUT_COLS:
                            if c not in df_tmp.columns:
                                df_tmp[c] = None
                        return df_tmp[_OUT_COLS]

                    def _save_progress(result_dicts):
                        """Persist partial results to session state so they survive a Stop."""
                        if result_dicts:
                            st.session_state.results_df = _build_df(result_dicts)

                    def _is_error(row_dict: dict) -> bool:
                        status = str(row_dict.get("Keyword_Search_Status", ""))
                        return (
                            "Download Error" in status
                            or "Timeout" in status
                            or "HTTP " in status
                            or status == "Download Error: Unknown"
                        )

                    start_time = time.time()

                    def log(msg):
                        ts = datetime.now().strftime("%H:%M:%S")
                        log_lines.append(f"[{ts}] {msg}")
                        if len(log_lines) > 80:
                            log_lines.pop(0)

                    def _run_pass(work_rows, pass_label, extra_delay=0):
                        """
                        Submit work_rows to the thread pool.
                        Returns (all_result_dicts, error_input_rows).
                        extra_delay: seconds to wait between submissions (second pass = slower).
                        completed is a list[int] so we can mutate it without nonlocal.
                        """
                        pass_results = []
                        pass_errors = []

                        with ThreadPoolExecutor(max_workers=workers) as executor:
                            future_map = {}
                            for r in work_rows:
                                if extra_delay > 0:
                                    time.sleep(extra_delay)
                                f = executor.submit(
                                    process_one_url,
                                    str(r.get("URL", "")),
                                    str(r.get("Keyword", "")),
                                    case_sensitive,
                                    timeout,
                                )
                                future_map[f] = r

                            for future in as_completed(future_map):
                                if not st.session_state.running:
                                    log("⏹ Stopped by user.")
                                    executor.shutdown(wait=False, cancel_futures=True)
                                    break

                                try:
                                    res_rows = future.result()
                                except Exception as e:
                                    res_rows = [{
                                        "URL": str(future_map[future].get("URL", "")),
                                        "Keyword": str(future_map[future].get("Keyword", "")),
                                        "Extraction_Option": "",
                                        "URL_Status": 0,
                                        "URL_Search_Status": f"Exception: {e}",
                                        "Keyword_Status": None,
                                        "feature_name": str(future_map[future].get("Keyword", "")),
                                        "feature_value": None,
                                        "Keyword_Search_Status": f"Exception: {e}",
                                        "match_count": 0,
                                        "context": "",
                                    }]

                                completed[0] += 1
                                res = res_rows[0]
                                status = res["Keyword_Search_Status"]
                                url_short = res["URL"][-50:] if len(res["URL"]) > 50 else res["URL"]
                                log(f"[{pass_label}][{completed[0]}/{total}] {status[:14]:14s} → …{url_short}")

                                pass_results.extend(res_rows)
                                if _is_error(res):
                                    pass_errors.append(future_map[future])

                                # Save progress every 100 completed rows
                                if completed[0] % 100 == 0:
                                    _save_progress(results + pass_results)
                                    log(f"💾 Progress saved — {completed[0]:,} rows")

                                # Update UI every N records
                                _n = max(1, min(20, total // 50))
                                if completed[0] % _n == 0 or completed[0] == total:
                                    pct = min(completed[0] / total, 1.0)
                                    elapsed = time.time() - start_time
                                    rate = completed[0] / elapsed if elapsed > 0 else 0
                                    eta_sec = (total - completed[0]) / rate if rate > 0 else 0
                                    prog_bar.progress(pct, text=f"[{pass_label}] {completed[0]:,}/{total:,}  •  {rate:.1f} URLs/sec  •  ETA {eta_sec:.0f}s")
                                    status_text.markdown(
                                        f"⏱ **Elapsed:** {elapsed:.1f}s  |  "
                                        f"**Speed:** {rate:.1f} URLs/s  |  "
                                        f"**Done:** {completed[0]:,}/{total:,}"
                                    )
                                    log_area.markdown(
                                        f'<div class="progress-box">' +
                                        "<br>".join(log_lines[-30:]) +
                                        "</div>",
                                        unsafe_allow_html=True,
                                    )

                        return pass_results, pass_errors

                    # ════════════════════════════════════════════
                    # PASS 1 — process all URLs
                    # ════════════════════════════════════════════
                    log(f"🚀 Pass 1 — {total:,} URLs, {workers} workers…")
                    pass1_results, pass1_errors = _run_pass(rows, "Pass1")
                    results.extend(pass1_results)

                    # ════════════════════════════════════════════
                    # PASS 2 — retry only the failed ones
                    # ════════════════════════════════════════════
                    if pass1_errors and st.session_state.running:
                        log(f"♻️  Pass 2 — retrying {len(pass1_errors):,} failed URLs (slower, more delay)…")
                        # Remove first-pass error rows from results; replace with pass-2 outcomes
                        error_keys = {(r.get("URL",""), r.get("Keyword","")) for r in pass1_errors}
                        results = [r for r in results
                                   if (r.get("URL",""), r.get("Keyword","")) not in error_keys]
                        total += len(pass1_errors)  # extend progress bar denominator

                        # Cool-down before second pass: let the server recover
                        log("⏳ Waiting 15 s before second pass…")
                        time.sleep(15)

                        pass2_results, still_failed = _run_pass(
                            pass1_errors, "Pass2",
                            extra_delay=1.0,  # 1 s stagger between submissions
                        )
                        results.extend(pass2_results)

                        if still_failed:
                            log(f"⚠️  {len(still_failed):,} URLs could not be reached after 2 passes.")
                        else:
                            log("✅ All previously failed URLs resolved in Pass 2!")

                    st.session_state.running = False

                    # Final save
                    if results:
                        _save_progress(results)
                        elapsed_total = time.time() - start_time
                        prog_bar.progress(1.0, text="✅ Search Complete!")
                        st.success(
                            f"✅ Finished **{completed[0]:,}** URLs in **{elapsed_total:.1f}s** "
                            f"({completed[0]/elapsed_total:.1f} URLs/sec)"
                        )
                    else:
                        st.warning("No results collected.")

        except Exception as e:
            st.error(f"❌ Failed to load file: {e}")


# ══════════════════════════════════════════════════════════════════
# TAB 2 — RESULTS
# ══════════════════════════════════════════════════════════════════
with tab_results:
    df = st.session_state.results_df

    if df is None:
        st.info("🔍 Run a search first to see results here.")
    else:
        # Summary stats
        total    = len(df)
        found    = (df["Keyword_Search_Status"] == "Found").sum()
        not_fnd  = (df["Keyword_Search_Status"] == "Not Found").sum()
        scanned  = df["Keyword_Search_Status"].str.contains("Non searchable", na=False).sum()
        errors   = total - found - not_fnd - scanned

        st.markdown("### 📊 Summary")
        render_stat_cards(total, found, not_fnd, scanned, errors)

        st.markdown("---")

        # Filters
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            status_filter = st.multiselect(
                "Filter by Status",
                options=df["Keyword_Search_Status"].unique().tolist(),
                default=df["Keyword_Search_Status"].unique().tolist(),
            )
        with col_f2:
            kw_filter = st.text_input("Filter by Keyword (contains)", "")

        filtered = df[df["Keyword_Search_Status"].isin(status_filter)]
        if kw_filter:
            filtered = filtered[filtered["Keyword"].astype(str).str.contains(kw_filter, case=False, na=False)]

        st.markdown(f"**Showing {len(filtered):,} rows**")

        # Styled table
        display_df = filtered[[
            "URL", "Keyword", "Extraction Option",
            "URL_Status", "URL_Search_Status",
            "Keyword_Status", "feature_name", "feature_value",
            "Keyword_Search_Status",
        ]].copy()

        if len(display_df) <= 5000:
            # pandas >= 2.1 renamed Styler.applymap → Styler.map; fall back gracefully
            _styler_fn = getattr(display_df.style, "map", None) or display_df.style.applymap
            styled = _styler_fn(apply_status_badge, subset=["Keyword_Search_Status"])
            st.dataframe(styled, use_container_width=True, height=450)
        else:
            st.dataframe(display_df, use_container_width=True, height=450)

        # Download
        st.markdown("---")
        st.markdown("### ⬇️ Download Results")
        col_dl1, col_dl2 = st.columns(2)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        with col_dl1:
            try:
                xlsx_bytes = df_to_excel_bytes(filtered)
                st.download_button(
                    "📥 Download Excel (.xlsx)",
                    data=xlsx_bytes,
                    file_name=f"keyword_search_results_{timestamp}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    type="primary",
                )
            except Exception as exc:
                st.error(f"Excel export failed: {exc}. Use CSV instead.")

        with col_dl2:
            csv_bytes = df_to_csv_bytes(filtered)
            st.download_button(
                "📥 Download CSV (.csv)",
                data=csv_bytes,
                file_name=f"keyword_search_results_{timestamp}.csv",
                mime="text/csv",
                use_container_width=True,
            )

        # Chart
        st.markdown("---")
        st.markdown("### 📈 Search Result Distribution")
        chart_data = df["Keyword_Search_Status"].value_counts().reset_index()
        chart_data.columns = ["Status", "Count"]
        st.bar_chart(chart_data.set_index("Status"))


# ══════════════════════════════════════════════════════════════════
# TAB 3 — GUIDE
# ══════════════════════════════════════════════════════════════════
with tab_guide:
    st.markdown("""
    ## 📖 User Guide

    ### How to Use

    1. **Download the Template** from the sidebar  
    2. **Fill your data** — each row needs:
       - `URL` → Direct link to a PDF file  
       - `Keyword` → The term you want to find  
    3. **Upload the file** on the Search tab  
    4. **Configure Settings** in the sidebar (workers, timeout, case)  
    5. **Click Start Search** and watch the live progress  
    6. **View results** on the Results tab and **Download** when done  

    ---

    ### Output Columns Explained

    | Column | Description |
    |--------|-------------|
    | `URL` | Original PDF URL |
    | `Keyword` | Search term used |
    | `Extraction Option` | Extraction method used |
    | `URL_Status` | HTTP/connection status code |
    | `URL_Search_Status` | "Done" if PDF was processed |
    | `Keyword_Status` | Numeric code (3.0 = processed) |
    | `feature_name` | The keyword searched |
    | `feature_value` | Matched context snippet |
    | `Keyword_Search_Status` | **Main result**: Found / Not Found / PDF Non-searchable / Error |

    ---

    ### Status Values

    | Status | Meaning |
    |--------|---------|
    | ✅ `Found` | Keyword was found in the PDF text |
    | ❌ `Not Found` | PDF is searchable but keyword was absent |
    | 🟡 `PDF is Non searchable…` | Image-based / scanned PDF (no text layer) |
    | 🔴 `PDF Not mirrored / Corrupted` | File is damaged or unreadable |
    | 🔴 `HTTP 404`, `Timeout`, etc. | Network or server errors |

    ---

    ### Performance Tips

    - Increase **Workers** for faster processing (up to 40)  
    - Reduce **Timeout** for faster failures on bad URLs  
    - Process in batches of **5,000–15,000** for best reliability  
    - The system limit is **50,000 URLs** per run  

    ---

    ### Notes

    - Only **PDF files** are supported  
    - **Scanned PDFs** (image-only) cannot be searched without OCR  
    - Search is **not case-sensitive** by default  
    - Results preserve the **same column structure** as the system output  
    """)
