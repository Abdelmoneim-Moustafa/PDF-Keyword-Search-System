"""
PDF Keyword Search System — v5.0 (Fast / Light Mode)
Exact 4-status output matching the original system format.
"""

import streamlit as st
import pandas as pd
import io, os, re, time, random, threading, tempfile
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from html.parser import HTMLParser

import fitz
import urllib3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ═══════════════════════════════════════════════════════════════════
# PAGE CONFIG + LIGHT CSS
# ═══════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="PDF Keyword Search",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
/* ── Light base ── */
body, .main, [data-testid="stAppViewContainer"] {
    background:#f8f9fc !important; color:#1a1d23 !important;
}
section[data-testid="stSidebar"] {
    background:#ffffff !important;
    border-right:1px solid #e2e6ef !important;
}
/* ── Header ── */
.hdr {
    background:#ffffff;
    border:1px solid #d0d7e8; border-radius:12px;
    padding:18px 28px; margin-bottom:18px;
    display:flex; align-items:center; justify-content:space-between;
    box-shadow:0 2px 8px rgba(0,0,0,.06);
}
.hdr h1 { font-size:1.65rem; font-weight:800; color:#1a73e8; margin:0; }
.hdr p  { font-size:0.82rem; color:#6b7a99; margin:3px 0 0; }
.badge  { font-size:0.76rem; font-weight:700; padding:4px 12px;
          border-radius:16px; letter-spacing:.3px; }
.b-ready   { background:#e8f5e9; color:#2e7d32; border:1px solid #81c784; }
.b-running { background:#fff8e1; color:#f57f17; border:1px solid #ffd54f; }
.b-done    { background:#e3f2fd; color:#1565c0; border:1px solid #64b5f6; }
/* ── Stat cards ── */
.sc { background:#fff; border:1px solid #e2e6ef; border-radius:10px;
      padding:14px 16px; text-align:center;
      box-shadow:0 1px 4px rgba(0,0,0,.05); }
.sc-n { font-size:1.75rem; font-weight:800; line-height:1; }
.sc-l { font-size:0.68rem; color:#6b7a99; margin-top:4px;
        text-transform:uppercase; letter-spacing:.5px; }
/* ── Log box ── */
.logbox {
    background:#f4f6fb; border:1px solid #d8dce8; border-radius:8px;
    padding:12px 16px; font-family:"Courier New",monospace; font-size:0.76rem;
    color:#3a4460; max-height:240px; overflow-y:auto; line-height:1.6;
}
/* ── Limit warning ── */
.lim { background:linear-gradient(90deg,#e53935,#f44336);
       color:#fff; border-radius:8px; padding:8px 14px;
       font-weight:700; font-size:.9rem; text-align:center; margin:6px 0; }
/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    gap:4px; border-bottom:2px solid #d0d7e8; }
.stTabs [data-baseweb="tab"] {
    background:#f0f3fb; border-radius:8px 8px 0 0;
    color:#6b7a99; font-weight:600; padding:7px 16px;
    border:1px solid #d0d7e8; border-bottom:none; }
.stTabs [aria-selected="true"] {
    background:#fff !important; color:#1a73e8 !important;
    border-color:#1a73e8 !important; }
/* ── Buttons ── */
.stButton > button { border-radius:7px; font-weight:700; transition:all .12s; }
.stButton > button[kind="primary"] {
    background:#1a73e8 !important; color:#fff !important; border:none !important; }
.stButton > button:hover { transform:translateY(-1px); box-shadow:0 3px 8px rgba(0,0,0,.12); }
/* ── Divider ── */
hr.d { border:none; border-top:1px solid #e2e6ef; margin:12px 0; }
/* ── Status badge colors in table ── */
.found-badge     { color:#2e7d32; font-weight:700; }
.notfound-badge  { color:#c62828; }
.scanned-badge   { color:#e65100; }
.corrupt-badge   { color:#6a1b9a; }
/* ── Sidebar labels ── */
.stRadio label, .stCheckbox label, .stSlider label {
    color:#1a1d23 !important; font-weight:500; }
/* ── Input widget text ── */
.stTextInput input, .stNumberInput input {
    background:#fff !important; color:#1a1d23 !important;
    border:1px solid #c8d0e0 !important; border-radius:7px; }
</style>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════
# SECTION 1 — Constants & exact 4-status model
# ═══════════════════════════════════════════════════════════════════
SEARCH_LIMIT    = 50_000
DEFAULT_WORKERS = 8       # higher default for speed
DEFAULT_TIMEOUT = 15      # tighter default — faster fail
_CONNECT_TO     = 10      # TCP connect timeout
_MIN_DELAY      = 0.15    # lighter rate limit for speed
_BLOCK_N        = 6
_BLOCK_SLEEP    = 25

# ── Exact 4 output status values (match original system) ──────────
ST_FOUND     = "Found"
ST_CORRUPT   = "PDF Not mirrored / Corrupted"
ST_SCANNED   = "PDF is Non searchable,Advanced Scanned Extraction can make the PDF searchable."
ST_NOT_FOUND = "Failed to get PDF text /  Not Found"   # covers network errors + not found

# Statuses that should trigger retry pass
_RETRY_SET = {ST_NOT_FOUND}   # only retry actual failures, not logical not-found

# ── Output columns — exact match to original system ───────────────
_OUT_COLS = [
    "URL", "Extraction Option", "Keyword",
    "URL_Status", "URL_Search_Status",
    "Keyword_Status", "feature_name",
    "feature_value", "Keyword_Search_Status",
]

# ── Autosave paths ─────────────────────────────────────────────────
_TMP          = tempfile.gettempdir()
_AUTOSAVE_CSV = os.path.join(_TMP, "pdf_search_v5_autosave.csv")

# ── Illegal xlsx chars ─────────────────────────────────────────────
_ILLEGAL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")

# ── HTML "not found" phrases ───────────────────────────────────────
_NFP = [
    "page not found","404 not found","404 error","file not found",
    "resource not found","does not exist","error 404","no results found",
    "page unavailable","could not be found","not available",
]

# ═══════════════════════════════════════════════════════════════════
# SECTION 2 — Network layer (fast + resilient)
# ═══════════════════════════════════════════════════════════════════
_tl = threading.local()
_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Accept":          "text/html,application/pdf,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "Referer":         "https://source.z2data.com/",
}

_rate_lk  = threading.Lock()
_last_req: dict[str, float] = {}
_blk_lk   = threading.Lock()
_cfail:    dict[str, int]   = {}
_blk_til:  dict[str, float] = {}
_cache_lk  = threading.Lock()
_txt_cache: dict[str, tuple[str, str]] = {}   # url → (text, status)


def _make_sess() -> requests.Session:
    s = requests.Session()
    s.headers.update(_HEADERS)
    s.verify = False
    a = HTTPAdapter(max_retries=Retry(total=0, raise_on_status=False),
                    pool_connections=25, pool_maxsize=25)
    s.mount("https://", a); s.mount("http://", a)
    return s

def _sess(fresh=False):
    if fresh or not hasattr(_tl, "s"): _tl.s = _make_sess()
    return _tl.s

def _h(url):
    try: return url.split("/")[2]
    except: return url

def _rl(host):
    with _rate_lk:
        gap = time.time() - _last_req.get(host, 0)
        if gap < _MIN_DELAY: time.sleep(_MIN_DELAY - gap)
        _last_req[host] = time.time()

def _fail(host):
    with _blk_lk:
        _cfail[host] = _cfail.get(host, 0) + 1
        if _cfail[host] >= _BLOCK_N:
            _blk_til[host] = time.time() + _BLOCK_SLEEP
            _cfail[host] = 0

def _ok(host):
    with _blk_lk: _cfail[host] = 0

def _wait_blk(host):
    w = _blk_til.get(host, 0) - time.time()
    if w > 0: time.sleep(w)

def _clear_state():
    with _cache_lk:  _txt_cache.clear()
    with _blk_lk:    _cfail.clear(); _blk_til.clear()
    with _rate_lk:   _last_req.clear()

def _alts(url):
    a = []
    if "//source1.z2data.com" in url:
        a.append(url.replace("//source1.z2data.com","//source.z2data.com",1))
    elif "//source.z2data.com" in url:
        a.append(url.replace("//source.z2data.com","//source1.z2data.com",1))
    if "/web/" in url:
        s = url.replace("/web/","/",1); a.append(s)
        if "//source1.z2data.com" in s:
            a.append(s.replace("//source1.z2data.com","//source.z2data.com",1))
        elif "//source.z2data.com" in s:
            a.append(s.replace("//source.z2data.com","//source1.z2data.com",1))
    return a

# ═══════════════════════════════════════════════════════════════════
# SECTION 3 — Download (3 attempts, fast fail on permanent errors)
# ═══════════════════════════════════════════════════════════════════
def _dl1(url, timeout, fresh=False):
    host = _h(url)
    _wait_blk(host); _rl(host)
    try:
        r = _sess(fresh).get(url, timeout=(_CONNECT_TO, timeout),
                             stream=False, allow_redirects=True)
        if r.status_code == 200:
            _ok(host); return r.content, "ok"
        _fail(host)
        if r.status_code in (404, 410): return None, "404"
        if r.status_code in (403, 429):
            if r.status_code == 429: time.sleep(4 + random.random() * 4)
            return None, "blocked"
        return None, f"http_{r.status_code}"
    except Exception as e:
        _fail(host)
        s = str(e).lower()
        if any(w in s for w in ("ssl","cert","tls")): return None, "ssl"
        if any(w in s for w in ("timeout","timed")): return None, "timeout"
        return None, "conn"

def _fetch(url, timeout, use_mirror=True):
    urls = [url] + (_alts(url) if use_mirror else [])
    conn_err = False
    for try_url in urls:
        for attempt in range(1, 4):
            c, cat = _dl1(try_url, timeout, fresh=(conn_err and attempt == 1))
            conn_err = False
            if c is not None:
                # Validate content (not just size)
                if len(c) < 32: break
                sig = c[:64].lstrip()
                if len(c) < 64 and not sig.startswith(b"%PDF") and \
                   not any(t in sig.lower() for t in (b"<html",b"<!doc",b"<head")):
                    break
                return c, "ok"
            if cat == "404":     return None, "404"
            if cat in ("blocked","ssl"): break   # no point retrying
            if cat in ("timeout","conn"): conn_err = True
            if attempt < 3:
                time.sleep((2**attempt) * (0.5 + 0.5*random.random()))
    return None, "fail"

# ═══════════════════════════════════════════════════════════════════
# SECTION 4 — Text extraction
# ═══════════════════════════════════════════════════════════════════
def _pdf(data):
    try:
        doc = fitz.open(stream=data, filetype="pdf")
        pages = [p.get_text("text") for p in doc]; doc.close()
        t = "\n".join(pages)
        return (t,"ok") if t.strip() else ("","scanned")
    except: return "","err"

class _HS(HTMLParser):
    def __init__(self): super().__init__(); self.p=[]; self._s=False
    def handle_starttag(self,t,a):
        if t in ("script","style","noscript","head"): self._s=True
    def handle_endtag(self,t):
        if t in ("script","style","noscript","head"): self._s=False
    def handle_data(self,d):
        if not self._s:
            s=d.strip()
            if s: self.p.append(s)

def _html(data):
    try:
        h=None
        for enc in ("utf-8","latin-1","cp1252"):
            try: h=data.decode(enc); break
            except: pass
        if not h: h=data.decode("utf-8",errors="replace")
        p=_HS(); p.feed(h)
        t="\n".join(p.p)
        return (t,"ok") if t.strip() else ("","scanned")
    except: return "","err"

def _get_text(url, content, is_html):
    """URL-level cache — same PDF never extracted twice."""
    with _cache_lk:
        if url in _txt_cache: return _txt_cache[url]
    if is_html:
        t,s = _html(content)
        if not t and s != "err": t,s = _pdf(content)
    else:
        t,s = _pdf(content)
        if s == "err": t,s = _html(content)
    with _cache_lk: _txt_cache[url] = (t,s)
    return t, s

# ═══════════════════════════════════════════════════════════════════
# SECTION 5 — Text normalization + line-based keyword search
# ═══════════════════════════════════════════════════════════════════
def _norm(text):
    text = _ILLEGAL.sub(" ", text)
    text = text.replace("\u00ad","").replace("\u00a0"," ")
    text = re.sub(r"-\s*\n\s*","",text)
    return text

def _is_nf_page(text):
    if not text: return False
    return any(p in text[:1000].lower() for p in _NFP)

def _search_lines(text, keyword, case_sensitive):
    """
    Return all LINES from the text that contain the keyword.
    Each line becomes one output row (matches original system format).
    Returns list of matching lines (stripped, illegal chars removed).
    """
    if not text or not keyword: return []
    flags = 0 if case_sensitive else re.IGNORECASE
    try: pat = re.compile(re.escape(str(keyword).strip()), flags)
    except re.error: return []

    results, seen = [], set()
    for line in text.splitlines():
        clean = _ILLEGAL.sub("", line).strip()
        if not clean: continue
        if pat.search(clean):
            if clean not in seen:
                seen.add(clean)
                results.append(clean)
    return results

# ═══════════════════════════════════════════════════════════════════
# SECTION 6 — Main per-URL processor
# Returns LIST of dicts (one per matching line, or one for non-Found)
# ═══════════════════════════════════════════════════════════════════
def process_url(url, keyword, case_sensitive, timeout,
                use_mirror=True, use_smart=True, row_id=0):
    """
    Returns a list of result dicts.
    - Found: one dict per unique matching line in the PDF
    - All others: exactly one dict
    """
    url = str(url).strip()
    kw  = str(keyword).strip()

    def _row(ks, fv=None, url_status=4, url_ss="", kw_status=None):
        return {
            "_row_id":            row_id,
            "URL":                url,
            "Extraction Option":  "Entire row",
            "Keyword":            kw,
            "URL_Status":         url_status,
            "URL_Search_Status":  url_ss,
            "Keyword_Status":     kw_status,
            "feature_name":       kw,
            "feature_value":      fv,
            "Keyword_Search_Status": ks,
        }

    if not url or not url.startswith("http"):
        return [_row(ST_NOT_FOUND)]

    is_html = url.lower().split("?")[0].endswith((".html",".htm"))

    content, dl_cat = _fetch(url, timeout, use_mirror=use_mirror)

    if content is None:
        return [_row(ST_NOT_FOUND)]

    text, ext_status = _get_text(url, content, is_html)

    if ext_status in ("err",""):
        return [_row(ST_CORRUPT, url_ss=ST_CORRUPT)]

    if ext_status == "scanned":
        return [_row(ST_SCANNED, url_ss=ST_SCANNED)]

    norm_text = _norm(text)

    if use_smart and _is_nf_page(norm_text):
        return [_row(ST_NOT_FOUND, url_status=3, url_ss="Done", kw_status=3)]

    # ── Keyword search — return one row per matching line ──────────
    matching_lines = _search_lines(norm_text, kw, case_sensitive)

    if matching_lines:
        return [
            _row(ST_FOUND, fv=line, url_status=3,
                 url_ss="Done", kw_status=3)
            for line in matching_lines
        ]
    else:
        return [_row(ST_NOT_FOUND, url_status=3, url_ss="Done", kw_status=3)]

# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — Output DataFrame + Export
# ═══════════════════════════════════════════════════════════════════
def _clean(v):
    return _ILLEGAL.sub("",v) if isinstance(v,str) else v

def _build_df(dicts):
    df = pd.DataFrame(dicts)
    for c in _OUT_COLS:
        if c not in df.columns: df[c] = None
    # Drop internal column
    return df[_OUT_COLS].copy()

def _to_excel(df):
    fn = getattr(df,"map",None) or df.applymap
    buf = io.BytesIO()
    clean = fn(_clean)
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        clean.to_excel(w, index=False, sheet_name="Results")
        f = clean[clean["Keyword_Search_Status"]==ST_FOUND]
        if not f.empty: f.to_excel(w, index=False, sheet_name="Found")
        nf = clean[clean["Keyword_Search_Status"]==ST_NOT_FOUND]
        if not nf.empty: nf.to_excel(w, index=False, sheet_name="Not Found")
        sc = clean[clean["Keyword_Search_Status"]==ST_SCANNED]
        if not sc.empty: sc.to_excel(w, index=False, sheet_name="Scanned")
        co = clean[clean["Keyword_Search_Status"]==ST_CORRUPT]
        if not co.empty: co.to_excel(w, index=False, sheet_name="Corrupted")
    return buf.getvalue()

def _to_csv(df):
    return df.to_csv(index=False).encode("utf-8-sig")

def _badge(v):
    v = str(v)
    if v == ST_FOUND:     return "background-color:#e8f5e9;color:#2e7d32;font-weight:700"
    if v == ST_SCANNED:   return "background-color:#fff8e1;color:#e65100"
    if v == ST_CORRUPT:   return "background-color:#f3e5f5;color:#6a1b9a"
    if v == ST_NOT_FOUND: return "background-color:#ffebee;color:#c62828"
    return ""

# ═══════════════════════════════════════════════════════════════════
# SECTION 8 — Autosave / Recovery
# ═══════════════════════════════════════════════════════════════════
def _save(dicts, done, total):
    try:
        if dicts: _build_df(dicts).to_csv(_AUTOSAVE_CSV, index=False)
    except: pass

def _load_save():
    try:
        if os.path.exists(_AUTOSAVE_CSV) and os.path.getsize(_AUTOSAVE_CSV)>0:
            return pd.read_csv(_AUTOSAVE_CSV, dtype={"Keyword":str,"URL":str})
    except: pass
    return None

def _clear_save():
    try: os.remove(_AUTOSAVE_CSV)
    except: pass

# ═══════════════════════════════════════════════════════════════════
# SECTION 9 — Template
# ═══════════════════════════════════════════════════════════════════
def _template():
    buf = io.BytesIO()
    pd.DataFrame({
        "URL":     ["https://source.z2data.com/example.pdf",
                    "https://source.z2data.com/example2.pdf"],
        "Keyword": ["51712160148","4015080000000"],
    }).to_excel(buf, index=False, sheet_name="Template")
    buf.seek(0); return buf.getvalue()

# ═══════════════════════════════════════════════════════════════════
# SECTION 10 — Session state
# ═══════════════════════════════════════════════════════════════════
for _k,_v in [("res_df",None),("running",False),
               ("paused",False),("logs",[])]:
    if _k not in st.session_state: st.session_state[_k]=_v

# ═══════════════════════════════════════════════════════════════════
# SECTION 11 — UI Helpers
# ═══════════════════════════════════════════════════════════════════
def _hdr():
    if st.session_state.running:
        b='<span class="badge b-running">⏳ Running</span>'
    elif st.session_state.res_df is not None:
        b='<span class="badge b-done">✅ Complete</span>'
    else:
        b='<span class="badge b-ready">🟢 Ready</span>'
    st.markdown(f"""
    <div class="hdr">
      <div>
        <h1>🔍 PDF Keyword Search</h1>
        <p>Fast • Clean • 4-Status Output</p>
      </div>
      {b}
    </div>""", unsafe_allow_html=True)

def _sc(col, n, color, label):
    with col:
        st.markdown(f"""
        <div class="sc">
          <div class="sc-n" style="color:{color}">{n:,}</div>
          <div class="sc-l">{label}</div>
        </div>""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════
# SECTION 12 — SIDEBAR
# ═══════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("## ⚙️ Settings")
    st.markdown(f'<div class="lim">⚠️ Limit: {SEARCH_LIMIT:,}</div>',
                unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("### 🚀 Performance")
    workers = st.slider("Workers", 2, 20, DEFAULT_WORKERS, 1,
        help="Parallel downloads. 8–12 is fast. Reduce to 4 if you see many errors.")
    timeout = st.slider("Timeout (sec)", 5, 60, DEFAULT_TIMEOUT, 5,
        help="Max wait per URL. 15s is fast; raise to 30s for slow servers.")

    st.markdown("---")
    st.markdown("### 🔍 Search Options")
    case_sensitive = st.checkbox("Case-Sensitive", value=False)

    st.markdown("---")
    st.markdown("### 🛡 Options")
    enable_retry  = st.checkbox("Retry Failed URLs",       value=True)
    enable_mirror = st.checkbox("Mirror Fallback",         value=True)
    enable_smart  = st.checkbox("Smart Error Detection",   value=True)
    out_fmt       = st.radio("Output Format",
                             ["Excel (.xlsx)","CSV (.csv)"], index=0)

    st.markdown("---")
    st.markdown("### 📋 Template")
    st.download_button("⬇️ Download Template",
        data=_template(), file_name="PDF_Search_Template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True)

    st.markdown("---")
    st.markdown("### 📊 Status Values")
    st.markdown(f"""
- 🟢 **Found**
- 🔴 **Failed to get PDF text / Not Found**
- 🟡 **PDF is Non searchable…**
- 🟣 **PDF Not mirrored / Corrupted**
""")

    st.markdown("---")
    st.markdown("### 💾 Recovery")
    _sv = _load_save()
    if _sv is not None:
        _n = len(_sv)
        st.success(f"📂 **{_n:,} rows** saved")
        _ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        st.download_button(f"📥 CSV ({_n:,})",
            data=_sv.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"autosave_{_ts}.csv", mime="text/csv",
            use_container_width=True, key="sb_csv")
        c1,c2=st.columns(2)
        with c1:
            if st.button("♻️ Restore",use_container_width=True,key="rst"):
                st.session_state.res_df=_sv; st.success("✅")
        with c2:
            if st.button("🗑 Clear",use_container_width=True,key="clr"):
                _clear_save(); st.rerun()
    else:
        st.caption("Auto-saved every 100 rows.")

# ═══════════════════════════════════════════════════════════════════
# SECTION 13 — MAIN
# ═══════════════════════════════════════════════════════════════════
_hdr()

t_search, t_results, t_logs = st.tabs(["🔍 Search","📊 Results","📜 Logs"])

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 1 — SEARCH
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with t_search:
    cu, cf = st.columns([2,1])
    with cu:
        st.markdown("### 📁 Upload File")
        st.markdown("Excel or CSV with **`URL`** and **`Keyword`** columns.")
        uploaded = st.file_uploader("",type=["xlsx","xls","csv"],
                                    label_visibility="collapsed")
    with cf:
        st.markdown("### 📌 Format")
        st.markdown("""
| Column | Example |
|--------|---------|
| `URL` | `https://…/file.pdf` |
| `Keyword` | `51712160148` |
""")

    if uploaded:
        try:
            if uploaded.name.endswith(".csv"):
                idf = pd.read_csv(uploaded, dtype={"Keyword":str})
            else:
                idf = pd.read_excel(uploaded, dtype={"Keyword":str})

            idf.columns = [c.strip() for c in idf.columns]
            if "URL" not in idf.columns and "Offline" in idf.columns:
                idf.rename(columns={"Offline":"URL"}, inplace=True)

            miss = [c for c in ["URL","Keyword"] if c not in idf.columns]
            if miss:
                st.error(f"❌ Missing columns: {miss}")
            else:
                idf = idf.dropna(subset=["URL"]).reset_index(drop=True)
                if len(idf) > SEARCH_LIMIT:
                    st.warning(f"⚠️ Capped at {SEARCH_LIMIT:,} rows.")
                    idf = idf.head(SEARCH_LIMIT)

                total_rows  = len(idf)
                unique_urls = idf["URL"].nunique()
                dup         = total_rows - unique_urls

                # Assign stable row IDs
                idf = idf.copy()
                idf["_row_id"] = range(len(idf))

                col_a, col_b, col_c = st.columns(3)
                col_a.metric("Total Rows",    f"{total_rows:,}")
                col_b.metric("Unique URLs",   f"{unique_urls:,}")
                col_c.metric("Duplicate URLs (cached)", f"{dup:,}")

                with st.expander("🔎 Preview (first 10 rows)", expanded=False):
                    st.dataframe(idf.drop("_row_id",axis=1).head(10),
                                 use_container_width=True)

                st.markdown("<hr class='d'>", unsafe_allow_html=True)

                b1,b2,b3,_ = st.columns([2,1,1,1])
                with b1:
                    start = st.button("🚀 Start Search",
                                      use_container_width=True, type="primary",
                                      disabled=st.session_state.running)
                with b2:
                    if st.button("⏸ Pause/Resume",use_container_width=True,
                                 disabled=not st.session_state.running):
                        st.session_state.paused = not st.session_state.paused
                with b3:
                    if st.button("⏹ Stop",use_container_width=True,
                                 disabled=not st.session_state.running):
                        st.session_state.running=False
                        st.session_state.paused=False

                if start and not st.session_state.running:
                    st.session_state.running = True
                    st.session_state.paused  = False
                    st.session_state.res_df  = None
                    st.session_state.logs    = []
                    _clear_state()

                    rows    = idf.to_dict("records")
                    total   = len(rows)
                    prog    = st.progress(0,"Starting…")
                    mtrs    = st.empty()
                    curl    = st.empty()
                    lbox    = st.empty()
                    results: list[dict] = []
                    done_n:  list[int]  = [0]
                    start_t = time.time()

                    def _log(msg):
                        ts = datetime.now().strftime("%H:%M:%S")
                        st.session_state.logs.append(f"[{ts}] {msg}")
                        if len(st.session_state.logs) > 500:
                            st.session_state.logs.pop(0)

                    def _persist(d):
                        if d:
                            st.session_state.res_df = _build_df(d)
                            _save(d, done_n[0], total)

                    def _needs_retry(r):
                        return r.get("Keyword_Search_Status") == ST_NOT_FOUND \
                               and r.get("URL_Status") != 3

                    def _run(work, label):
                        p_res:  list[dict] = []
                        p_err:  list[dict] = []

                        with ThreadPoolExecutor(max_workers=workers) as ex:
                            fmap = {}
                            for r in work:
                                while st.session_state.paused \
                                      and st.session_state.running:
                                    time.sleep(0.4)
                                if not st.session_state.running: break
                                f = ex.submit(
                                    process_url,
                                    str(r.get("URL","")),
                                    str(r.get("Keyword","")),
                                    case_sensitive, timeout,
                                    enable_mirror, enable_smart,
                                    int(r.get("_row_id",0)),
                                )
                                fmap[f] = r

                            for future in as_completed(fmap):
                                if not st.session_state.running:
                                    _log("⏹ Stopped.")
                                    ex.shutdown(wait=False,cancel_futures=True)
                                    break
                                try:
                                    res_list = future.result()
                                except Exception as exc:
                                    src = fmap[future]
                                    res_list = [{
                                        "_row_id": int(src.get("_row_id",0)),
                                        "URL": str(src.get("URL","")),
                                        "Extraction Option": "Entire row",
                                        "Keyword": str(src.get("Keyword","")),
                                        "URL_Status": 4,
                                        "URL_Search_Status": "",
                                        "Keyword_Status": None,
                                        "feature_name": str(src.get("Keyword","")),
                                        "feature_value": None,
                                        "Keyword_Search_Status": ST_NOT_FOUND,
                                    }]

                                done_n[0] += 1
                                r0  = res_list[0]
                                ks  = r0["Keyword_Search_Status"]
                                icon = ("✅" if ks==ST_FOUND else
                                        "🟡" if ks==ST_SCANNED else
                                        "🟣" if ks==ST_CORRUPT else "❌")
                                u = r0["URL"][-50:]
                                _log(f"[{label}][{done_n[0]}/{total}] {icon} {ks[:28]:28s} …{u}")

                                p_res.extend(res_list)
                                # Only retry actual network failures (URL_Status=4)
                                if r0.get("URL_Status") == 4:
                                    p_err.append(fmap[future])

                                if done_n[0] % 100 == 0:
                                    _persist(results + p_res)
                                    _log(f"💾 Saved {done_n[0]:,} rows")

                                _n = max(1, min(20, total//50))
                                if done_n[0] % _n == 0 or done_n[0] == total:
                                    pct  = min(done_n[0]/total, 1.0)
                                    el   = time.time()-start_t
                                    rate = done_n[0]/el if el else 0
                                    eta  = (total-done_n[0])/rate if rate else 0
                                    found_so_far = sum(
                                        1 for r in p_res
                                        if r.get("Keyword_Search_Status")==ST_FOUND
                                    )
                                    prog.progress(pct,
                                        text=f"[{label}] {done_n[0]:,}/{total:,} "
                                             f"• {rate:.1f} URLs/s • ETA {eta:.0f}s")
                                    mtrs.markdown(
                                        f"⏱ **{el:.0f}s** &nbsp;|&nbsp; "
                                        f"⚡ **{rate:.1f}** URLs/s &nbsp;|&nbsp; "
                                        f"✅ **{found_so_far:,}** found &nbsp;|&nbsp; "
                                        f"📊 **{done_n[0]:,}/{total:,}**"
                                    )
                                    curl.markdown(f"`…{u}`")
                                    lbox.markdown(
                                        '<div class="logbox">' +
                                        "<br>".join(st.session_state.logs[-35:]) +
                                        "</div>", unsafe_allow_html=True)

                        return p_res, p_err

                    # ── Pass 1 ─────────────────────────────────────
                    _log(f"🚀 Pass 1 — {total:,} rows, {workers} workers")
                    p1, p1_err = _run(rows, "Pass 1")
                    results.extend(p1)

                    # ── Pass 2 (retry network failures only) ────────
                    if enable_retry and p1_err and st.session_state.running:
                        _log(f"♻️ Pass 2 — {len(p1_err):,} network failures…")
                        err_ids = {r.get("_row_id") for r in p1_err}
                        results = [r for r in results
                                   if r.get("_row_id") not in err_ids]
                        total  += len(p1_err)
                        _log("⏳ 12s cooldown…")
                        time.sleep(12)
                        p2, _ = _run(p1_err, "Pass 2")
                        results.extend(p2)

                    st.session_state.running = False
                    st.session_state.paused  = False

                    if results:
                        _persist(results)
                        el = time.time()-start_t
                        prog.progress(1.0, text="✅ Complete!")
                        st.success(
                            f"✅ **{done_n[0]:,}** URLs in "
                            f"**{el:.1f}s** ({done_n[0]/el:.1f} URLs/s)"
                        )
                    else:
                        st.warning("No results.")

        except Exception as e:
            st.error(f"❌ {e}")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 2 — RESULTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with t_results:
    df = st.session_state.res_df
    if df is None:
        st.info("🔍 Run a search to see results.")
    else:
        for c in _OUT_COLS:
            if c not in df.columns: df[c] = None

        t_r  = len(df)
        # Count unique URL+Keyword pairs for the stat cards
        found_pairs  = df[df["Keyword_Search_Status"]==ST_FOUND][["URL","Keyword"]].drop_duplicates()
        f_r  = len(found_pairs)
        nf_r = (df["Keyword_Search_Status"]==ST_NOT_FOUND).sum()
        sc_r = (df["Keyword_Search_Status"]==ST_SCANNED).sum()
        co_r = (df["Keyword_Search_Status"]==ST_CORRUPT).sum()

        st.markdown("### 📊 Summary")
        cols = st.columns(5)
        _sc(cols[0], t_r,  "#1a73e8", "Total Rows")
        _sc(cols[1], f_r,  "#2e7d32", "Found (Unique)")
        _sc(cols[2], nf_r, "#c62828", "Not Found")
        _sc(cols[3], sc_r, "#e65100", "Scanned")
        _sc(cols[4], co_r, "#6a1b9a", "Corrupted")

        st.markdown("---")

        fc1, fc2 = st.columns(2)
        with fc1:
            s_opts  = sorted(df["Keyword_Search_Status"].dropna().unique().tolist())
            s_filt  = st.multiselect("Filter by Status", s_opts, default=s_opts)
        with fc2:
            kw_filt = st.text_input("Filter by Keyword", "")

        fdf = df[df["Keyword_Search_Status"].isin(s_filt)]
        if kw_filt:
            fdf = fdf[fdf["Keyword"].astype(str).str.contains(
                kw_filt, case=False, na=False)]

        st.markdown(f"**{len(fdf):,} rows**")

        disp = fdf[_OUT_COLS].copy()
        if len(disp) <= 5000:
            sfn = getattr(disp.style,"map",None) or disp.style.applymap
            st.dataframe(sfn(_badge, subset=["Keyword_Search_Status"]),
                         use_container_width=True, height=460)
        else:
            st.dataframe(disp, use_container_width=True, height=460)

        st.markdown("---")
        st.markdown("### ⬇️ Download")
        _ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
        _pex = (out_fmt == "Excel (.xlsx)")
        dc1, dc2 = st.columns(2)
        with dc1:
            try:
                st.download_button("📥 Excel (.xlsx) — 5 sheets",
                    data=_to_excel(fdf[_OUT_COLS]),
                    file_name=f"keyword_search_{_ts}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    type="primary" if _pex else "secondary")
            except Exception as ex:
                st.error(f"Excel error: {ex}")
        with dc2:
            st.download_button("📥 CSV",
                data=_to_csv(fdf[_OUT_COLS]),
                file_name=f"keyword_search_{_ts}.csv",
                mime="text/csv", use_container_width=True,
                type="primary" if not _pex else "secondary")

        st.markdown("---")
        st.markdown("### 📈 Distribution")
        cht = df["Keyword_Search_Status"].value_counts().reset_index()
        cht.columns = ["Status","Count"]
        st.bar_chart(cht.set_index("Status"))

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 3 — LOGS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with t_logs:
    ll = st.session_state.logs
    if not ll:
        st.info("No logs yet.")
    else:
        lc1,lc2=st.columns([3,1])
        with lc1: st.markdown(f"**{len(ll):,} entries**")
        with lc2:
            st.download_button("📥 Export",
                data="\n".join(ll).encode("utf-8"),
                file_name=f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                mime="text/plain", use_container_width=True)
        st.markdown(
            '<div class="logbox" style="max-height:500px">'
            + "<br>".join(ll) + "</div>",
            unsafe_allow_html=True)
