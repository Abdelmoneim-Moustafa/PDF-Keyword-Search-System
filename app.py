"""
PDF Keyword Search — Streamlit Cloud Edition
Results stored as BYTES inside SQLite (works on ephemeral filesystems).
No external file storage needed. Downloads served directly from DB.
"""

import streamlit as st
import pandas as pd
import sqlite3, io, time, threading, uuid, json, asyncio, os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
import aiohttp
import fitz

# ── Config ─────────────────────────────────────────────────────────────────────
DB_PATH     = "jobs.db"          # single file, created in working dir
CONCURRENCY = 150
MAX_BYTES   = 524_288            # 512 KB range cap per PDF
TIMEOUT     = 20
RETRIES     = 2
LIMIT       = 50_000
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0",
      "Accept": "application/pdf,*/*"}

# ── DB (blobs for results) ─────────────────────────────────────────────────────
def init_db():
    c = sqlite3.connect(DB_PATH)
    c.execute("""CREATE TABLE IF NOT EXISTS jobs(
        id       TEXT PRIMARY KEY,
        name     TEXT,
        filename TEXT,
        started  TEXT,
        status   TEXT DEFAULT 'Pending',
        total    INT  DEFAULT 0,
        done     INT  DEFAULT 0,
        found    INT  DEFAULT 0,
        failed   INT  DEFAULT 0,
        xlsx_bytes BLOB,
        csv_bytes  BLOB,
        err      TEXT,
        progress TEXT DEFAULT '{}'
    )""")
    c.commit(); c.close()

def _db(): return sqlite3.connect(DB_PATH, check_same_thread=False, timeout=15)

def save_job(j):
    c = _db()
    c.execute("""INSERT OR REPLACE INTO jobs
        (id,name,filename,started,status,total,done,found,failed,err,progress)
        VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
        (j['id'], j['name'], j['filename'], j['started'], j['status'],
         j['total'], 0, 0, 0, '', '{}'))
    c.commit(); c.close()

def upd(jid, **kw):
    # separate blob fields from normal fields
    blobs = {k: v for k, v in kw.items() if k in ('xlsx_bytes','csv_bytes')}
    plain = {k: v for k, v in kw.items() if k not in ('xlsx_bytes','csv_bytes')}
    c = _db()
    if plain:
        c.execute(f"UPDATE jobs SET {','.join(k+'=?' for k in plain)} WHERE id=?",
                  [*plain.values(), jid])
    if blobs:
        for k, v in blobs.items():
            c.execute(f"UPDATE jobs SET {k}=? WHERE id=?", (v, jid))
    c.commit(); c.close()

def get_job(jid):
    c = _db()
    cur = c.execute("SELECT * FROM jobs WHERE id=?", (jid,))
    row = cur.fetchone(); cols = [d[0] for d in cur.description]; c.close()
    return dict(zip(cols, row)) if row else None

def all_jobs():
    c = _db()
    # Don't load blobs in list view — too heavy
    df = pd.read_sql(
        "SELECT id,name,filename,started,status,total,done,found,failed,err FROM jobs "
        "ORDER BY started DESC", c)
    c.close(); return df

def get_blob(jid, col):
    c = _db()
    row = c.execute(f"SELECT {col} FROM jobs WHERE id=?", (jid,)).fetchone()
    c.close(); return bytes(row[0]) if row and row[0] else None

def cleanup():
    cut = (datetime.now() - timedelta(days=7)).isoformat()
    c = _db(); c.execute("DELETE FROM jobs WHERE started<?", (cut,))
    c.commit(); c.close()

# ── Validate ───────────────────────────────────────────────────────────────────
def validate(df):
    errs, warns = [], []
    miss = [col for col in ('URL','Keyword') if col not in df.columns]
    if miss: errs.append(f"Missing columns: {miss}"); return errs, warns
    if df.empty: errs.append("File has no data rows.")
    if len(df) > LIMIT: errs.append(f"Exceeds {LIMIT:,} row limit ({len(df):,} rows)")
    nu = int(df['URL'].isna().sum())
    if nu: warns.append(f"{nu} empty URLs will be skipped")
    return errs, warns

# ── Template ───────────────────────────────────────────────────────────────────
@st.cache_data
def make_template() -> bytes:
    wb = Workbook(); ws = wb.active; ws.title = "Data"
    hf   = PatternFill("solid", fgColor="1F4E79")
    hfon = Font(bold=True, color="FFFFFF", name="Arial", size=11)
    th   = Side(style="thin", color="AAAAAA")
    bdr  = Border(left=th, right=th, top=th, bottom=th)
    for i, h in enumerate(["URL","Keyword"], 1):
        c = ws.cell(1,i,h); c.fill=hf; c.font=hfon; c.border=bdr
        c.alignment=Alignment(horizontal="center")
    dfon = Font(name="Arial", size=10)
    for i,(u,k) in enumerate([
        ("https://example.com/doc1.pdf", "39131706"),
        ("https://example.com/doc2.pdf", "EAN13 8013975216323|HS Code 853"),
        ("https://example.com/doc3.pdf", "keyword1|keyword2|keyword3"),
    ], 2):
        for j,v in enumerate([u,k],1):
            c=ws.cell(i,j,v); c.font=dfon; c.border=bdr
    ws.column_dimensions['A'].width=60
    ws.column_dimensions['B'].width=40
    buf=io.BytesIO(); wb.save(buf); return buf.getvalue()

# ── Search engine ──────────────────────────────────────────────────────────────
def raw_search(data: bytes, keywords: list) -> dict:
    text = data.decode("latin-1", errors="replace"); tlow = text.lower(); out = {}
    for kw in keywords:
        idx = tlow.find(kw.lower())
        if idx >= 0:
            ctx = text[max(0,idx-15):idx+len(kw)+30].replace("\n"," ").strip()
            out[kw] = ("Found", ctx[:80])
        else:
            out[kw] = ("Not Found", None)
    return out

def pdf_search(data: bytes, keywords: list) -> dict:
    try:
        doc  = fitz.open(stream=data, filetype="pdf")
        text = "".join(p.get_text() for p in doc); doc.close()
    except Exception:
        return {kw: ("Error", None) for kw in keywords}
    tlow = text.lower(); out = {}
    for kw in keywords:
        idx = tlow.find(kw.lower())
        if idx >= 0:
            ctx = text[max(0,idx-15):idx+len(kw)+30].replace("\n"," ").strip()
            out[kw] = ("Found", ctx[:80])
        else:
            out[kw] = ("Not Found", None)
    return out

async def search_one(session, sem, url: str, keywords: list, pool) -> tuple:
    empty = {kw: ("Not Found", None) for kw in keywords}
    url   = (url or "").strip()
    if not url or not url.startswith("http"):
        return empty, "Invalid URL"
    loop = asyncio.get_event_loop()
    for attempt in range(RETRIES + 1):
        try:
            async with sem:
                hdrs = {**UA, "Range": f"bytes=0-{MAX_BYTES-1}"}
                tmo  = aiohttp.ClientTimeout(total=TIMEOUT)
                async with session.get(url, headers=hdrs, timeout=tmo,
                                       allow_redirects=True, ssl=False) as r:
                    if r.status not in (200, 206):
                        if attempt < RETRIES: await asyncio.sleep(1.5**attempt); continue
                        return empty, f"HTTP {r.status}"
                    data = await r.read()
            if not data: return empty, "Empty"
            raw = await loop.run_in_executor(pool, raw_search, data, keywords)
            if all(v[0]=="Found" for v in raw.values()): return raw, "Done"
            if (b"FlateDecode" in data or b"flatedecode" in data) and \
               any(v[0]!="Found" for v in raw.values()):
                full   = await loop.run_in_executor(pool, pdf_search, data, keywords)
                merged = {kw: raw[kw] if raw[kw][0]=="Found" else full[kw] for kw in keywords}
                return merged, "Done"
            return raw, "Done"
        except asyncio.TimeoutError:
            if attempt < RETRIES: await asyncio.sleep(1); continue
            return empty, "Timeout"
        except Exception as e:
            if attempt < RETRIES: await asyncio.sleep(1); continue
            return empty, str(e)[:40]
    return empty, "Failed"

# ── Async orchestrator ─────────────────────────────────────────────────────────
async def _run(jid: str, df: pd.DataFrame):
    job      = get_job(jid)
    prog     = json.loads(job.get("progress","{}") or "{}")
    done_set = {int(k) for k in prog}
    total    = len(df)
    upd(jid, status="Running", total=total, done=len(done_set),
        found=sum(1 for v in prog.values() if v.get("us")=="Done"),
        failed=len(done_set)-sum(1 for v in prog.values() if v.get("us")=="Done"))

    sem  = asyncio.Semaphore(CONCURRENCY)
    conn = aiohttp.TCPConnector(limit=CONCURRENCY, limit_per_host=30,
                                ttl_dns_cache=300, enable_cleanup_closed=True)
    pool = ThreadPoolExecutor(max_workers=8)
    BATCH = 50

    async with aiohttp.ClientSession(connector=conn) as session:
        pending = []
        for idx, row in df.iterrows():
            if idx in done_set: continue
            url  = str(row.get("URL","")).strip()
            kws  = [k.strip() for k in str(row.get("Keyword","")).split("|")
                    if k.strip()][:3]
            pending.append((idx, url, kws))

        for b in range(0, len(pending), BATCH):
            batch = pending[b:b+BATCH]
            res   = await asyncio.gather(
                *[search_one(session, sem, url, kws, pool) for (_,url,kws) in batch],
                return_exceptions=True)
            for (idx, url, kws), r in zip(batch, res):
                if isinstance(r, Exception):
                    kw_res, us = {kw:("Not Found",None) for kw in kws}, str(r)[:40]
                else:
                    kw_res, us = r
                prog[str(idx)] = {
                    "us": us,
                    "kw": {k: {"s": v[0], "ctx": v[1]} for k,v in kw_res.items()}
                }
            n_done  = len(prog)
            n_found = sum(1 for v in prog.values() if v.get("us")=="Done")
            upd(jid, total=total, done=n_done, found=n_found,
                failed=n_done-n_found, progress=json.dumps(prog))

    pool.shutdown(wait=False)

def build_output(df: pd.DataFrame, prog: dict) -> pd.DataFrame:
    rows = []
    for idx, row in df.iterrows():
        kw_str = str(row.get("Keyword","")).strip()
        kws    = [k.strip() for k in kw_str.split("|") if k.strip()][:3]
        p      = prog.get(str(idx), {})
        us     = p.get("us","Pending")
        kwr    = p.get("kw", {})
        for kw in (kws or [kw_str]):
            r = kwr.get(kw, {"s":"Not Found","ctx":None})
            rows.append({
                "URL":                   str(row.get("URL","")),
                "Keyword":               kw_str,
                "Extraction Option":     None,
                "URL_Status":            3 if us=="Done" else 0,
                "URL_Search_Status":     us,
                "Keyword_Status":        3.0 if r["s"]=="Found" else 0.0,
                "feature_name":          kw,
                "feature_value":         r.get("ctx"),
                "Keyword_Search_Status": r["s"],
            })
    return pd.DataFrame(rows)

def to_xlsx_bytes(df: pd.DataFrame) -> bytes:
    wb = Workbook(); ws = wb.active; ws.title = "Results"
    hf   = PatternFill("solid", fgColor="1F4E79")
    hfon = Font(bold=True, color="FFFFFF", size=10, name="Arial")
    th   = Side(style="thin", color="CCCCCC")
    bdr  = Border(left=th, right=th, top=th, bottom=th)
    gf   = PatternFill("solid", fgColor="C6EFCE")
    rf   = PatternFill("solid", fgColor="FFCCCC")
    for i,h in enumerate(df.columns,1):
        c=ws.cell(1,i,h); c.fill=hf; c.font=hfon; c.border=bdr
        c.alignment=Alignment(horizontal="center")
    for ri,row in enumerate(df.itertuples(index=False),2):
        for ci,val in enumerate(row,1):
            c=ws.cell(ri,ci,val); c.font=Font(name="Arial",size=9); c.border=bdr
            if ci==len(df.columns):
                if str(val)=="Found":
                    c.fill=gf; c.font=Font(name="Arial",size=9,bold=True,color="276221")
                elif str(val)=="Not Found":
                    c.fill=rf; c.font=Font(name="Arial",size=9,color="9C0006")
    for i,w in enumerate([55,30,18,12,18,15,30,35,22][:len(df.columns)],1):
        ws.column_dimensions[ws.cell(1,i).column_letter].width=w
    buf=io.BytesIO(); wb.save(buf); return buf.getvalue()

def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")

def run_job(jid: str, df: pd.DataFrame):
    try:
        loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)
        loop.run_until_complete(_run(jid, df)); loop.close()

        job   = get_job(jid)
        prog  = json.loads(job.get("progress","{}") or "{}")
        out   = build_output(df, prog)

        # ── Store result bytes directly in DB ──────────────────────────────
        xlsx  = to_xlsx_bytes(out)
        csv_b = to_csv_bytes(out)
        upd(jid, status="Done", xlsx_bytes=xlsx, csv_bytes=csv_b)

    except Exception as e:
        upd(jid, status="Failed", err=str(e)[:500])

# ── UI ─────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="PDF Search", page_icon="🔍", layout="wide")

st.markdown("""
<style>
[data-testid="stAppViewContainer"]{background:#f5f7fa}
.block-container{padding:1.5rem 2rem;max-width:1300px}
.card{background:white;border-radius:12px;padding:18px 22px;
      box-shadow:0 2px 10px rgba(0,0,0,.07);margin-bottom:12px}
.big{font-size:2.2rem;font-weight:800;line-height:1}
.lbl{font-size:.78rem;color:#888;margin-top:4px}
.tag{display:inline-block;padding:3px 12px;border-radius:20px;
     font-size:.8rem;font-weight:700;letter-spacing:.3px}
.done   {background:#dcfce7;color:#166534}
.running{background:#fef9c3;color:#854d0e}
.failed {background:#fee2e2;color:#991b1b}
.pending{background:#f1f5f9;color:#475569}
.row-card{background:white;border-radius:10px;padding:14px 18px;
          box-shadow:0 1px 5px rgba(0,0,0,.06);margin-bottom:8px}
</style>
""", unsafe_allow_html=True)

init_db(); cleanup()

# ── Header ─────────────────────────────────────────────────────────────────────
st.markdown("""
<div style='background:linear-gradient(135deg,#1e3a5f,#2563eb);
     padding:20px 28px;border-radius:14px;color:white;margin-bottom:20px'>
  <div style='font-size:1.6rem;font-weight:800'>🔍 PDF Keyword Search</div>
  <div style='opacity:.8;margin-top:4px;font-size:.88rem'>
    150 parallel connections · 512 KB range · Raw-byte + PyMuPDF · Up to 50,000 rows
  </div>
</div>
""", unsafe_allow_html=True)

# ── Top bar ────────────────────────────────────────────────────────────────────
t1, t2, t3 = st.columns([1, 2, 1])
with t1:
    st.download_button("⬇ Template", make_template(),
        file_name="search_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True)
with t2:
    uf = st.file_uploader("Upload Excel (.xlsx)", type=["xlsx"],
                          label_visibility="collapsed")
with t3:
    if st.button("🔄 Refresh", use_container_width=True): st.rerun()

# ── Upload flow ────────────────────────────────────────────────────────────────
if uf:
    try:
        df = pd.read_excel(uf, dtype=str)
        errs, warns = validate(df)
        for w in warns: st.warning(w)
        if errs:
            for e in errs: st.error(f"❌ {e}")
        else:
            est = max(1, int(len(df) / CONCURRENCY * 7 / 60))
            c1, c2 = st.columns([4, 1])
            c1.success(f"✅ {len(df):,} rows · {df['URL'].notna().sum():,} valid URLs  |  "
                       f"Est. ~{est} min")
            if c2.button("🚀 Start", type="primary", use_container_width=True):
                jid  = str(uuid.uuid4())[:8]
                ts   = datetime.now().strftime("%Y%m%d%H%M%S%f")[:16]
                name = f"{ts}_{os.path.splitext(uf.name)[0]}"
                save_job({'id': jid, 'name': name, 'filename': uf.name,
                          'started': datetime.now().isoformat(),
                          'status': 'Pending', 'total': len(df)})
                threading.Thread(
                    target=run_job, args=(jid, df.copy()), daemon=True).start()
                st.toast("Job started! 🚀"); time.sleep(0.5); st.rerun()
    except Exception as e:
        st.error(f"❌ {e}")

st.markdown("---")

# ── Dashboard ──────────────────────────────────────────────────────────────────
jobs = all_jobs()
if jobs.empty:
    st.info("No jobs yet — upload a file above.")
else:
    # Metrics
    m1,m2,m3,m4,m5 = st.columns(5)
    for col,(v,l,clr) in zip([m1,m2,m3,m4,m5],[
        (len(jobs),                           "Total",   "#1e3a5f"),
        ((jobs.status=='Running').sum(),       "Running", "#854d0e"),
        ((jobs.status=='Done').sum(),          "Done",    "#166534"),
        ((jobs.status=='Failed').sum(),        "Failed",  "#991b1b"),
        ((jobs.status=='Pending').sum(),       "Pending", "#475569"),
    ]):
        col.markdown(
            f"<div class='card' style='text-align:center;padding:14px'>"
            f"<div class='big' style='color:{clr}'>{v}</div>"
            f"<div class='lbl'>{l}</div></div>", unsafe_allow_html=True)

    # Job rows
    for _, job in jobs.iterrows():
        status = job['status']
        tc     = {'Done':'done','Running':'running','Failed':'failed'}.get(status,'pending')
        tot    = int(job['total']  or 0)
        done_n = int(job['done']   or 0)
        found  = int(job['found']  or 0)
        fail   = int(job['failed'] or 0)

        with st.container():
            st.markdown('<div class="row-card">', unsafe_allow_html=True)
            a, b, c, d, e, f = st.columns([3.5, 1, 1, 1, 1, 2])

            nm = str(job['name'])
            a.markdown(f"**{nm[:58]}{'…' if len(nm)>58 else ''}**")
            b.markdown(f"<span class='tag {tc}'>{status}</span>", unsafe_allow_html=True)
            c.metric("Rows", f"{tot:,}")
            d.metric("✓ Found", f"{found:,}")
            e.metric("✗ Failed", f"{fail:,}")

            if status == 'Done':
                f1, f2 = f.columns(2)
                # Load blobs only when job is Done and shown
                jid = str(job['id'])
                xlsx_b = get_blob(jid, 'xlsx_bytes')
                csv_b  = get_blob(jid, 'csv_bytes')
                ts_part = str(job['started'])[:10].replace('-','')
                fname   = f"{ts_part}_{job['filename']}"
                if xlsx_b:
                    f1.download_button(
                        "⬇ XLSX", xlsx_b,
                        file_name=fname.replace('.xlsx','').replace('.csv','') + ".xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"x{jid}")
                if csv_b:
                    f2.download_button(
                        "⬇ CSV", csv_b,
                        file_name=fname.replace('.xlsx','').replace('.csv','') + ".csv",
                        mime="text/csv", key=f"c{jid}")

            elif status == 'Failed':
                f.error(str(job.get('err','Error'))[:80])
            elif status == 'Running':
                f.markdown(f"⏳ {done_n:,} / {tot:,}")
            else:
                f.markdown(f"⏳ Queued")

            if status == 'Running' and tot > 0:
                pct = done_n / tot
                st.progress(pct, text=f"⚡ {done_n:,} / {tot:,}  ({pct*100:.1f}%)")

            st.markdown('</div>', unsafe_allow_html=True)

# Auto-refresh while active
if not jobs.empty and jobs['status'].isin(['Running','Pending']).any():
    time.sleep(3); st.rerun()
