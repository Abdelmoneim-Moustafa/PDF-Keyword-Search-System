
"""PDF Keyword Search — Simple Streamlit Cloud Edition

No external storage, no database, only upload -> process -> download.
"""

from __future__ import annotations

import asyncio
import io
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Dict, List, Tuple

import aiohttp
import fitz  # PyMuPDF
import pandas as pd
import streamlit as st
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
CONCURRENCY = 80
TIMEOUT = 20
RETRIES = 2
MAX_BYTES = 524_288
MAX_FEATURES = 3
LIMIT = 50_000
PDF_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0",
    "Accept": "application/pdf,*/*",
}

ILLEGAL_XLSX_CHARS = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def clean_text(value) -> str:
    """Remove control characters that Excel/openpyxl cannot store."""
    if value is None:
        return ""
    s = str(value)
    return ILLEGAL_XLSX_CHARS.sub(" ", s).strip()


def safe_sheet_title(title: str) -> str:
    title = clean_text(title) or "Sheet"
    title = re.sub(r"[\[\]\:\*\?\/\\]", "_", title)
    return title[:31]


def safe_filename(name: str) -> str:
    name = clean_text(name)
    name = re.sub(r"[^\w\-. ]+", "_", name, flags=re.UNICODE)
    return name.replace(" ", "_").strip("_") or "results"


def parse_keywords(keyword_text: str) -> List[str]:
    raw = clean_text(keyword_text)
    return [k.strip() for k in raw.split("|") if k.strip()][:MAX_FEATURES]


def validate_input(df: pd.DataFrame) -> Tuple[List[str], List[str]]:
    errors, warnings = [], []
    needed = [c for c in ("URL", "Keyword") if c not in df.columns]
    if needed:
        errors.append(f"Missing required columns: {', '.join(needed)}")
        return errors, warnings

    if df.empty:
        errors.append("The file has no data rows.")
    elif len(df) > LIMIT:
        errors.append(f"Row limit exceeded: {len(df):,} rows (limit {LIMIT:,}).")

    empty_urls = int(df["URL"].isna().sum())
    if empty_urls:
        warnings.append(f"{empty_urls} empty URL row(s) will be skipped.")

    too_many = sum(1 for x in df["Keyword"].fillna("").astype(str) if len(parse_keywords(x)) > MAX_FEATURES)
    if too_many:
        warnings.append(
            f"{too_many} row(s) contain more than {MAX_FEATURES} keywords; only the first {MAX_FEATURES} are used."
        )
    return errors, warnings


@st.cache_data
def make_template() -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = safe_sheet_title("Template")

    header_fill = PatternFill("solid", fgColor="1F4E79")
    header_font = Font(bold=True, color="FFFFFF", name="Arial", size=11)
    thin = Side(style="thin", color="B7C0CC")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    headers = ["URL", "Keyword"]
    for col_idx, header in enumerate(headers, 1):
        cell = ws.cell(1, col_idx, header)
        cell.fill = header_fill
        cell.font = header_font
        cell.border = border
        cell.alignment = Alignment(horizontal="center")

    sample_rows = [
        ("https://example.com/doc1.pdf", "39131706"),
        ("https://example.com/doc2.pdf", "EAN13 8013975216323|HS Code 85362010"),
        ("https://example.com/doc3.pdf", "keyword1|keyword2|keyword3"),
    ]
    for row_idx, (url, kw) in enumerate(sample_rows, 2):
        ws.cell(row_idx, 1, url)
        ws.cell(row_idx, 2, kw)

    ws.column_dimensions["A"].width = 60
    ws.column_dimensions["B"].width = 42

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def _raw_search(data: bytes, keywords: List[str]) -> Dict[str, Tuple[str, str | None]]:
    text = data.decode("latin-1", errors="replace")
    low = text.lower()
    out: Dict[str, Tuple[str, str | None]] = {}
    for kw in keywords:
        idx = low.find(kw.lower())
        if idx >= 0:
            ctx = text[max(0, idx - 18): idx + len(kw) + 35].replace("\n", " ").replace("\r", " ").strip()
            out[kw] = ("Found", clean_text(ctx[:120]))
        else:
            out[kw] = ("Not Found", None)
    return out


def _pdf_search(data: bytes, keywords: List[str]) -> Dict[str, Tuple[str, str | None]]:
    try:
        doc = fitz.open(stream=data, filetype="pdf")
        text = "".join(page.get_text() for page in doc)
        doc.close()
    except Exception:
        return {kw: ("Error", None) for kw in keywords}

    low = text.lower()
    out: Dict[str, Tuple[str, str | None]] = {}
    for kw in keywords:
        idx = low.find(kw.lower())
        if idx >= 0:
            ctx = text[max(0, idx - 18): idx + len(kw) + 35].replace("\n", " ").replace("\r", " ").strip()
            out[kw] = ("Found", clean_text(ctx[:120]))
        else:
            out[kw] = ("Not Found", None)
    return out


async def search_one(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    url: str,
    keywords: List[str],
    pool: ThreadPoolExecutor,
) -> Tuple[Dict[str, Tuple[str, str | None]], str]:
    empty = {kw: ("Not Found", None) for kw in keywords}
    url = clean_text(url)
    if not url or not url.startswith("http"):
        return empty, "Invalid URL"

    loop = asyncio.get_running_loop()

    for attempt in range(RETRIES + 1):
        try:
            async with sem:
                headers = {**PDF_HEADERS, "Range": f"bytes=0-{MAX_BYTES - 1}"}
                timeout = aiohttp.ClientTimeout(total=TIMEOUT)
                async with session.get(url, headers=headers, timeout=timeout, allow_redirects=True, ssl=False) as resp:
                    if resp.status not in (200, 206):
                        if attempt < RETRIES:
                            await asyncio.sleep(1.5 ** attempt)
                            continue
                        return empty, f"HTTP {resp.status}"
                    data = await resp.read()

            if not data:
                return empty, "Empty"

            raw = await loop.run_in_executor(pool, _raw_search, data, keywords)
            if all(v[0] == "Found" for v in raw.values()):
                return raw, "Done"

            # Use PDF text extraction if the first pass missed something.
            pdf_like = b"%PDF" in data[:20] or b"FlateDecode" in data or b"stream" in data[:1000]
            if pdf_like and any(v[0] != "Found" for v in raw.values()):
                full = await loop.run_in_executor(pool, _pdf_search, data, keywords)
                merged = {kw: raw[kw] if raw[kw][0] == "Found" else full[kw] for kw in keywords}
                return merged, "Done"

            return raw, "Done"

        except asyncio.TimeoutError:
            if attempt < RETRIES:
                await asyncio.sleep(1)
                continue
            return empty, "Timeout"
        except Exception as exc:
            if attempt < RETRIES:
                await asyncio.sleep(1)
                continue
            return empty, clean_text(str(exc))[:80]

    return empty, "Failed"


async def process_dataframe(
    df: pd.DataFrame,
    progress_placeholder=None,
) -> Dict[int, Dict]:
    sem = asyncio.Semaphore(CONCURRENCY)
    connector = aiohttp.TCPConnector(
        limit=CONCURRENCY,
        limit_per_host=30,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )
    pool = ThreadPoolExecutor(max_workers=8)
    results: Dict[int, Dict] = {}
    total = len(df)

    async with aiohttp.ClientSession(connector=connector) as session:
        pending = []
        for idx, row in df.iterrows():
            url = clean_text(row.get("URL", ""))
            kws = parse_keywords(row.get("Keyword", ""))
            pending.append((idx, url, kws))

        if progress_placeholder is not None:
            progress_placeholder.progress(0.0, text="Starting...")

        batch_size = 40
        completed = 0
        for start in range(0, len(pending), batch_size):
            batch = pending[start:start + batch_size]
            batch_results = await asyncio.gather(
                *[search_one(session, sem, url, kws, pool) for _, url, kws in batch],
                return_exceptions=True,
            )

            for (idx, _, kws), item in zip(batch, batch_results):
                if isinstance(item, Exception):
                    kw_res, status = {kw: ("Not Found", None) for kw in kws}, clean_text(str(item))[:80]
                else:
                    kw_res, status = item
                results[idx] = {"status": status, "keywords": kw_res}

            completed += len(batch)
            if progress_placeholder is not None and total:
                pct = completed / total
                progress_placeholder.progress(pct, text=f"Processing {completed:,}/{total:,} rows ({pct:.0%})")

    pool.shutdown(wait=False)
    return results


def build_output(df: pd.DataFrame, results: Dict[int, Dict]) -> pd.DataFrame:
    rows = []
    for idx, row in df.iterrows():
        url = clean_text(row.get("URL", ""))
        kw_text = clean_text(row.get("Keyword", ""))
        kws = parse_keywords(kw_text) or [kw_text]
        result = results.get(idx, {"status": "Pending", "keywords": {}})
        url_status = result.get("status", "Pending")
        kw_results = result.get("keywords", {})

        for kw in kws:
            kw_status, ctx = kw_results.get(kw, ("Not Found", None))
            rows.append(
                {
                    "URL": url,
                    "Keyword": kw_text,
                    "Extraction Option": "",
                    "URL_Status": 3 if url_status == "Done" else 0,
                    "URL_Search_Status": url_status,
                    "Keyword_Status": 3 if kw_status == "Found" else 0,
                    "feature_name": kw,
                    "feature_value": clean_text(ctx) if ctx else "",
                    "Keyword_Search_Status": kw_status,
                }
            )
    return pd.DataFrame(rows)


def dataframe_to_xlsx_bytes(df: pd.DataFrame, sheet_name: str = "Results") -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = safe_sheet_title(sheet_name)

    header_fill = PatternFill("solid", fgColor="1F4E79")
    header_font = Font(bold=True, color="FFFFFF", name="Arial", size=10)
    thin = Side(style="thin", color="D0D7DE")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    found_fill = PatternFill("solid", fgColor="C6EFCE")
    not_found_fill = PatternFill("solid", fgColor="FCE4D6")
    error_fill = PatternFill("solid", fgColor="F4CCCC")

    # Write header
    for col_idx, col_name in enumerate(df.columns, 1):
        cell = ws.cell(1, col_idx, clean_text(col_name))
        cell.fill = header_fill
        cell.font = header_font
        cell.border = border
        cell.alignment = Alignment(horizontal="center", vertical="center")

    # Write data
    for row_idx, row in enumerate(df.itertuples(index=False), 2):
        row_values = list(row)
        for col_idx, value in enumerate(row_values, 1):
            safe_value = clean_text(value)
            cell = ws.cell(row_idx, col_idx, safe_value)
            cell.font = Font(name="Arial", size=9)
            cell.border = border
            if col_idx == len(df.columns):
                status = str(safe_value)
                if status == "Found":
                    cell.fill = found_fill
                    cell.font = Font(name="Arial", size=9, bold=True, color="1F6B2E")
                elif status == "Not Found":
                    cell.fill = not_found_fill
                elif status not in ("Done", "Pending", "Invalid URL"):
                    cell.fill = error_fill

    # Widths
    widths = [58, 34, 18, 12, 18, 12, 28, 42, 22]
    for i, width in enumerate(widths[: len(df.columns)], 1):
        ws.column_dimensions[get_column_letter(i)].width = width

    ws.freeze_panes = "A2"

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    out = df.copy()
    for col in out.columns:
        out[col] = out[col].map(clean_text)
    return out.to_csv(index=False).encode("utf-8-sig")


def result_summary(output_df: pd.DataFrame) -> Dict[str, int]:
    if output_df.empty:
        return {"rows": 0, "found": 0, "not_found": 0, "invalid": 0}
    found = int((output_df["Keyword_Search_Status"] == "Found").sum())
    not_found = int((output_df["Keyword_Search_Status"] == "Not Found").sum())
    invalid = int((output_df["URL_Search_Status"] == "Invalid URL").sum())
    return {"rows": int(len(output_df)), "found": found, "not_found": not_found, "invalid": invalid}


# ──────────────────────────────────────────────────────────────────────────────
# UI
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="PDF Keyword Search", page_icon="🔍", layout="wide")

st.markdown(
    """
<style>
.block-container{padding-top:1.1rem;padding-bottom:1rem;max-width:1400px}
.hero{
    background: linear-gradient(135deg,#1e3a5f,#2563eb);
    padding: 22px 26px;
    border-radius: 18px;
    color: white;
    margin-bottom: 16px;
}
.hero h1{margin:0;font-size:1.8rem;font-weight:800}
.hero p{margin:.35rem 0 0 0;opacity:.88}
.card{
    background: white;
    border-radius: 16px;
    padding: 16px 18px;
    box-shadow: 0 2px 10px rgba(0,0,0,.06);
    margin-bottom: 12px;
}
.metric-title{font-size:.82rem;color:#64748b;margin-top:4px}
.badge{
    display:inline-block;padding:3px 11px;border-radius:999px;
    font-weight:700;font-size:.8rem
}
.good{background:#dcfce7;color:#166534}
.warn{background:#fef3c7;color:#854d0e}
.bad{background:#fee2e2;color:#991b1b}
.muted{color:#64748b}
</style>
""",
    unsafe_allow_html=True,
)

st.markdown(
    """
<div class="hero">
  <h1>🔍 PDF Keyword Search System</h1>
  <p>Upload an Excel file, scan the URLs, preview the results, and download XLSX/CSV immediately. No storage needed.</p>
</div>
""",
    unsafe_allow_html=True,
)

tab_overview, tab_run, tab_results, tab_help = st.tabs(["Overview", "Run Search", "Results", "Help"])

if "last_input_df" not in st.session_state:
    st.session_state.last_input_df = None
if "last_output_df" not in st.session_state:
    st.session_state.last_output_df = None
if "last_xlsx" not in st.session_state:
    st.session_state.last_xlsx = None
if "last_csv" not in st.session_state:
    st.session_state.last_csv = None
if "last_name" not in st.session_state:
    st.session_state.last_name = None
if "last_started" not in st.session_state:
    st.session_state.last_started = None

with tab_overview:
    st.subheader("What this app does")
    c1, c2, c3 = st.columns(3)
    c1.markdown('<div class="card"><b>1. Upload</b><div class="metric-title">Excel with URL + Keyword</div></div>', unsafe_allow_html=True)
    c2.markdown('<div class="card"><b>2. Process</b><div class="metric-title">Search PDF content from URLs</div></div>', unsafe_allow_html=True)
    c3.markdown('<div class="card"><b>3. Download</b><div class="metric-title">Get XLSX and CSV instantly</div></div>', unsafe_allow_html=True)

    if st.session_state.last_output_df is not None:
        summary = result_summary(st.session_state.last_output_df)
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Output rows", f"{summary['rows']:,}")
        m2.metric("Found", f"{summary['found']:,}")
        m3.metric("Not found", f"{summary['not_found']:,}")
        m4.metric("Invalid URLs", f"{summary['invalid']:,}")

        chart_df = pd.DataFrame(
            {
                "Status": ["Found", "Not Found", "Invalid URL"],
                "Count": [summary["found"], summary["not_found"], summary["invalid"]],
            }
        ).set_index("Status")
        st.bar_chart(chart_df)
    else:
        st.info("Run a search first to see output metrics and charts.")

with tab_run:
    left, right = st.columns([1, 1])
    with left:
        st.download_button(
            "⬇ Download template",
            make_template(),
            file_name="pdf_keyword_search_template.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    with right:
        if st.button("🔄 Clear current result", use_container_width=True):
            st.session_state.last_input_df = None
            st.session_state.last_output_df = None
            st.session_state.last_xlsx = None
            st.session_state.last_csv = None
            st.session_state.last_name = None
            st.session_state.last_started = None
            st.rerun()

    uploaded = st.file_uploader("Upload Excel file with URL and Keyword columns", type=["xlsx"])

    if uploaded is not None:
        try:
            input_df = pd.read_excel(uploaded, dtype=str).fillna("")
            errors, warnings = validate_input(input_df)

            for w in warnings:
                st.warning(w)
            for e in errors:
                st.error(f"❌ {e}")

            if not errors:
                st.session_state.last_input_df = input_df.copy()

                rows = len(input_df)
                valid_urls = int(input_df["URL"].astype(str).str.startswith("http").sum())
                kw_counts = input_df["Keyword"].astype(str).apply(lambda x: len(parse_keywords(x) or []))

                a, b, c, d = st.columns(4)
                a.metric("Rows", f"{rows:,}")
                b.metric("Valid URLs", f"{valid_urls:,}")
                c.metric("Avg keywords", f"{kw_counts.mean():.1f}")
                d.metric("Max keywords", f"{kw_counts.max():,}")

                st.caption("Input preview")
                st.dataframe(input_df.head(10), use_container_width=True)

                st.caption("Input visualization")
                st.bar_chart(kw_counts.value_counts().sort_index())

                if st.button("🚀 Start search", type="primary", use_container_width=True):
                    st.session_state.last_started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    st.session_state.last_name = clean_text(uploaded.name)

                    progress = st.empty()
                    spinner = st.spinner("Processing URLs...")
                    with spinner:
                        results = asyncio.run(process_dataframe(input_df, progress_placeholder=progress))

                    output_df = build_output(input_df, results)
                    xlsx_bytes = dataframe_to_xlsx_bytes(output_df, sheet_name="Results")
                    csv_bytes = dataframe_to_csv_bytes(output_df)

                    st.session_state.last_output_df = output_df
                    st.session_state.last_xlsx = xlsx_bytes
                    st.session_state.last_csv = csv_bytes

                    progress.empty()
                    st.success("Done. Your files are ready to download.")
                    st.rerun()

        except Exception as exc:
            st.error(f"❌ {clean_text(exc)}")

with tab_results:
    if st.session_state.last_output_df is None:
        st.info("No output yet. Run a search first.")
    else:
        output_df = st.session_state.last_output_df
        summary = result_summary(output_df)

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Rows", f"{summary['rows']:,}")
        m2.metric("Found", f"{summary['found']:,}")
        m3.metric("Not found", f"{summary['not_found']:,}")
        m4.metric("Invalid URLs", f"{summary['invalid']:,}")

        st.markdown(
            f"<span class='badge good'>Ready</span> "
            f"<span class='muted'>Input: {st.session_state.last_name or 'file'} | Started: {st.session_state.last_started or '-'}</span>",
            unsafe_allow_html=True,
        )

        dl1, dl2 = st.columns(2)
        dl1.download_button(
            "⬇ Download XLSX",
            st.session_state.last_xlsx,
            file_name=safe_filename((st.session_state.last_name or "results").replace(".xlsx", "")) + ".xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
        dl2.download_button(
            "⬇ Download CSV",
            st.session_state.last_csv,
            file_name=safe_filename((st.session_state.last_name or "results").replace(".csv", "")) + ".csv",
            mime="text/csv",
            use_container_width=True,
        )

        st.caption("Output preview")
        st.dataframe(output_df.head(20), use_container_width=True)

        st.caption("Output visualization")
        chart_df = pd.DataFrame(
            {
                "Status": ["Found", "Not Found", "Invalid URL"],
                "Count": [summary["found"], summary["not_found"], summary["invalid"]],
            }
        ).set_index("Status")
        st.bar_chart(chart_df)

with tab_help:
    st.markdown("### How to use")
    st.write(
        """
        1. Download the template.
        2. Fill the Excel file with `URL` and `Keyword`.
        3. Use `|` to separate multiple keywords in one row.
        4. Upload the file and click **Start search**.
        5. Download the generated XLSX or CSV from the Results tab.
        """
    )

    st.markdown("### Notes")
    st.write(
        f"""
        - Max rows: {LIMIT:,}
        - Max keywords per row: {MAX_FEATURES}
        - Fetch byte range: {MAX_BYTES:,} bytes
        - Timeout: {TIMEOUT} seconds
        - No database, no external storage, only in-session results
        """
    )
