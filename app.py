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
CONCURRENT_DOWNLOADS = 12
TIMEOUT_SECONDS = 20


# ─── Core Search Logic ───────────────────────────────────────────────────────────

def extract_text_from_pdf_bytes(pdf_bytes: bytes) -> tuple[str, str]:
    """
    Extract text from PDF bytes.
    Returns (text, extraction_status)
    extraction_status: 'searchable' | 'scanned'
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


def search_keyword_in_text(text: str, keyword: str, case_sensitive: bool = False) -> tuple[bool, int, list[str]]:
    """
    Search keyword in text.
    Returns (found, count, context_snippets)
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
    for m in matches[:3]:
        start = max(0, m.start() - 60)
        end = min(len(text), m.end() + 60)
        snippet = text[start:end].replace("\n", " ").strip()
        snippets.append(f"…{snippet}…")

    return True, len(matches), snippets


def process_one_url(url: str, keyword: str, case_sensitive: bool, session_timeout: int) -> dict:
    """
    Download a PDF from URL and search for keyword.
    Returns a result dict.
    """
    result = {
        "URL": url,
        "Keyword": keyword,
        "Extraction_Option": "",
        "URL_Status": None,
        "URL_Search_Status": "",
        "Keyword_Status": None,
        "feature_name": keyword,
        "feature_value": "",
        "Keyword_Search_Status": "",
        "match_count": 0,
        "context": "",
    }

    if not url or not str(url).startswith("http"):
        result["URL_Status"] = 0
        result["URL_Search_Status"] = "Invalid URL"
        result["Keyword_Search_Status"] = "Invalid URL"
        return result

    import requests
    try:
        resp = requests.get(url, timeout=session_timeout, stream=True)
        if resp.status_code != 200:
            result["URL_Status"] = resp.status_code
            result["URL_Search_Status"] = f"HTTP {resp.status_code}"
            result["Keyword_Search_Status"] = f"HTTP {resp.status_code}"
            return result

        content_type = resp.headers.get("content-type", "")
        pdf_bytes = resp.content

    except requests.exceptions.Timeout:
        result["URL_Status"] = 0
        result["URL_Search_Status"] = "Timeout"
        result["Keyword_Search_Status"] = "Timeout"
        return result
    except Exception as e:
        result["URL_Status"] = 0
        result["URL_Search_Status"] = f"Download Error: {str(e)[:60]}"
        result["Keyword_Search_Status"] = f"Download Error: {str(e)[:60]}"
        return result

    # Attempt text extraction
    text, extraction_status = extract_text_from_pdf_bytes(pdf_bytes)

    if "error:" in extraction_status:
        result["URL_Status"] = 3
        result["URL_Search_Status"] = "Done"
        result["Keyword_Search_Status"] = f"PDF Not mirrored / Corrupted"
        return result

    result["URL_Status"] = 3
    result["URL_Search_Status"] = "Done"

    if extraction_status == "scanned":
        result["Keyword_Search_Status"] = (
            "PDF is Non searchable,"
            "Advanced Scanned Extraction can make the PDF searchable."
        )
        result["Keyword_Status"] = None
        return result

    # Keyword search
    found, count, snippets = search_keyword_in_text(text, keyword, case_sensitive)

    result["Keyword_Status"] = 3.0
    result["match_count"] = count

    if found:
        result["Keyword_Search_Status"] = "Found"
        result["feature_value"] = "; ".join(snippets[:2])
        result["context"] = "; ".join(snippets)
    else:
        result["Keyword_Search_Status"] = "Not Found"

    return result


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


def df_to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Results")
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

    workers = st.slider("Concurrent Workers", 4, 40, CONCURRENT_DOWNLOADS, 2,
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
                input_df = pd.read_csv(uploaded_file)
            else:
                input_df = pd.read_excel(uploaded_file)

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
                    completed = 0
                    log_lines = []

                    start_time = time.time()

                    def log(msg):
                        ts = datetime.now().strftime("%H:%M:%S")
                        log_lines.append(f"[{ts}] {msg}")
                        if len(log_lines) > 80:
                            log_lines.pop(0)

                    log(f"Starting search for {total:,} URLs with {workers} workers…")

                    with ThreadPoolExecutor(max_workers=workers) as executor:
                        futures = {
                            executor.submit(
                                process_one_url,
                                str(row.get("URL", "")),
                                str(row.get("Keyword", "")),
                                case_sensitive,
                                timeout,
                            ): row
                            for row in rows
                        }

                        for future in as_completed(futures):
                            if not st.session_state.running:
                                log("⏹ Stopped by user.")
                                executor.shutdown(wait=False, cancel_futures=True)
                                break

                            try:
                                res = future.result()
                                results.append(res)
                                completed += 1

                                status = res["Keyword_Search_Status"]
                                url_short = res["URL"][-50:] if len(res["URL"]) > 50 else res["URL"]
                                log(f"[{completed}/{total}] {status:12s} → …{url_short}")

                            except Exception as e:
                                completed += 1
                                log(f"[{completed}/{total}] EXCEPTION: {e}")

                            # Update UI every N records
                            if completed % max(1, min(20, total // 50)) == 0 or completed == total:
                                pct = completed / total
                                elapsed = time.time() - start_time
                                rate = completed / elapsed if elapsed > 0 else 0
                                eta_sec = (total - completed) / rate if rate > 0 else 0

                                prog_bar.progress(pct, text=f"Processing {completed:,}/{total:,}  •  {rate:.1f} URLs/sec  •  ETA {eta_sec:.0f}s")
                                status_text.markdown(
                                    f"⏱ **Elapsed:** {elapsed:.1f}s  |  "
                                    f"**Speed:** {rate:.1f} URLs/s  |  "
                                    f"**Done:** {completed:,}/{total:,}"
                                )

                                # Live log
                                log_area.markdown(
                                    f'<div class="progress-box">' +
                                    "<br>".join(log_lines[-30:]) +
                                    "</div>",
                                    unsafe_allow_html=True,
                                )

                    st.session_state.running = False

                    # Build results DataFrame
                    if results:
                        out_cols = [
                            "URL", "Keyword", "Extraction_Option",
                            "URL_Status", "URL_Search_Status",
                            "Keyword_Status", "feature_name",
                            "feature_value", "Keyword_Search_Status",
                        ]
                        results_df = pd.DataFrame(results)
                        # Rename to match expected output
                        results_df.rename(columns={"Extraction_Option": "Extraction Option"}, inplace=True)
                        out_cols[2] = "Extraction Option"
                        st.session_state.results_df = results_df[out_cols]

                        elapsed_total = time.time() - start_time
                        prog_bar.progress(1.0, text="✅ Search Complete!")
                        st.success(
                            f"✅ Finished **{completed:,}** URLs in **{elapsed_total:.1f}s** "
                            f"({completed/elapsed_total:.1f} URLs/sec)"
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

        styler = display_df.style
        if hasattr(styler, "map"):
            styled = styler.map(apply_status_badge, subset=["Keyword_Search_Status"])
        else:
            styled = styler.applymap(apply_status_badge, subset=["Keyword_Search_Status"])
        st.dataframe(styled, use_container_width=True, height=450)

        # Download
        st.markdown("---")
        st.markdown("### ⬇️ Download Results")
        col_dl1, col_dl2 = st.columns(2)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        with col_dl1:
            xlsx_bytes = df_to_excel_bytes(filtered)
            st.download_button(
                "📥 Download Excel (.xlsx)",
                data=xlsx_bytes,
                file_name=f"keyword_search_results_{timestamp}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                type="primary",
            )

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
