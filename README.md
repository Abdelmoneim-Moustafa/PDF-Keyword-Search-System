# 🔍 PDF Keyword Search System 

> **Single • Multi • Table Search Engine**
>
> Fast, concurrent keyword search across PDF and HTML URLs —
> with clean user-facing output, full occurrence counting,
> smart retry, disk-backed autosave, and full error isolation.

---

## ✅ What's New in v4.0

| Feature | Description |
|---------|-------------|
| 🧹 **Clean Output File** | Main result column shows only Found / Not Found / Partial Match / Non searchable. Technical errors go to the Notes column only. |
| 📊 **4 Excel Sheets** | All Results · Found · Not Found and Partial · Errors and Issues |
| 🔢 **Full Occurrence Count** | Counts every match in the document, not just the first |
| 📋 **3 Templates in One File** | Single Search / Multi Search / Table Search — clear sheet names |
| 💾 **Job State + Autosave** | Saves progress CSV + JSON state file every 100 rows |
| 🔧 **7 Bug Fixes** | Row dedup, state reset, wired controls, Extraction Option, retry efficiency, content validation, HTML not-found detection |

---

## ⚠️ Current Limitation

This version runs on **Streamlit**, which means:
- Processing happens in the browser session — not a true background server job
- If Streamlit Cloud restarts the app container, the in-memory job stops

**The autosave system (CSV + JSON on disk) mitigates most practical issues** — the sidebar
Recovery panel lets you download or restore partial results at any time.

For a fully persistent system that survives disconnection, see [Production Architecture](#production-architecture) below.

---

## 🗂️ Project Structure

```
pdf-keyword-search-system/
├── app.py               ← Main Streamlit application (v4.0)
├── requirements.txt     ← Python dependencies
└── README.md            ← This file
```

---

## 🚀 Setup & Run

### Requirements

- **Python 3.10+** (uses `X | Y` union type syntax)
- Internet access

### Install & Run

```bash
git clone https://github.com/YOUR_USERNAME/pdf-keyword-search-system.git
cd pdf-keyword-search-system

python -m venv venv
source venv/bin/activate    # Windows: venv\Scripts\activate

pip install -r requirements.txt
streamlit run app.py
```

Opens at `http://localhost:8501`

---

## ☁️ Deploy on Streamlit Cloud

1. Push repo to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io) → **New app**
3. Select repo, set **Main file** = `app.py`
4. Click **Deploy**

---

## 📋 Input Format

Upload Excel (`.xlsx`) or CSV with two columns:

| URL | Keyword |
|-----|---------|
| `https://source.z2data.com/…/file.pdf` | `51712160148` |
| `https://source.z2data.com/…/page.html` | `EAN123\|UPC456` |
| `https://source.z2data.com/…/doc.pdf` | `8471.30\|8471.41\|8471.49` |

Download the template from the sidebar before filling your data.
The template file contains three sheets: **Single Search**, **Multi Search**, **Table Search**.

---

## 🔘 Search Modes

| Mode | Keyword Format | Logic |
|------|---------------|-------|
| **Single Search** | `51712160148` | Found or Not Found |
| **Multi Search** | `EAN\|UPC\|GTIN` | ANY: Found if ≥1 match · ALL: Partial if some match |
| **Table Search** | `8471.30\|8471.41` | Same as Multi, optimized for numeric codes |
| **Auto Detect** | Any | `\|` → Multi; numeric → Table; else → Single |

---

## 🚦 Output: Result Values

| Result | Meaning |
|--------|---------|
| ✅ **Found** | Keyword located in the document |
| ❌ **Not Found** | Document searchable, keyword absent |
| ⚠️ **Partial Match** | Multi + Match ALL: some keywords found |
| 🟡 **Non searchable** | Image/scanned PDF — no text layer |

> Technical issues (SSL errors, timeouts, connection failures) appear only in the **Notes** column.
> The **Result** column always contains one of the four values above — clean for non-technical users.

---

## 📤 Output File Columns

| Column | Description |
|--------|-------------|
| `URL` | Original URL |
| `Keyword` | Keyword(s) as entered |
| `Search Mode` | Single / Multi / Table / Auto |
| `Result` | **Main result** — one of the four values above |
| `Match Count` | Total occurrences found in the full document |
| `Snippet` | ~100-char context window around the first match |
| `Matched Keywords` | Which keywords were found (Multi mode) |
| `Missing Keywords` | Which keywords were not found (Multi mode) |
| `Notes` | Technical detail for errors — empty for normal results |

### Excel Output Sheets

| Sheet | Contents |
|-------|----------|
| **All Results** | Every row in the run |
| **Found** | Rows where Result = Found |
| **Not Found and Partial** | Rows where Result = Not Found, Partial Match, or Non searchable |
| **Errors and Issues** | Rows with a non-empty Notes column |

---

## ⚙️ Settings Reference

| Setting | Default | Description |
|---------|---------|-------------|
| **Concurrent Workers** | 6 | Parallel downloads — keep 4–6 for z2data.com |
| **Timeout per URL** | 20s | Max wait — increase for large/slow PDFs |
| **Case-Sensitive** | OFF | OFF: `EAN123` matches `ean123` |
| **Enable Retry System** | ON | Automatically retries failed URLs in Pass 2 |
| **Enable Mirror Fallback** | ON | Tries `source1.z2data.com` if `source.z2data.com` fails |
| **Enable Smart Error Detection** | ON | Detects HTML "not found" pages returned as 200 OK |

---

## 🔄 Retry & Fallback

| Pass | What happens |
|------|-------------|
| **Pass 1** | All URLs — 3 attempts + exponential back-off |
| **Mirror swap** | `source.z2data.com` ↔ `source1.z2data.com` (automatic) |
| **Path strip** | `/web/` paths tried without `/web/` segment |
| **15s cooldown** | Waits before Pass 2 |
| **Pass 2** | Failed rows only — successful rows never re-processed |

---

## 💾 Auto-Save / Recovery

- Progress saved to disk every **100 rows** (CSV + JSON job state)
- Sidebar **Auto-Save / Recovery** panel: download CSV/Excel or restore to Results tab
- Partial results survive: page refresh, internet drop, accidental Stop
- Click **Clear** after downloading to remove saved files

---

## 🐛 Bug Fixes (v3 → v4)

| # | Bug | Fix |
|---|-----|-----|
| 1 | Retry dropped valid duplicate rows | Row IDs assigned per input row; retry tracks by ID not (URL, Keyword) |
| 2 | Rate-limiter delays carried over between runs | `_last_req.clear()` added to `_clear_all_state()` |
| 3 | enable_mirror / enable_smart / output_format had no effect | All three now wired into processing and download logic |
| 4 | Extraction Option column always empty | Set to "PDF" or "HTML" immediately after URL type detection |
| 5 | Pass 2 submission stagger killed parallel efficiency | Stagger removed; back-off stays inside download loop |
| 6 | Responses < 64 bytes marked corrupted incorrectly | Now checks PDF/HTML content signature before rejecting |
| 7 | HTML not-found detection skipped pages > 2000 chars | Now checks first 1000 chars of any page + expanded phrase list |

---

## 🏗 Production Architecture

For a fully server-side system where jobs survive disconnection, the recommended architecture is:

```
┌─────────────────────────────────────────────────┐
│  Frontend  (React / Next.js)                    │
│  • Upload file                                  │
│  • Poll job progress                            │
│  • Download result file                         │
└──────────────────┬──────────────────────────────┘
                   │ HTTP
┌──────────────────▼──────────────────────────────┐
│  Backend API  (FastAPI + Python 3.10+)          │
│  POST /api/jobs/upload                          │
│  GET  /api/jobs/{id}          (status + %)      │
│  GET  /api/jobs/{id}/download (result file)     │
│  POST /api/jobs/{id}/retry    (re-run failures) │
│  POST /api/jobs/{id}/cancel                     │
└──────────────────┬──────────────────────────────┘
                   │ Enqueue
┌──────────────────▼──────────────────────────────┐
│  Queue Broker  (Redis / RabbitMQ)               │
└──────────────────┬──────────────────────────────┘
                   │ Consume
┌──────────────────▼──────────────────────────────┐
│  Background Worker  (Celery / RQ)               │
│  • Download URLs concurrently                   │
│  • Extract PDF / HTML text                      │
│  • Search keywords (full occurrence count)      │
│  • Cache repeated URLs                          │
│  • Autosave every 100 rows to DB                │
│  • Retry failed rows                            │
│  • Write final result file to storage           │
└──────────────────┬──────────────────────────────┘
         ┌─────────┴──────────┐
         ▼                    ▼
┌────────────────┐   ┌────────────────────────┐
│  PostgreSQL    │   │  S3-compatible Storage │
│  • jobs        │   │  • uploaded files      │
│  • job_rows    │   │  • autosave CSV        │
│  • job_logs    │   │  • result files        │
│  • url_cache   │   └────────────────────────┘
└────────────────┘
```

### Recommended Stack

| Layer | Technology |
|-------|-----------|
| Frontend | React or Next.js |
| Backend API | FastAPI (Python 3.10+) |
| Worker | Celery + Redis |
| Database | PostgreSQL |
| File Storage | S3-compatible |
| Deployment | Docker on cloud VPS |

### Database Schema (key tables)

**jobs**: id, status, search_mode, total_rows, processed_rows, created_at, finished_at

**job_rows**: id, job_id, row_index, url, keyword, result, match_count, snippet, notes, retry_count

**job_logs**: id, job_id, log_level, message, created_at

**cached_documents**: normalized_url, extracted_text_path, extraction_type, created_at

### MVP Build Order

1. **Phase 1**: Upload → background job → download result (basic pipeline)
2. **Phase 2**: URL caching, retry system, autosave checkpoints
3. **Phase 3**: User accounts, job history, admin dashboard, usage analytics

---

## 🛠 Requirements

```
streamlit>=1.32.0
pandas>=2.0.0
openpyxl>=3.1.0
PyMuPDF>=1.23.0
requests>=2.31.0
urllib3>=2.0.0
```

**Python 3.10+** required.
