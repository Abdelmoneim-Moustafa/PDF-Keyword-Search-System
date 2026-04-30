# 🔍 PDF Keyword Search — Streamlit App

A fast, concurrent PDF & HTML keyword search tool that processes thousands of URLs and reports whether a keyword was found, with automatic retry on failure and smart rate limiting.

---

## ✅ Features

- Upload Excel / CSV with `URL` + `Keyword` columns
- Supports **PDF** and **HTML** page URLs
- Concurrent multi-threaded downloading (up to 20 workers)
- Searches text-based PDFs using PyMuPDF
- Searches HTML product pages using built-in HTML parser
- Detects scanned / image-only PDFs
- **Automatic second-pass retry** for any failed URLs
- **Smart rate limiter** — prevents server bans (250 ms min gap between requests)
- **Blocked-server detection** — auto-pauses 30 s when a host fails 5× in a row
- **Saves progress every 100 rows** — partial results survive a Stop
- Mirror fallback: tries `source.z2data.com` ↔ `source1.z2data.com` automatically
- Live progress bar + speed metrics + live log
- Filter, view, and download results as Excel or CSV
- **Limit: 50,000 URLs per run**

---

## 📂 Project Structure

```
pdf-keyword-search-system/
├── app.py               ← Main Streamlit application
├── requirements.txt     ← Python dependencies
└── README.md            ← This file
```

---

## 🚀 Setup & Run

### 1. Install Python (3.10+)

The app uses Python 3.10+ union type syntax (`X | Y`). Make sure your Python version is at least 3.10:

```bash
python --version
```

### 2. Create a virtual environment (recommended)

```bash
python -m venv venv

# Windows:
venv\Scripts\activate

# Mac/Linux:
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Run the app

```bash
streamlit run app.py
```

The app will open in your browser at `http://localhost:8501`

---

## ☁️ Deploy to GitHub + Streamlit Cloud

### Step 1 — Push to GitHub

```bash
git init
git add .
git commit -m "Initial commit: PDF Keyword Search app"
git remote add origin https://github.com/YOUR_USERNAME/pdf-keyword-search.git
git push -u origin main
```

### Step 2 — Deploy on Streamlit Cloud

1. Go to [share.streamlit.io](https://share.streamlit.io)
2. Click **New app**
3. Select your GitHub repo
4. Set **Main file path** to `app.py`
5. Click **Deploy**

---

## 📋 Input File Format

Your Excel (`.xlsx`) or CSV must have exactly these two columns:

| URL | Keyword |
|-----|---------|
| https://example.com/document.pdf | 51712160148 |
| https://example.com/product.html | 4015081636822 |

> **Note:** Both PDF and HTML URLs are supported. The app auto-detects the content type.

---

## 📤 Output Columns

| Column | Description |
|--------|-------------|
| `URL` | Original URL |
| `Keyword` | Keyword searched |
| `Extraction Option` | Extraction method used |
| `URL_Status` | `0` = failed, `3` = processed successfully |
| `URL_Search_Status` | `"Done"` if processed, error message if failed |
| `Keyword_Status` | `3.0` if keyword was checked, `None` if not reached |
| `feature_name` | The keyword searched |
| `feature_value` | Matched context snippet (~100 chars around first match) |
| `Keyword_Search_Status` | **Main result** — see values below |

### Keyword_Search_Status Values

| Value | Meaning |
|-------|---------|
| `Found` | Keyword found in the document |
| `Not Found` | Document searchable, keyword absent |
| `PDF is Non searchable, Advanced Scanned Extraction can make the PDF searchable.` | Image/scanned PDF — no text layer |
| `PDF Not mirrored / Corrupted` | File is unreadable or corrupted |
| `Download Error: …` | Network or connection failure (surfaced after both passes fail) |
| `HTTP 404`, `HTTP 403`, etc. | Permanent server errors — not retried |
| `Timeout` | Server did not respond within the timeout setting |

---

## ⚙️ Settings (Sidebar)

| Setting | Default | Range | Description |
|---------|---------|-------|-------------|
| Concurrent Workers | **6** | 2 – 20 | Parallel downloads. Keep low (4–8) for z2data.com to avoid rate limits |
| Per-URL Timeout | 20 s | 5 – 60 s | Max seconds to wait for a response per URL |
| Case-Sensitive Search | Off | On / Off | Toggle exact-case keyword matching |
| Output Format | Excel | Excel / CSV | Download format for results |

> **Tip:** If you see many Download Errors, reduce Workers to 4 and increase Timeout to 30 s.

---

## 🔄 Retry Logic

The app uses a **two-pass retry system** to minimise errors:

| Pass | What happens |
|------|-------------|
| **Pass 1** | All URLs processed with up to 4 attempts each + exponential back-off |
| **Mirror fallback** | On failure, automatically swaps `source.z2data.com` ↔ `source1.z2data.com` |
| **Path fallback** | For old `/web/` URLs, also tries the path without `/web/` |
| **15 s cooldown** | Waits 15 seconds before starting Pass 2 |
| **Pass 2** | Only the failed URLs are retried, with 1 s stagger between submissions |

---

## 🛡 Rate Limiting & Block Detection

| Feature | Behaviour |
|---------|-----------|
| **Inter-request delay** | Minimum 250 ms between requests to the same host (shared across all workers) |
| **429 handler** | Extra 5–10 s pause when the server returns HTTP 429 (Too Many Requests) |
| **Block detector** | After 5 consecutive failures on one host, all workers pause 30 s automatically |
| **Session refresh** | A fresh TCP connection is opened after any connection-level error |

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

- **Python 3.10+** required (uses union type syntax `X | Y`)
- Internet access (to download PDFs and HTML pages)
- At least 2 GB RAM for large batches (10,000+ URLs)
