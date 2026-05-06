# 🔍 PDF Keyword Search System — v5.0

> **Fast · Reliable · 5-Status Output**
> Search for keywords inside PDF documents and HTML pages at scale.
> Upload a file, start the job, download clean results.

---

## ✨ What This System Does

You provide a list of document URLs and keywords. The system downloads each document, reads the text, searches for your keyword, and tells you exactly what it found — in one of five clear result values.

It runs multiple downloads in parallel, automatically retries failures, saves progress to disk, and produces a clean Excel or CSV output file.

---

## ✅ Key Features

| Feature | Details |
|---------|---------|
| ⚡ **Parallel processing** | Up to 20 concurrent workers |
| 📄 **PDF + HTML** | Both file types handled automatically |
| 🔁 **Automatic retry** | Failed downloads retried in a second pass |
| 💾 **Auto-save every 100 rows** | Progress saved to disk — survives page refresh or disconnect |
| ♻️ **Recovery panel** | Restore or download partial results at any time |
| 🔍 **Full text search** | Counts every occurrence, not just the first |
| 🔗 **URL caching** | Same document downloaded only once even with multiple keywords |
| ☀️🌙 **Adaptive theme** | Clean in both light and dark mode |

---

## 🗂️ Project Structure

```
pdf-keyword-search/
├── app.py               ← Streamlit application
├── requirements.txt     ← Python dependencies
└── README.md            ← This file
```

---

## 🚀 Setup

### Requirements

- **Python 3.10 or higher**
- Internet access to download documents

### Install and Run

```bash
git clone https://github.com/YOUR_USERNAME/pdf-keyword-search.git
cd pdf-keyword-search

python -m venv venv

# macOS / Linux
source venv/bin/activate

# Windows
venv\Scripts\activate

pip install -r requirements.txt
streamlit run app.py
```

Opens at `http://localhost:8501`

---

## ☁️ Deploy on Streamlit Cloud

1. Push repository to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io) → **New app**
3. Select your repository
4. Set **Main file path** → `app.py`
5. Click **Deploy**

---

## 📋 Input File Format

Upload an **Excel (.xlsx)** or **CSV** file with exactly these two columns:

| URL | Keyword |
|-----|---------|
| `https://example.com/document.pdf` | `search term` |
| `https://example.com/page.html` | `another term` |

**Notes:**
- `URL` must be a direct link to a PDF or HTML file
- `Keyword` is the exact term to search for
- For multiple keywords in one row, separate with `|` — e.g. `term1|term2|term3`
- Blank rows are ignored automatically
- Maximum **50,000 rows** per run
- Download the template from the sidebar before filling your data

---

## 🚦 The 5 Result Statuses

Every row in the output will have exactly one of these values in `Keyword_Search_Status`:

| Status | Meaning |
|--------|---------|
| ✅ **Found** | Keyword was located in the document |
| ❌ **Not Found** | Document was read successfully — keyword is not present |
| 🟡 **PDF is Non searchable, Advanced Scanned Extraction can make the PDF searchable.** | Image or scanned PDF with no text layer |
| 🟣 **PDF Not mirrored / Corrupted** | File is damaged, empty, or unreadable |
| 🔺 **Failed to get PDF text** | Document could not be downloaded or accessed |

> The difference between **Not Found** and **Failed to get PDF text**:
> - **Not Found** = system read the document successfully, keyword is simply absent
> - **Failed to get PDF text** = system could not retrieve the document at all

---

## 📤 Output File Columns

| Column | Description |
|--------|-------------|
| `URL` | Original URL from your input file |
| `Keyword` | Keyword as entered in your input file |
| `Search Mode` | Single / Multi / Table / Auto |
| `Keyword_Search_Status` | **Main result** — one of the 5 statuses above |
| `Match Count` | Total times the keyword appears in the document |
| `Snippet` | ~120-character context around the first match |
| `Matched Keywords` | Which keywords were found (Multi Search only) |
| `Missing Keywords` | Which keywords were not found (Multi Search only) |
| `Notes` | Extra detail for Failed rows — empty for all other statuses |

### Excel Output — 6 Sheets

| Sheet | Contents |
|-------|----------|
| **All Results** | Every row |
| **Found** | Keyword was found |
| **Not Found** | Document read, keyword absent |
| **Scanned** | Image PDFs with no text |
| **Corrupted** | Unreadable files |
| **Failed** | Could not be downloaded |

---

## 🔘 Search Modes

| Mode | Keyword Format | How It Works |
|------|---------------|-------------|
| **Auto Detect** *(recommended)* | Any | App detects the right mode automatically |
| **Single Search** | `keyword` | One keyword per row |
| **Multi Search** | `kw1\|kw2\|kw3` | Multiple keywords — Match ANY or Match ALL |
| **Table Search** | `code1\|code2` | Same as Multi, optimised for numeric values |

**Match ANY** *(default)*: Found if at least one keyword is present.
**Match ALL**: Found only when every keyword matches.

---

## ⚙️ Settings Reference

| Setting | Default | Range | Description |
|---------|---------|-------|-------------|
| **Concurrent Workers** | 10 | 2 – 20 | Parallel downloads. Reduce to 4–6 if you see many failures. |
| **Timeout per URL** | 15 s | 5 – 60 s | Max wait per document. Raise for large or slow files. |
| **Case-Sensitive Search** | OFF | ON / OFF | OFF: `ABC` matches `abc`. ON: exact case only. |
| **Retry Failed URLs** | ON | ON / OFF | Retries failed downloads automatically in a second pass. |
| **Mirror Fallback** | ON | ON / OFF | Tries alternate URL paths if the primary fails. |
| **Smart Error Detection** | ON | ON / OFF | Detects pages that return success but contain an error message. |
| **Output Format** | Excel | Excel / CSV | Download format for results. |

---

## 🔄 Retry Logic

| Pass | What happens |
|------|-------------|
| **Pass 1** | Every URL — up to 3 attempts with exponential back-off |
| **Alternate paths** | On failure, alternate URL structures are tried automatically |
| **12-second cooldown** | Pause before Pass 2 to let servers recover |
| **Pass 2** | Only failed URLs retried — all successful rows are preserved |

---

## 💾 Auto-Save and Recovery

- Results saved to disk every **100 rows** during processing
- Survives: page refresh · internet drop · browser close · accidental Stop
- The **Recovery** panel in the sidebar lets you:
  - See how many rows are saved and when
  - Download saved data as CSV or Excel
  - Restore saved results to the Results tab
  - Clear the saved file after downloading

---

## ⚡ Performance Tips

- **10 workers** is the default — works well for most servers
- **Reduce to 4–6** if you see many Failed results
- **Timeout 15 s** for most documents; raise to 30–40 s for large files
- **Duplicate URLs** are downloaded only once — extracted text is cached and reused across all rows with the same URL
- **CSV export** is faster than Excel for large result sets
- Check the **Logs tab** after a run for a full issue breakdown

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

**Python 3.10 or higher is required.**
