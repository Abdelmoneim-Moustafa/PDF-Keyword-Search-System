# 🔍 PDF Keyword Search System

A Streamlit web application that searches PDF files for keywords using direct URL access.

## ✨ Features

- **Upload Excel files** with URLs and keywords (up to 50,000 rows)
- **Multi-keyword search**: Use `|` to separate up to 3 keywords per row (e.g. `keyword1|keyword2|keyword3`)
- **Download results** as both XLSX and CSV
- **Live progress tracking** with auto-refresh
- **Resumable jobs**: If internet interrupts, job resumes from where it stopped
- **Auto-cleanup**: Files older than 7 days are deleted automatically
- **Validation**: File structure and limit checks before processing

## 📁 Excel File Structure

Your upload file must have exactly these columns:

| Column | Required | Description | Example |
|--------|----------|-------------|---------|
| `URL` | ✅ | Direct link to PDF | `https://example.com/file.pdf` |
| `Keyword` | ✅ | Keyword(s) separated by `\|` | `39131706\|EAN 1234\|barcode` |

**Download the template** from the app to get a pre-formatted file.

## 📤 Output File Structure

Results are saved with the same base name as your upload:

| Column | Description |
|--------|-------------|
| `URL` | Original URL |
| `Keyword` | Original keyword string |
| `Extraction Option` | (blank) |
| `URL_Status` | 3=success, 0=failed |
| `URL_Search_Status` | Done / Timeout / HTTP 404 / etc |
| `Keyword_Status` | 3.0=found, 0.0=not found |
| `feature_name` | The specific keyword searched |
| `feature_value` | Text context where keyword was found |
| `Keyword_Search_Status` | Found / Not Found |

## 🚀 Quick Start

### Option 1: Run Locally

```bash
# 1. Clone the repo
git clone https://github.com/YOUR_USERNAME/pdf-keyword-search.git
cd pdf-keyword-search

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run
streamlit run app.py
```

Then open http://localhost:8501 in your browser.

### Option 2: Deploy on Streamlit Cloud (Free)

1. Push this repo to GitHub
2. Go to https://share.streamlit.io
3. Connect your GitHub account
4. Select this repo, branch: `main`, file: `app.py`
5. Click **Deploy** — done!

## 📂 Project Structure

```
pdf-keyword-search/
├── app.py              ← Main Streamlit application (run this)
├── requirements.txt    ← Python dependencies
├── README.md           ← This file
├── .gitignore          ← Git ignore rules
├── database/           ← SQLite DB (auto-created)
│   └── jobs.db
├── uploads/            ← Uploaded Excel files (auto-cleaned weekly)
└── results/            ← Result XLSX/CSV files (auto-cleaned weekly)
```

## 🔧 How It Works

1. **Upload**: You upload an Excel file with URLs and keywords
2. **Validate**: System checks columns, row limit (50k), and data quality
3. **Process**: Each PDF URL is downloaded and searched for keywords
4. **Resume**: If connection drops, job saves progress and resumes when restarted
5. **Download**: Results available as XLSX (formatted) or CSV

## 💡 Tips

- **Multiple keywords**: Use `|` between them — e.g. `barcode|EAN|GTIN`
- **Large files**: The system processes in background — refresh the page to check progress
- **Slow PDFs**: Each PDF has a 30-second timeout with 3 retry attempts
- **File naming**: Results are named `TIMESTAMP_OriginalFileName.xlsx/csv`

## 🗄️ Database

Jobs are tracked in a local SQLite database (`database/jobs.db`). Each job stores:
- Job ID, name, upload time, status
- Progress (row by row, persisted as JSON)
- Result file paths

**Cleanup**: Jobs and files older than 7 days are deleted automatically on startup.

## ⚙️ Configuration

Edit these constants at the top of `app.py`:

```python
LIMIT = 50000        # Max rows per upload
MAX_FEATURES = 3     # Max keywords per row (pipe-separated)
```

## 🌐 Streamlit Cloud Notes

When deployed on Streamlit Cloud:
- Files persist between sessions within the same deployment
- For production use with large volumes, consider a persistent database (PostgreSQL) and object storage (S3/GCS)
- The free tier allows 1 app with 1 GB RAM
