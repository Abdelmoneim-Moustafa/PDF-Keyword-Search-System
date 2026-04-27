# 🔍 PDF Keyword Search — Streamlit App

A fast, concurrent PDF keyword search tool that processes thousands of PDF URLs and reports whether a keyword was found.

---

## ✅ Features

- Upload Excel / CSV with `URL` + `Keyword` columns  
- Concurrent multi-threaded downloading (up to 40 workers)  
- Searches text-based PDFs using PyMuPDF  
- Detects scanned/image-only PDFs  
- Live progress bar + speed metrics  
- Filter, view, and download results as Excel or CSV  
- **Limit: 50,000 URLs per run**  

---

## 📂 Project Structure

```
keyword_search_app/
├── app.py               ← Main Streamlit application
├── requirements.txt     ← Python dependencies
└── README.md            ← This file
```

---

## 🚀 Setup & Run

### 1. Install Python (3.9+)

Make sure Python is installed:
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

Your Excel or CSV must have these columns:

| URL | Keyword |
|-----|---------|
| https://example.com/document.pdf | 51712160148 |
| https://example.com/report.pdf | 4015081636822 |

---

## 📤 Output Columns

| Column | Description |
|--------|-------------|
| `URL` | Original PDF URL |
| `Keyword` | Keyword searched |
| `Extraction Option` | Method used |
| `URL_Status` | Status code (3=OK, 4=Non-searchable) |
| `URL_Search_Status` | "Done" if processed |
| `Keyword_Status` | 3.0 if keyword was checked |
| `feature_name` | Keyword searched |
| `feature_value` | Matched context snippet |
| `Keyword_Search_Status` | **Main result** (see below) |

### Keyword_Search_Status Values

| Value | Meaning |
|-------|---------|
| `Found` | Keyword found in PDF |
| `Not Found` | PDF searchable, keyword absent |
| `PDF is Non searchable, Advanced Scanned Extraction can make the PDF searchable.` | Image/scanned PDF |
| `PDF Not mirrored / Corrupted` | File is unreadable |
| `HTTP 404`, `Timeout`, etc. | Network errors |

---

## ⚙️ Settings (Sidebar)

| Setting | Default | Description |
|---------|---------|-------------|
| Concurrent Workers | 12 | Parallel downloads |
| Per-URL Timeout | 20s | Max wait per URL |
| Case-Sensitive | Off | Toggle case matching |
| Output Format | Excel | Excel or CSV download |

---

## 🛠 Requirements

- Python 3.9+
- Internet access (to download PDFs)
- At least 2GB RAM for large batches
