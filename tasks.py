
import os
import re
import time
import json
import shutil
import logging
import subprocess
import requests
import mysql.connector
from pathlib import Path
from typing import Optional
from celery.utils.log import get_task_logger
from playwright.sync_api import sync_playwright

from celery_worker import app

# ---------------------------
# ML IMPORTS (Lazy load or explicit)
# ---------------------------
# ML Imports moved to inside calculate_relevancy to prevent memory bloat on PDF workers
# import sys
# import joblib
# import numpy as np

# Add relevency/scripts to path found in run.py logic:
GLOBAL_RELEVANCY_DIR = os.path.join(os.path.dirname(__file__), "relevency", "scripts")
# if GLOBAL_RELEVANCY_DIR not in sys.path:
#     sys.path.append(GLOBAL_RELEVANCY_DIR)

# try:
#     from global_relevancy import predict as global_predict
#     # Load Matcher
#     from app.matching.datastore import KeywordStore
#     from app.matching.matcher import Matcher
    
#     # Initialize Matcher
#     BACKEND_DIR = os.path.join(os.path.dirname(__file__), "app")
#     DATA_DIR = os.path.join(BACKEND_DIR, "data")
#     STORE = KeywordStore()
    
#     # Try loading all CSVs
#     for csv_name in ["keywords_diagnostic.csv", "keywords_endo.csv", "keywords_sheet1.csv"]:
#         csv_path = os.path.join(DATA_DIR, csv_name)
#         if os.path.exists(csv_path):
#             cat = "Diagnostic" if "diagnostic" in csv_name else ("Endo" if "endo" in csv_name else "Overall")
#             STORE.load_csv(csv_path, category=cat)
            
#     MATCHER = Matcher(STORE)
    
# except Exception as e:
#     pass
        print(f"app/matching contents: {os.listdir(os.path.join(os.path.dirname(__file__), 'app', 'matching'))}")
    except Exception as dir_err:
        print(f"Could not list directories: {dir_err}")

    global_predict = None
    MATCHER = None


# ---------------------------
# CONFIG
# ---------------------------
# Using relative paths so it works both on host and inside Docker (mapped to /app)
BASE_DIR = Path(__file__).resolve().parent

PDF_DIR = BASE_DIR / "PDF"
JSON_DIR = BASE_DIR / "OUTPUT"
EXTRACTOR_DIR = BASE_DIR / "extractor"
EXTRACTOR_PATH = EXTRACTOR_DIR / "url_pdf_extraction.py"
TEMP_BASE = BASE_DIR / "TEMP_WORKERS"

# Ensure directories exist
os.makedirs(PDF_DIR, exist_ok=True)
os.makedirs(JSON_DIR, exist_ok=True)
os.makedirs(EXTRACTOR_DIR, exist_ok=True)
os.makedirs(TEMP_BASE, exist_ok=True)

# Logger
logger = get_task_logger(__name__)

# DB Config
# We support env vars for Docker usage, defaulting to host.docker.internal for 'localhost' access from container
DB_HOST = os.getenv("DB_HOST", "host.docker.internal")
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "tender_automation_with_ai")

DB_CONFIG = {
    "host": DB_HOST,
    "user": DB_USER,
    "password": DB_PASS,
    "database": DB_NAME
}

# Constants
PDF_DOWNLOAD_TIMEOUT = 30
DOWNLOAD_MAX_RETRIES = 3
MIN_PDF_SIZE_BYTES = 1000
EXTRACTOR_RUN_TIMEOUT = 5 * 60

# ---------------------------
# Utility Functions
# ---------------------------

def safe_name(text: str) -> str:
    text = (text or "").strip()
    return re.sub(r'[^A-Za-z0-9_-]', '_', text)

def db_connect():
    return mysql.connector.connect(**DB_CONFIG)

def urllib_base(url: str) -> str:
    try:
        from urllib.parse import urlparse
        p = urlparse(url)
        return f"{p.scheme}://{p.netloc}"
    except Exception:
        return ""

def sys_executable() -> str:
    import sys
    return sys.executable

def save_doc_record(bid_number, detail_url, pdf_url, pdf_path, json_path):
    conn = None
    try:
        conn = db_connect()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO gem_tender_docs (bid_number, detail_url, pdf_url, pdf_path, json_path)
            VALUES (%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                pdf_url = VALUES(pdf_url),
                pdf_path = VALUES(pdf_path),
                json_path = VALUES(json_path)
        """, (bid_number, detail_url, pdf_url, pdf_path, json_path))
        conn.commit()
        cursor.close()
        logger.info(f"DB saved for {bid_number}")
    except Exception as e:
        logger.error(f"Failed to save DB record for {bid_number}: {e}")
    finally:
        if conn:
            conn.close()

# ---------------------------
# Extraction & Download Logic (Ported)
# ---------------------------

def extract_pdf_url(detail_url: str, retry: int = 2, retry_delay: float = 1.0) -> Optional[str]:
    detail_url = (detail_url or "").strip()
    if not detail_url:
        return None

    if detail_url.lower().endswith(".pdf"):
        return detail_url
    if "showbidDocument" in detail_url:
        return detail_url

    for attempt in range(1, retry+2):
        try:
            with sync_playwright() as p:
                # Use chrome channel if available, or fall back to bundled chromium
                browser = p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])
                page = browser.new_page()
                try:
                    page.goto(detail_url, timeout=30000, wait_until="domcontentloaded")
                except Exception:
                    browser.close()
                    time.sleep(retry_delay)
                    continue

                links = page.query_selector_all("a")
                for link in links:
                    href = link.get_attribute("href")
                    if href and ".pdf" in href.lower():
                        if href.startswith("/"):
                            href = urllib_base(detail_url) + href
                        browser.close()
                        return href
                browser.close()
                return None
        except Exception as e:
            logger.warning(f"extract_pdf_url attempt {attempt} failed: {e}")
            time.sleep(retry_delay)
            continue
    return None

def download_pdf(pdf_url: str, dest_path: Path, timeout: int = PDF_DOWNLOAD_TIMEOUT) -> Optional[Path]:
    session = requests.Session()
    session.headers.update({"User-Agent": "tender_pipeline_bot/1.0"})
    
    for attempt in range(1, DOWNLOAD_MAX_RETRIES + 1):
        try:
            r = session.get(pdf_url, stream=True, timeout=timeout)
            r.raise_for_status()
            
            # Atomic write
            tmp_path = dest_path.with_suffix(dest_path.suffix + ".part")
            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            if tmp_path.exists() and tmp_path.stat().st_size >= MIN_PDF_SIZE_BYTES:
                os.replace(tmp_path, dest_path)
                logger.info(f"Downloaded {pdf_url} -> {dest_path}")
                return dest_path
            else:
                logger.warning(f"File too small ({tmp_path.stat().st_size if tmp_path.exists() else 0}), retrying...")
                if tmp_path.exists():
                    os.unlink(tmp_path)
        except Exception as e:
            logger.warning(f"Download attempt {attempt} failed: {e}")
            time.sleep(1)
            
    return None

def run_extractor(temp_pdf_dir: Path, temp_out_dir: Path, extractor_workers: int = 1) -> bool:
    if not EXTRACTOR_PATH.exists():
        logger.error(f"Extractor not found at {EXTRACTOR_PATH}")
        return False

    cmd = [
        sys_executable(),
        str(EXTRACTOR_PATH),
        "--skip-download",
        "--pdf-folder", str(temp_pdf_dir),
        "--out-folder", str(temp_out_dir),
        "--extract-workers", str(max(1, int(extractor_workers)))
    ]
    
    logger.info(f"Running extractor: {' '.join(cmd)}")
    try:
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=EXTRACTOR_RUN_TIMEOUT, text=True)
        if proc.returncode != 0:
            logger.error(f"Extractor failed (rc={proc.returncode}). Stderr: {proc.stderr}")
        
        # Check for JSONs
        jsons = list(temp_out_dir.glob("*.json"))
        return bool(jsons)
    except Exception as e:
        logger.exception(f"Extractor execution exception: {e}")
        return False

# ---------------------------
# CELERY TASK
# ---------------------------

@app.task(bind=True, acks_late=True, max_retries=3)
def process_tender(self, row: dict):
    """
    Celery task to process a single tender.
    row: {'bid_number': '...', 'detail_url': '...'}
    """
    bid = row.get("bid_number")
    detail_url = row.get("detail_url")
    
    # Unique ID for this task execution (using Celery task ID)
    task_id = self.request.id
    worker_name = f"task_{task_id}"
    
    logger.info(f"Starting task {task_id} for bid {bid}")
    
    safe_bid = safe_name(bid or "unknown")
    
    # Isolated temp folders using Task ID
    temp_pdf_dir = TEMP_BASE / f"pdf_{worker_name}"
    temp_out_dir = TEMP_BASE / f"out_{worker_name}"
    
    try:
        # cleanup old if exists
        if temp_pdf_dir.exists(): shutil.rmtree(temp_pdf_dir)
        if temp_out_dir.exists(): shutil.rmtree(temp_out_dir)
        temp_pdf_dir.mkdir(parents=True, exist_ok=True)
        temp_out_dir.mkdir(parents=True, exist_ok=True)
        
        # 1. Extract PDF URL
        pdf_url = extract_pdf_url(detail_url)
        if not pdf_url:
            logger.error(f"No PDF URL found for {bid}")
            return False
            
        # 2. Download
        pdf_filename = safe_bid + ".pdf"
        dest_path = temp_pdf_dir / pdf_filename
        downloaded = download_pdf(pdf_url, dest_path)
        
        if not downloaded:
            logger.error(f"Failed to download PDF for {bid}")
            return False
            
        # 3. Persistent Copy (Optional)
        try:
            shutil.copy2(dest_path, PDF_DIR / dest_path.name)
        except Exception:
            pass
            
        # 4. Run Extractor
        ok = run_extractor(temp_pdf_dir, temp_out_dir)
        if not ok:
            logger.error(f"Extractor failed for {bid}")
            return False
            
        # 5. Find and Move JSON
        json_path = None
        # Logic to find the right JSON
        candidates = list(temp_out_dir.glob("*.json"))
        expected = temp_out_dir / (safe_bid + ".json")
        
        if expected.exists() and expected.stat().st_size > 100:
            json_path = expected
        elif len(candidates) > 0:
            # simple fallback
            json_path = candidates[0]
            
        if not json_path:
            logger.error(f"No JSON generated for {bid}")
            return False
            
        final_json_path = JSON_DIR / (safe_bid + ".json")
        shutil.move(str(json_path), str(final_json_path))
        
        # 6. Save DB
        save_doc_record(bid, detail_url, pdf_url, str(dest_path), str(final_json_path))
        
        # 7. Cleanup Persistent PDF if needed
        # (Original code deleted it, keeping that behavior)
        persistent_pdf = PDF_DIR / dest_path.name
        if persistent_pdf.exists():
            persistent_pdf.unlink()
            
        logger.info(f"Successfully processed {bid}")
        return True

    except Exception as exc:
        logger.exception(f"Error processing {bid}: {exc}")
        # Retry logic handled by Celery if needed, for now just logging
        return False
    finally:
        # Cleanup temp dirs
        try:
            if temp_pdf_dir.exists(): shutil.rmtree(temp_pdf_dir, ignore_errors=True)
            if temp_out_dir.exists(): shutil.rmtree(temp_out_dir, ignore_errors=True)
        except Exception:
            pass

# ---------------------------
# RELEVANCY WORKER TASK
# ---------------------------

@app.task(bind=True, acks_late=True)
def calculate_relevancy(self, row: dict):
    """
    row: {
        'bid_number': str,
        'items': str
    }
    """
    bid_number = row.get("bid_number")
    items = row.get("items") or ""
    
    logger.info(f"Relevancy Worker Task Started for {bid_number}")

    if not global_predict:
        logger.error("Global_predict model is NONE. Check imports/paths in tasks.py")
        
    if not MATCHER:
        logger.error("MATCHER is NONE. Check imports/paths in tasks.py")

    if not global_predict or not MATCHER:
        logger.error("ML models not loaded. Cannot calculate relevancy.")
        return False
        
    try:
        logger.info(f"Running prediction for items: {items[:50]}...")
        # 1. Run Global Relevancy
        # Returns: (relevant_bool, score_float, category_str, best_match_json, top_matches_json)
        global_relevant, global_score, global_dept, best_match_json, top_matches_json = global_predict(items)
        
        # 2. Run Keyword Matcher
        match_count, matches, matches_status = MATCHER.match(items)
        match_relevency = "High" if match_count > 0 else "Low"
        
        # 3. Update Database
        conn = db_connect()
        cursor = conn.cursor()
        
        # Update gem_tenders
        update_sql = """
            UPDATE gem_tenders SET
                relevency_result = %s,
                main_relevency_score = %s,
                dept = %s,
                match_count = %s,
                matches = %s,
                matches_status = %s,
                match_relevency = %s
            WHERE bid_number = %s
        """
        cursor.execute(update_sql, (
            global_relevant,
            global_score,
            global_dept,
            match_count,
            json.dumps(matches),
            matches_status,
            match_relevency,
            bid_number
        ))
        
        # Upsert Main_Relevency
        main_sql = """
            INSERT INTO Main_Relevency
            (bid_number, query, detected_category, relevancy_score, relevant, best_match, top_matches)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                detected_category = VALUES(detected_category),
                relevancy_score = VALUES(relevancy_score),
                relevant = VALUES(relevant),
                best_match = VALUES(best_match),
                top_matches = VALUES(top_matches)
        """
        cursor.execute(main_sql, (
            bid_number,
            items,
            global_dept,
            global_score,
            global_relevant,
            best_match_json,
            top_matches_json
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Relevancy calculated for {bid_number}: Score={global_score}")
        return True

    except Exception as e:
        logger.exception(f"Error calculating relevancy for {bid_number}: {e}")
        return False
