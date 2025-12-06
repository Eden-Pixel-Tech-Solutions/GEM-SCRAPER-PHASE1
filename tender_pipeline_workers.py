#!/usr/bin/env python3
"""
tender_pipeline_workers.py

Production-ready parallel tender processing main script.

Features:
- Fetch N pending tenders from DB
- Process tenders in parallel with configurable worker count
- Each worker uses isolated temp folders to avoid collisions
- Robust retries, logging, graceful shutdown
- Calls existing extractor CLI (url_pdf_extraction.py) with --skip-download and --extract-workers 1
"""

import os
import re
import time
import json
import shutil
import signal
import logging
import argparse
import subprocess
from pathlib import Path
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial

import requests
import mysql.connector
from playwright.sync_api import sync_playwright

# ---------------------------
# CONFIG (tweak as needed)
# ---------------------------
BASE_DIR = Path(__file__).resolve().parent

# directories
PDF_DIR = BASE_DIR / "PDF"           # persistent pdf store (optional)
JSON_DIR = BASE_DIR / "OUTPUT"       # final JSON outputs
EXTRACTOR_DIR = BASE_DIR / "extractor"
EXTRACTOR_PATH = EXTRACTOR_DIR / "url_pdf_extraction.py"  # extractor CLI script (your file)

# temp base - each worker will have its own subfolders inside this
TEMP_BASE = BASE_DIR / "TEMP_WORKERS"

# database config
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "tender_automation_with_ai"
}

# worker & pipeline config (user-chosen)
GLOBAL_WORKERS = 5        # your choice (1) - number of parallel tender workers
FETCH_LIMIT = 20          # how many pending tenders to fetch per batch
SLEEP_BETWEEN_BATCHES = 60  # seconds to sleep between batches
EXTRACTOR_WORKERS_PER_JOB = 1  # per your choice A

# network & retry
PDF_DOWNLOAD_TIMEOUT = 30
DOWNLOAD_MAX_RETRIES = 3
EXTRACT_POLL_TIMEOUT = 120  # seconds to wait for extractor subprocess (max)
EXTRACTOR_RUN_TIMEOUT = 5 * 60  # seconds - extractor subprocess timeout

# logging
LOG_FILE = BASE_DIR / "tender_pipeline.log"
LOG_LEVEL = logging.INFO

# safety
MIN_PDF_SIZE_BYTES = 1000  # treat anything smaller as invalid

# ---------------------------
# utility & setup
# ---------------------------

os.makedirs(PDF_DIR, exist_ok=True)
os.makedirs(JSON_DIR, exist_ok=True)
os.makedirs(EXTRACTOR_DIR, exist_ok=True)
os.makedirs(TEMP_BASE, exist_ok=True)

# set up logging
logger = logging.getLogger("tender_pipeline")
logger.setLevel(LOG_LEVEL)
fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
fh = logging.FileHandler(LOG_FILE)
fh.setFormatter(fmt)
logger.addHandler(fh)
ch = logging.StreamHandler()
ch.setFormatter(fmt)
logger.addHandler(ch)

def safe_name(text: str) -> str:
    text = (text or "").strip()
    return re.sub(r'[^A-Za-z0-9_-]', '_', text)

def db_connect():
    return mysql.connector.connect(**DB_CONFIG)

def fetch_pending_tenders(limit: int = FETCH_LIMIT):
    """Return list of dicts: [{'bid_number':..., 'detail_url':...}, ...]"""
    conn = None
    try:
        conn = db_connect()
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT bid_number, detail_url
            FROM gem_tenders
            WHERE bid_number NOT IN (SELECT bid_number FROM gem_tender_docs)
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        cur.close()
        return rows or []
    except Exception as e:
        logger.exception("DB fetch error: %s", e)
        return []
    finally:
        if conn:
            conn.close()

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
        logger.info("DB saved for %s", bid_number)
    except Exception:
        logger.exception("Failed to save DB record for %s", bid_number)
    finally:
        if conn:
            conn.close()

# ---------------------------
# PDF URL extraction (Playwright)
# ---------------------------
def extract_pdf_url(detail_url: str, worker_name: str, retry: int = 2, retry_delay: float = 1.0) -> Optional[str]:
    """Return PDF URL or None. Retries on transient errors."""
    detail_url = (detail_url or "").strip()
    if not detail_url:
        return None

    # quick cases
    if detail_url.lower().endswith(".pdf"):
        return detail_url
    if "showbidDocument" in detail_url:
        return detail_url

    last_exc = None
    for attempt in range(1, retry+2):
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                try:
                    page.goto(detail_url, timeout=0, wait_until="domcontentloaded")
                except Exception as e:
                    # page probably not reachable; still try to close and return None
                    logger.warning("[%s] Playwright page.goto failed attempt %d for %s: %s", worker_name, attempt, detail_url, e)
                    browser.close()
                    last_exc = e
                    time.sleep(retry_delay)
                    continue

                links = page.query_selector_all("a")
                for link in links:
                    href = link.get_attribute("href")
                    if href and ".pdf" in href.lower():
                        if href.startswith("/"):
                            # common for gem
                            href = urllib_base(detail_url) + href
                        browser.close()
                        return href
                browser.close()
                return None
        except Exception as e:
            last_exc = e
            logger.warning("[%s] extract_pdf_url attempt %d failed: %s", worker_name, attempt, e)
            time.sleep(retry_delay)
            continue
    logger.error("[%s] extract_pdf_url all attempts failed for %s: %s", worker_name, detail_url, last_exc)
    return None

def urllib_base(url: str) -> str:
    # returns scheme+netloc, e.g. https://bidplus.gem.gov.in
    try:
        from urllib.parse import urlparse
        p = urlparse(url)
        return f"{p.scheme}://{p.netloc}"
    except Exception:
        return ""

# ---------------------------
# Download with retries
# ---------------------------
def download_pdf(pdf_url: str, dest_path: Path, worker_name: str, timeout: int = PDF_DOWNLOAD_TIMEOUT, max_retries: int = DOWNLOAD_MAX_RETRIES) -> Optional[Path]:
    """Download a pdf_url into dest_path. Returns dest_path on success else None."""
    session = requests.Session()
    session.headers.update({"User-Agent": "tender_pipeline_bot/1.0"})
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(pdf_url, stream=True, timeout=timeout)
            r.raise_for_status()
            tmp_path = dest_path.with_suffix(dest_path.suffix + ".part")
            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            os.replace(tmp_path, dest_path)  # atomic
            if dest_path.exists() and dest_path.stat().st_size >= MIN_PDF_SIZE_BYTES:
                logger.info("[%s] Downloaded %s -> %s (size=%d)", worker_name, pdf_url, dest_path, dest_path.stat().st_size)
                return dest_path
            else:
                last_err = f"file too small {dest_path.stat().st_size if dest_path.exists() else 'no-file'}"
                logger.warning("[%s] Downloaded file too small attempt %d: %s", worker_name, attempt, last_err)
        except Exception as e:
            last_err = str(e)
            logger.warning("[%s] Download attempt %d failed for %s: %s", worker_name, attempt, pdf_url, e)
            time.sleep(0.5 * attempt)
            continue
    logger.error("[%s] All download attempts failed for %s: %s", worker_name, pdf_url, last_err)
    return None

# ---------------------------
# Run extractor subprocess
# ---------------------------
def run_extractor(temp_pdf_dir: Path, temp_out_dir: Path, worker_name: str, extractor_workers: int = EXTRACTOR_WORKERS_PER_JOB, timeout: int = EXTRACTOR_RUN_TIMEOUT) -> bool:
    """
    Run extractor CLI with --skip-download. Returns True on success (some JSON produced)
    """
    if not EXTRACTOR_PATH.exists():
        logger.error("[%s] Extractor not found at %s", worker_name, EXTRACTOR_PATH)
        return False

    cmd = [
        str(sys_executable()),
        str(EXTRACTOR_PATH),
        "--skip-download",
        "--pdf-folder", str(temp_pdf_dir),
        "--out-folder", str(temp_out_dir),
        "--extract-workers", str(max(1, int(extractor_workers)))
    ]
    logger.info("[%s] Running extractor: %s", worker_name, " ".join(cmd))
    try:
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout, text=True)
        if proc.returncode != 0:
            logger.error("[%s] Extractor failed (rc=%s). stdout: %s stderr: %s", worker_name, proc.returncode, proc.stdout, proc.stderr)
        else:
            logger.info("[%s] Extractor finished. stdout len=%d stderr len=%d", worker_name, len(proc.stdout or ""), len(proc.stderr or ""))
        # success judged by presence of JSON files in temp_out_dir
        jsons = list(temp_out_dir.glob("*.json"))
        if jsons:
            return True
        else:
            logger.warning("[%s] Extractor produced no JSON files in %s", worker_name, temp_out_dir)
            return False
    except subprocess.TimeoutExpired:
        logger.exception("[%s] Extractor timed out after %s seconds", worker_name, timeout)
        return False
    except Exception:
        logger.exception("[%s] Running extractor raised exception", worker_name)
        return False

def sys_executable() -> str:
    # returns python executable path used to run the extractor
    import sys
    return sys.executable

# ---------------------------
# Worker job
# ---------------------------
def process_tender_job(row: dict, worker_idx: int):
    """
    Process one tender end-to-end.
    row: dict with keys 'bid_number' and 'detail_url'
    worker_idx: small int (1..N) used to create isolated temp dirs and logging context
    """
    worker_name = f"W{worker_idx}"
    bid = row.get("bid_number")
    detail_url = row.get("detail_url")
    logger.info("[%s] Starting job for %s", worker_name, bid)

    safe_bid = safe_name(bid or "unknown")
    # worker-specific temp folders
    temp_pdf_dir = TEMP_BASE / f"pdf_{worker_name}"
    temp_out_dir = TEMP_BASE / f"out_{worker_name}"

    # ensure clean temp dirs
    try:
        if temp_pdf_dir.exists():
            shutil.rmtree(temp_pdf_dir, ignore_errors=True)
        if temp_out_dir.exists():
            shutil.rmtree(temp_out_dir, ignore_errors=True)
        temp_pdf_dir.mkdir(parents=True, exist_ok=True)
        temp_out_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        logger.exception("[%s] Could not prepare temp folders", worker_name)
        return False

    try:
        # 1) extract pdf url (with small retries)
        pdf_url = extract_pdf_url(detail_url, worker_name, retry=2)
        if not pdf_url:
            logger.error("[%s] No PDF URL found for %s", worker_name, detail_url)
            return False
        logger.info("[%s] PDF URL: %s", worker_name, pdf_url)

        # 2) download pdf into worker temp folder (use filename from url or safe_bid if missing)
        # choose file name: safe_bid.pdf to make downstream predictable
        pdf_filename = safe_bid + ".pdf"
        dest_path = temp_pdf_dir / pdf_filename

        downloaded = download_pdf(pdf_url, dest_path, worker_name, timeout=PDF_DOWNLOAD_TIMEOUT, max_retries=DOWNLOAD_MAX_RETRIES)
        if not downloaded:
            logger.error("[%s] Failed to download PDF for %s", worker_name, bid)
            return False

        # 3) optionally copy to persistent PDF_DIR (optional)
        try:
            persistent_path = PDF_DIR / dest_path.name
            shutil.copy2(dest_path, persistent_path)
        except Exception:
            logger.debug("[%s] Could not copy to persistent PDF_DIR (non-fatal)", worker_name)

        # 4) run extractor CLI
        ok = run_extractor(temp_pdf_dir, temp_out_dir, worker_name, extractor_workers=EXTRACTOR_WORKERS_PER_JOB, timeout=EXTRACTOR_RUN_TIMEOUT)
        if not ok:
            logger.error("[%s] Extractor failed for %s", worker_name, dest_path)
            return False

        # 5) find produced JSON. Prefer safe_bid.json, else if only one json present take that.
        expected = temp_out_dir / (safe_bid + ".json")
        json_path = None
        if expected.exists() and expected.stat().st_size > 100:
            json_path = expected
        else:
            # find any json in folder
            json_files = [p for p in temp_out_dir.glob("*.json") if p.is_file() and p.stat().st_size > 100]
            if len(json_files) == 1:
                json_path = json_files[0]
            elif len(json_files) > 1:
                # try to find file whose basename matches original pdf basename
                pdf_base = dest_path.stem
                match = next((p for p in json_files if p.stem == pdf_base), None)
                json_path = match or json_files[0]
        if not json_path:
            logger.error("[%s] No valid JSON found after extraction for %s", worker_name, bid)
            return False

        # 6) move JSON to global OUTPUT folder using safe name
        final_json_name = safe_bid + ".json"
        final_json_path = JSON_DIR / final_json_name
        try:
            shutil.move(str(json_path), str(final_json_path))
        except Exception:
            # try copy & unlink fallback
            try:
                shutil.copy2(str(json_path), str(final_json_path))
                json_path.unlink(missing_ok=True)
            except Exception:
                logger.exception("[%s] Could not move/copy JSON into OUTPUT", worker_name)
                return False

        # 7) save DB record
        save_doc_record(bid, detail_url, pdf_url, str(dest_path), str(final_json_path))

        logger.info("[%s] Completed job for %s -> %s", worker_name, bid, final_json_path)
        return True
    except Exception:
        logger.exception("[%s] Unexpected error processing %s", worker_name, bid)
        return False
    finally:
        # cleanup worker temp dirs (best-effort)
        try:
            if temp_pdf_dir.exists():
                shutil.rmtree(temp_pdf_dir, ignore_errors=True)
            if temp_out_dir.exists():
                shutil.rmtree(temp_out_dir, ignore_errors=True)
        except Exception:
            logger.debug("[%s] Failed to cleanup temp dirs", worker_name)

# ---------------------------
# Coordinator: run a batch using ThreadPoolExecutor
# ---------------------------
def run_batch(global_workers: int = GLOBAL_WORKERS, fetch_limit: int = FETCH_LIMIT):
    rows = fetch_pending_tenders(limit=fetch_limit)
    if not rows:
        logger.info("No pending tenders found.")
        return

    logger.info("Fetched %d pending tenders; scheduling up to %d worker threads", len(rows), global_workers)
    results = []
    with ThreadPoolExecutor(max_workers=global_workers) as exe:
        futures = {}
        # schedule up to len(rows) tasks
        for idx, row in enumerate(rows, start=1):
            # worker index (wrap around global_workers for naming if many tasks)
            worker_idx = idx % global_workers if (idx % global_workers) != 0 else global_workers
            fut = exe.submit(process_tender_job, row, worker_idx)
            futures[fut] = (row.get("bid_number"), worker_idx)
        for fut in as_completed(futures):
            bid, worker_idx = futures[fut]
            try:
                ok = fut.result()
                results.append((bid, ok))
                logger.info("Job finished: bid=%s worker=%s status=%s", bid, worker_idx, ok)
            except Exception:
                logger.exception("Unhandled exception when processing bid=%s worker=%s", bid, worker_idx)
                results.append((bid, False))
    # summary
    succeeded = sum(1 for _, ok in results if ok)
    failed = len(results) - succeeded
    logger.info("Batch complete. Total=%d Success=%d Failed=%d", len(results), succeeded, failed)

# ---------------------------
# Graceful shutdown handling
# ---------------------------
SHUTDOWN = False
def _signal_handler(signum, frame):
    global SHUTDOWN
    logger.info("Received signal %s. Will finish current batch then exit.", signum)
    SHUTDOWN = True

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# ---------------------------
# Main loop
# ---------------------------
def main_loop(global_workers: int = GLOBAL_WORKERS, fetch_limit: int = FETCH_LIMIT, sleep_seconds: int = SLEEP_BETWEEN_BATCHES):
    logger.info("Starting tender pipeline. workers=%d fetch_limit=%d", global_workers, fetch_limit)
    while True:
        if SHUTDOWN:
            logger.info("Shutdown flag set - exiting main loop.")
            break
        try:
            run_batch(global_workers=global_workers, fetch_limit=fetch_limit)
        except Exception:
            logger.exception("Uncaught exception during run_batch")
        # sleep between batches, but break early on shutdown
        for _ in range(int(sleep_seconds)):
            if SHUTDOWN:
                break
            time.sleep(1)
        if SHUTDOWN:
            logger.info("Shutdown detected after sleep - breaking")
            break
    logger.info("Pipeline exiting cleanly.")

# ---------------------------
# CLI arg parsing
# ---------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Tender pipeline: process pending tenders in parallel.")
    p.add_argument("--workers", type=int, default=GLOBAL_WORKERS, help="Number of parallel worker threads.")
    p.add_argument("--fetch-limit", type=int, default=FETCH_LIMIT, help="How many pending tenders to fetch per batch.")
    p.add_argument("--sleep", type=int, default=SLEEP_BETWEEN_BATCHES, help="Seconds to sleep between batches.")
    p.add_argument("--log-file", default=str(LOG_FILE), help="Log file path.")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    # update logger file handler if user specified different log path
    if args.log_file and args.log_file != str(LOG_FILE):
        for h in list(logger.handlers):
            logger.removeHandler(h)
        fh = logging.FileHandler(args.log_file)
        fh.setFormatter(fmt)
        logger.addHandler(fh)
        logger.addHandler(ch)
    try:
        main_loop(global_workers=args.workers, fetch_limit=args.fetch_limit, sleep_seconds=args.sleep)
    except Exception:
        logger.exception("Fatal error in main")
        raise
