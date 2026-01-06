#!/usr/bin/env python3
# run_realtime.py
"""
Production-ready real-time scraper + relevancy pipeline.

Features:
 - Playwright-based scraping (headless)
 - ML relevancy (joblib model)
 - Keyword matcher (existing Matcher)
 - Global relevancy (global_relevancy.predict) -> stored in gem_tenders + Main_Relevency
 - Batched upserts to gem_tenders and batched inserts to Main_Relevency
 - CSV snapshots
 - Graceful shutdown
"""

import os

# Set environment variables to prevent segmentation faults caused by OpenMP/MKL conflicts
# This must be done BEFORE importing numpy/torch/pandas
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["TOKENIZERS_PARALLELISM"] = "false"

import asyncio
import json
import os
import re
import signal
import sys
import time
import faulthandler

faulthandler.enable()

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import List, Tuple, Dict, Any, Optional

import mysql.connector
import pandas as pd
from playwright.async_api import async_playwright
from tasks import process_tender, calculate_relevancy

# ---------------------------
# CONFIG (tweak as necessary)
# ---------------------------
BASE_URL = "https://bidplus.gem.gov.in"
QUEUE_MAXSIZE = 20000
BATCH_SIZE = 10
BATCH_TIMEOUT = 5.0  # seconds
CSV_SNAPSHOT_EVERY = 600  # seconds
LOG_FILE = "realtime_scraper.log"

# Output directories for JSON files
OUTPUT_DIR = "OUTPUT"
REP_DIR = os.path.join(OUTPUT_DIR, "Representation")
CORR_DIR = os.path.join(OUTPUT_DIR, "Corrigendum")

# ThreadPool for DB operations (use multiple threads for parallel writes)
DB_WORKER_THREADS = int(os.getenv("DB_WORKER_THREADS", "6"))

# DB config (override via env vars if you like)
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "tender_automation_with_ai"),
    "autocommit": False,
    "charset": "utf8mb4",
    "use_unicode": True,
    "use_pure": True,  # Force pure Python implementation to avoid C-extension conflicts
}

# Model / file paths (allow overrides)
MODEL_FILE = os.getenv("MODEL_FILE", "data/processed/relevancy_model.pkl")
VECT_FILE = os.getenv("VECT_FILE", "data/processed/vectorizer.pkl")
GLOBAL_RELEVANCY_DIR = os.path.join(os.path.dirname(__file__), "relevency", "scripts")

# Logging
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
logger = logging.getLogger("realtime")

# ---------------------------
# Load ML relevance model (joblib)
# ---------------------------
import joblib

try:
    model = joblib.load(MODEL_FILE)
    vectorizer = joblib.load(VECT_FILE)
    logger.info("Loaded ML relevancy model and vectorizer.")
except Exception:
    logger.exception("Failed to load ML model or vectorizer. Exiting.")
    raise


def clean_text(txt: Optional[str]) -> str:
    if txt is None:
        return ""
    txt = str(txt).lower()
    txt = re.sub(r"[^a-z0-9\s/-]", " ", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def predict_relevance(text: str) -> Tuple[int, float]:
    """Return (pred_label, probability_of_positive)."""
    clean = clean_text(text)
    vec = vectorizer.transform([clean])
    pred = int(model.predict(vec)[0])
    proba = float(model.predict_proba(vec)[0][1])
    return pred, proba


# ---------------------------
# Keyword MATCHER (existing)
# ---------------------------
from app.matching.datastore import KeywordStore
from app.matching.matcher import Matcher

BACKEND_DIR = os.path.join(os.path.dirname(__file__), "app")
DATA_DIR = os.path.join(BACKEND_DIR, "data")
DIAGNOSTIC_CSV = os.path.join(DATA_DIR, "keywords_diagnostic.csv")
ENDO_CSV = os.path.join(DATA_DIR, "keywords_endo.csv")
OVERALL_CSV = os.path.join(DATA_DIR, "keywords_sheet1.csv")

STORE = KeywordStore()
if os.path.exists(DIAGNOSTIC_CSV):
    try:
        STORE.load_csv(DIAGNOSTIC_CSV, category="Diagnostic")
        logger.info(f"Loaded Diagnostic keywords: {DIAGNOSTIC_CSV}")
    except Exception:
        logger.exception("Failed to load Diagnostic CSV.")
else:
    logger.warning(f"Diagnostic CSV not found at {DIAGNOSTIC_CSV}")

if os.path.exists(ENDO_CSV):
    try:
        STORE.load_csv(ENDO_CSV, category="Endo")
        logger.info(f"Loaded Endo keywords: {ENDO_CSV}")
    except Exception:
        logger.exception("Failed to load Endo CSV.")
else:
    logger.warning(f"Endo CSV not found at {ENDO_CSV}")

if os.path.exists(OVERALL_CSV):
    try:
        STORE.load_csv(OVERALL_CSV, category="Overall")
        logger.info(f"Loaded Overall keywords: {OVERALL_CSV}")
    except Exception:
        logger.exception("Failed to load Overall CSV.")
else:
    logger.warning(f"Overall CSV not found at {OVERALL_CSV}")

MATCHER = Matcher(STORE)
logger.info("Matcher initialized with loaded KeywordStore.")

# ---------------------------
# Load global_relevancy.predict
# ---------------------------
# Ensure the directory is on sys.path so import works
if GLOBAL_RELEVANCY_DIR not in sys.path:
    sys.path.append(GLOBAL_RELEVANCY_DIR)

try:
    from global_relevancy import predict as global_predict  # type: ignore

    logger.info("Loaded global_relevancy.predict successfully.")
except Exception:
    logger.exception("Failed to import global_relevancy.predict. Exiting.")
    raise

# ---------------------------
# SQL - Extended UPSERT for gem_tenders (new columns appended)
# Order must match tuples appended to rows
# ---------------------------
UPSERT_SQL = """
INSERT INTO gem_tenders
  (page_no, bid_number, detail_url, items, quantity, department, start_date, end_date,
   relevance, relevance_score, match_count, match_relevency, matches, matches_status,
   relevency_result, main_relevency_score, dept, ra_no, ra_url, Representation_json, Corrigendum_json)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
  page_no = VALUES(page_no),
  detail_url = VALUES(detail_url),
  quantity = VALUES(quantity),
  department = VALUES(department),
  start_date = VALUES(start_date),
  end_date = VALUES(end_date),
  relevance = VALUES(relevance),
  relevance_score = VALUES(relevance_score),
  match_count = VALUES(match_count),
  match_relevency = VALUES(match_relevency),
  matches = VALUES(matches),
  matches_status = VALUES(matches_status),
  relevency_result = VALUES(relevency_result),
  main_relevency_score = VALUES(main_relevency_score),
  dept = VALUES(dept),
  ra_no = VALUES(ra_no),
  ra_url = VALUES(ra_url),
  Representation_json = VALUES(Representation_json),
  Corrigendum_json = VALUES(Corrigendum_json)
;
"""

# SQL for inserting into Main_Relevency (batched)
MAIN_RELEVANCY_INSERT_SQL = """
INSERT INTO Main_Relevency
  (bid_number, query, detected_category, relevancy_score, relevant, best_match, top_matches)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

# ---------------------------
# GLOBALS
# ---------------------------
SHUTDOWN = False


# ---------------------------
# DB CONNECTION HELPERS
# ---------------------------
def db_connect():
    # create new connection per thread
    return mysql.connector.connect(**DB_CONFIG)


def db_execute_many_upsert(rows: List[Tuple[Any, ...]]) -> int:
    """Execute UPSERTs into gem_tenders. Returns number of rows processed."""
    if not rows:
        return 0
    conn = db_connect()
    cur = conn.cursor()
    try:
        cur.executemany(UPSERT_SQL, rows)
        conn.commit()
        return len(rows)
    except Exception:
        conn.rollback()
        logger.exception("db_execute_many_upsert failed.")
        raise
    finally:
        cur.close()
        conn.close()


def db_insert_main_relevancy(rows: List[Tuple[Any, ...]]) -> int:
    """Insert rows into Main_Relevency (bid_number, query, detected_category, relevancy_score, relevant, best_match, top_matches)."""
    if not rows:
        return 0
    conn = db_connect()
    cur = conn.cursor()
    try:
        if rows:
            logger.info(f"Attempting to insert {len(rows)} rows into Main_Relevency...")
        cur.executemany(MAIN_RELEVANCY_INSERT_SQL, rows)
        conn.commit()
        if rows:
            logger.info(f"Successfully inserted {len(rows)} rows into Main_Relevency.")
        return len(rows)
    except Exception as e:
        conn.rollback()
        logger.exception(f"db_insert_main_relevancy failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


# ---------------------------
# SCRAPER UTILITIES
# ---------------------------
async def apply_sorting(page):
    logger.info("Applying sorting: Bid Start Date -> Latest")
    dropdown_btn = await page.query_selector("#currentSort")
    if dropdown_btn:
        await dropdown_btn.click()
        await asyncio.sleep(0.5)
    sort_option = await page.query_selector("#Bid-Start-Date-Latest")
    if sort_option:
        await sort_option.click()
        await asyncio.sleep(1.5)
    else:
        logger.warning("Sorting option not found.")


async def extract_total_counts(page) -> Tuple[int, int]:
    await page.goto(f"{BASE_URL}/all-bids", timeout=0, wait_until="networkidle")
    await asyncio.sleep(1.5)
    await apply_sorting(page)

    total_records = 0
    total_pages = 1

    records_el = await page.query_selector("span.pos-bottom")
    if records_el:
        txt = await records_el.inner_text()
        m = re.search(r"of\s+(\d+)\s+records", txt)
        if m:
            total_records = int(m.group(1))

    last_page_el = await page.query_selector(
        "#light-pagination a.page-link:nth-last-child(2)"
    )
    if last_page_el:
        t = (await last_page_el.inner_text()).strip()
        if t.isdigit():
            total_pages = int(t)

    return total_records, total_pages


def safe_json_dumps(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        try:
            return json.dumps(str(obj), ensure_ascii=False)
        except Exception:
            return '""'


async def extract_modal_data(page, trigger_element=None, js_trigger=None):
    """
    Triggers modal via element click OR JS code, waits for modal, extracts data.
    """
    try:
        # Trigger phase
        if js_trigger:
            logger.info(f"Triggering modal via JS: {js_trigger}")
            await page.evaluate(js_trigger)
        elif trigger_element:
            # Click phase
            # 1. Try standard force click
            try:
                await trigger_element.click(force=True, timeout=2000)
            except:
                pass
            # 2. Try JS click
            await page.evaluate("el => el.click()", trigger_element)
            
        # Wait for modal - 15s timeout
        try:
            modal = await page.wait_for_selector("div.modal:visible", state="visible", timeout=6000)
        except:
            if trigger_element and not js_trigger:
                # Retry click if modal not found yet
                logger.info("Modal not appearing, retrying click...")
                await page.evaluate("el => el.click()", trigger_element)
                modal = await page.wait_for_selector("div.modal:visible", state="visible", timeout=10000)
            else:
                return None
            
        if not modal:
            return None
            
        # Give it a moment to populate
        await asyncio.sleep(1.0)
        
        # Header/Title
        title = ""
        # Try finding a header inside the modal
        header_el = await modal.query_selector(".modal-header, .modal-title, h4")
        if header_el:
            title = (await header_el.inner_text()).strip()
            
        content_data = []
        
        # Custom extraction for Corrigendum based on provided HTML structure:
        # <div class="well"><div class="col-block">...</div>...</div>
        if "Corrigendum" in title or "View(s)" in title: # Title is "Corrigendum" or "View(s)"
            wells = await modal.query_selector_all("div.well")
            if wells:
                for w in wells:
                    # Each well is a block of changes
                    # Extract text from all col-blocks
                    cols = await w.query_selector_all("div.col-block")
                    well_data = {}
                    full_text = []
                    for c in cols:
                        # Parsing logic:
                        # <div class="col-block"> <span id="..."><strong>Modified On: </strong><span>DATE</span></span></div>
                        # <div class="col-block">Bid extended to <strong>DATE</strong></div>
                        
                        txt = (await c.inner_text()).strip()
                        txt = re.sub(r'\s+', ' ', txt) # normalize spaces
                        if txt:
                            full_text.append(txt)
                            
                    if full_text:
                        # Join them for a stored entry
                        content_data.append({"change": " | ".join(full_text)})
        
        # Fallback if no wells found (or standard table extraction)
        if not content_data:
            # Tables
            tables = await modal.query_selector_all("table")
            for tbl in tables:
                rows = await tbl.query_selector_all("tr")
                # attempt to detect headers if first row is th
                # For simplicity, we just dump rows.
                # But let's try to match user requested format if 2 or 3 columns
                for r in rows:
                    cols = await r.query_selector_all("td, th")
                    txts = [(await c.inner_text()).strip() for c in cols]
                    
                    if not txts:
                        continue
                        
                    if len(txts) == 2:
                        content_data.append({"label": txts[0], "value": txts[1]})
                    elif len(txts) == 3:
                         # Corrigendum style?
                        content_data.append({"field": txts[0], "old": txts[1], "new": txts[2]})
                    else:
                        content_data.append({"row": txts})
        
        # If no table rows found, maybe just paragraphs?
        if not content_data:
            ps = await modal.query_selector_all("p")
            for p in ps:
                t = (await p.inner_text()).strip()
                if t and "Corrigendum Details" not in t: # Skip header p
                    content_data.append({"text": t})

        result = {
            "title": title,
            "content": content_data if "Representation" in title or "Corrigendum" not in title else [],
            "changes": content_data if "Corrigendum" in title else []
        }
        
        # Fallback if title is empty or ambiguous, put in content
        if not result["content"] and not result["changes"]:
             result["content"] = content_data

        # Close
        close_btn = await modal.query_selector("button.close, button[data-dismiss='modal'], .modal-footer button.btn-default")
        if close_btn:
            await close_btn.click()
        else:
            await page.keyboard.press("Escape")
            
        await page.wait_for_selector("div.modal:visible", state="hidden", timeout=4000)
        
        return json.dumps(result, ensure_ascii=False)

    except Exception as e:
        logger.warning(f"Modal extraction error: {e}")
        # try escape
        try:
            await page.keyboard.press("Escape")
        except:
            pass
        return None


async def scrape_single_page_to_rows(page, page_no: int):
    """
    Scrape visible cards on `page` and return:
      - gem_rows: list of tuples matching UPSERT_SQL order
      - main_rows: list of tuples matching MAIN_RELEVANCY_INSERT_SQL
    """
    # scroll a bit to ensure lazy elements load
    for _ in range(3):
        await page.mouse.wheel(0, 2000)
        await asyncio.sleep(0.2)

    cards = await page.query_selector_all("div.card")
    gem_rows = []
    main_rows = []

    for c in cards:
        try:
            bid_link = await c.query_selector(".block_header a.bid_no_hover")
            bid_no = (await bid_link.inner_text()).strip() if bid_link else ""
            if not bid_no:
                continue

            detail_url = (
                BASE_URL + "/" + (await bid_link.get_attribute("href")).lstrip("/")
            )

            item_el = await c.query_selector(".card-body .col-md-4 .row:nth-child(1) a")
            items = (await item_el.inner_text()).strip() if item_el else ""

            qty_el = await c.query_selector(".card-body .col-md-4 .row:nth-child(2)")
            quantity = (
                (await qty_el.inner_text()).replace("Quantity:", "").strip()
                if qty_el
                else ""
            )

            dept_el = await c.query_selector(".card-body .col-md-5 .row:nth-child(2)")
            department = (await dept_el.inner_text()).strip() if dept_el else ""

            start_el = await c.query_selector("span.start_date")
            start_date = (await start_el.inner_text()).strip() if start_el else ""

            end_el = await c.query_selector("span.end_date")
            end_date = (await end_el.inner_text()).strip() if end_el else ""

            # ---- AI relevance prediction (ML classifier) ----
            try:
                pred, score = predict_relevance(items)
            except Exception:
                logger.exception(
                    "predict_relevance failed; defaulting to not relevant."
                )
                pred, score = 0, 0.0

            # ---- KEYWORD MATCHER ----
            try:
                # Optimized Matcher uses .analyze() and returns a dict
                # Returns: { "relevant": bool, "score_pct": int, "matches": list, "matched_count": int, ... }
                matcher_res = MATCHER.analyze(items)
                
                match_count = matcher_res.get("matched_count", 0)
                matches_list = matcher_res.get("matches", [])
                # Logic for status/text
                if matcher_res.get("relevant"):
                    match_relevency = "High"
                    matches_status = "Relevant"
                elif match_count > 0:
                    match_relevency = "Medium"
                    matches_status = "Partial"
                else:
                    match_relevency = "Low"
                    matches_status = "None"
                    
            except Exception:
                logger.exception("Matcher analyze failed; falling back to 0 matches.")
                match_count = 0
                matches_list = []
                match_relevency = "Low"
                matches_status = "Error"

            try:
                matches_json = safe_json_dumps(matches_list)
            except Exception:
                matches_json = "[]"

            # ---- GLOBAL RELEVANCY (embedding + special models) ----
            try:
                # global_predict(text) -> Returns dict with 'results' list (supports multi-query)
                # We assume 'items' is treated as a single query or auto-split.
                g_res = global_predict(items)
                
                # Check results
                if g_res.get("results"):
                    # Take the first result (primary)
                    first_res = g_res["results"][0]
                    global_score = first_res.get("relevancy_score", 0.0)
                    global_relevant = 1 if first_res.get("relevant") else 0
                    global_dept = first_res.get("detected_category") or ""
                    
                    best_match_data = first_res.get("best_match", {})
                    best_match_json = safe_json_dumps(best_match_data)
                    
                    top_matches_data = first_res.get("top_matches", [])
                    top_matches_json = safe_json_dumps(top_matches_data)
                else:
                    global_score = 0.0
                    global_relevant = 0
                    global_dept = ""
                    best_match_json = "{}"
                    top_matches_json = "[]"
                    
            except Exception:
                logger.exception("global_predict failed; setting defaults.")
                global_score = 0.0
                global_relevant = 0
                global_dept = ""
                best_match_json = "{}"
                top_matches_json = "[]"

            # Variable used later:
            relevency_result = global_relevant

            # ---- RA NO & URL EXTRACTION ----
            ra_no = ""
            ra_url = ""
            try:
                # User pattern:
                # <p class="bid_no pull-left"> <span class="bid_title">RA NO:&nbsp;</span> <a ...> ... </a> </p>
                # We look for the p.bid_no that has span.bid_title with text "RA NO:"
                
                # Check directly in the card content
                ra_p = await c.query_selector("p.bid_no:has(span.bid_title:text('RA NO:'))")
                if ra_p:
                    ra_link = await ra_p.query_selector("a")
                    if ra_link:
                        ra_no = (await ra_link.inner_text()).strip()
                        href = await ra_link.get_attribute("href")
                        if href:
                            if href.startswith("/"):
                                ra_url = BASE_URL + href
                            else:
                                ra_url = href
            except Exception:
                logger.exception("Error extracting RA NO/URL")

            # ---- Representation / Corrigendum ----
            # User Step 1: Click "View Corrigendum/Representation" toggle if present (e.g. Back button or Expand)
            try:
                # Matches <a>...View Corrigendum/Representation</a>
                toggle_btn = await c.query_selector("a:has-text('View Corrigendum/Representation')")
                if toggle_btn:
                    # logger.info("Found 'View Corrigendum/Representation' link. Clicking it...")
                    await toggle_btn.click()
                    await asyncio.sleep(2) # Wait for UI to update
            except Exception as e:
                # logger.warning(f"Error clicking toggle: {e}")
                pass

            rep_json = None
            corr_json = None
            try:
                # Representation
                # Pattern: <a>View Representation</a> often inside <span onclick="...">
                rep_js_trigger = None
                rep_element = None
                try:
                    # Find elements with text "View Representation"
                    # We check 'a' and 'span'
                    candidates = await c.query_selector_all("a, span")
                    for cand in candidates:
                        txt = (await cand.inner_text()) or ""
                        if "View Representation" in txt:
                            # Check if this element has onclick
                            oc = await cand.get_attribute("onclick")
                            if oc:
                                rep_js_trigger = oc
                                logger.info(f"Found Representation JS trigger on element: {oc}")
                                break
                            
                            # Check parent
                            parent_handles = await cand.xpath("..")
                            if parent_handles:
                                parent = parent_handles[0]
                                oc_p = await parent.get_attribute("onclick")
                                if oc_p:
                                    rep_js_trigger = oc_p
                                    logger.info(f"Found Representation JS trigger on parent: {oc_p}")
                                    break
                            
                            # If no onclick found yet, keep this as candidate to click directly
                            if not rep_element:
                                rep_element = cand
                                
                except Exception as e:
                    logger.warning(f"Error finding Representation trigger: {e}")
                
                if rep_js_trigger:
                     rep_json = await extract_modal_data(page, js_trigger=rep_js_trigger)
                elif rep_element:
                     # logger.info("No JS trigger found for Representation, trying direct click.")
                     rep_json = await extract_modal_data(page, trigger_element=rep_element)
                
                if rep_json:
                     try:
                         safe_bid = re.sub(r'[^A-Za-z0-9_-]', '_', bid_no)
                         fname = os.path.join(REP_DIR, f"{safe_bid}.json")
                         with open(fname, "w", encoding="utf-8") as f:
                             f.write(rep_json)
                         logger.info(f"Saved Representation JSON for {bid_no}")
                     except Exception:
                         logger.exception(f"Failed to save Representation JSON for {bid_no}")

                # Corrigendum
                # User structure: <span onclick="view_corrigendum_modal(8316925)" data-bid="8316925">
                # We extract the `data-bid` and trigger it directly via JS.
                corr_js_trigger = None
                
                try:
                    # Find span with 'data-bid' and 'View Corrigendum'
                    spans = await c.query_selector_all("span[data-bid]")
                    target_span = None
                    for s in spans:
                        txt = (await s.inner_text()) or ""
                        if "View Corrigendum" in txt:
                            target_span = s
                            break
                    
                    if target_span:
                        data_bid = await target_span.get_attribute("data-bid")
                        if data_bid:
                            corr_js_trigger = f"view_corrigendum_modal('{data_bid}')"
                            # logger.info(f"Using JS trigger for Corrigendum: {corr_js_trigger}")
                except Exception as e:
                    pass
                    # logger.warning(f"Error preparing Corrigendum JS trigger: {e}")

                if corr_js_trigger:
                     # logger.info(f"Found View Corrigendum trigger for {bid_no}")
                     corr_json = await extract_modal_data(page, js_trigger=corr_js_trigger)
                     if corr_json:
                         try:
                             safe_bid = re.sub(r'[^A-Za-z0-9_-]', '_', bid_no)
                             fname = os.path.join(CORR_DIR, f"{safe_bid}.json")
                             with open(fname, "w", encoding="utf-8") as f:
                                 f.write(corr_json)
                             logger.info(f"Saved Corrigendum JSON for {bid_no}")
                         except Exception:
                             logger.exception(f"Failed to save Corrigendum JSON for {bid_no}")
            except Exception:
                logger.exception("Error extracting modal data")



            # Build gem_tenders tuple (must match UPSERT_SQL order)
            gem_row = (
                page_no,
                bid_no,
                detail_url,
                items,
                quantity,
                department,
                start_date,
                end_date,
                pred,
                score,
                match_count,
                match_relevency,
                matches_json,
                matches_status,
                global_relevant,  # relevency_result
                global_score,  # main_relevency_score
                global_dept,  # dept
                ra_no,
                ra_url,
                rep_json,
                corr_json,
            )
            gem_rows.append(gem_row)

            # Build Main_Relevency insert tuple:
            main_row = (
                bid_no,
                items,
                global_dept,
                global_score,
                global_relevant,
                best_match_json,
                top_matches_json,
            )

            main_rows.append(main_row)

        except Exception:
            logger.exception("Error scraping a card — skipping it.")
            continue

    return gem_rows, main_rows


# ---------------------------
# SCRAPER WORKER
# ---------------------------
async def scraper_worker(queue: asyncio.Queue, interval_seconds: int = 60):
    global SHUTDOWN
    logger.info("Scraper starting...")

    # Playwright context options for production:
    playwright_launch_args = {
        # "channel": "chrome",  <-- Commented out to use the default bundled Chromium
        "headless": True,
        "args": [
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
        ],
    }

    async with async_playwright() as p:
        browser = await p.chromium.launch(**playwright_launch_args)
        context = await browser.new_context()
        page = await context.new_page()

        while not SHUTDOWN:
            try:
                total_records, total_pages = await extract_total_counts(page)
                logger.info(
                    f"Found {total_records} records across {total_pages} pages."
                )

                page_no = 1
                gem_rows, main_rows = await scrape_single_page_to_rows(page, page_no)

                # enqueue gem_rows and main_rows together as a single item for consumer
                for g_row, m_row in zip(gem_rows, main_rows):
                    try:
                        # item is a tuple of (gem_row, main_row)
                        queue.put_nowait((g_row, m_row))
                    except asyncio.QueueFull:
                        await queue.put((g_row, m_row))

                while page_no < total_pages and not SHUTDOWN:
                    next_btn = await page.query_selector("#light-pagination a.next")
                    if not next_btn:
                        break
                    await next_btn.click()
                    await asyncio.sleep(1.2)

                    page_no += 1
                    gem_rows, main_rows = await scrape_single_page_to_rows(
                        page, page_no
                    )
                    for g_row, m_row in zip(gem_rows, main_rows):
                        try:
                            queue.put_nowait((g_row, m_row))
                        except asyncio.QueueFull:
                            await queue.put((g_row, m_row))

                # go back to listing & sleep
                await page.goto(
                    f"{BASE_URL}/all-bids", timeout=0, wait_until="networkidle"
                )
                await asyncio.sleep(0.5)

                # logger.info(f"Scraper sleeping for {interval_seconds}s.")
                # for _ in range(int(interval_seconds)):
                #     if SHUTDOWN:
                #         break
                #     await asyncio.sleep(1)

            except Exception:
                logger.exception("Scraper error — retrying in 10s.")
                await asyncio.sleep(10)

        logger.info("Scraper shutting down...")
        await browser.close()


# ---------------------------
# DB CONSUMER (non-blocking; uses executor)
# ---------------------------
async def db_consumer(queue: asyncio.Queue, executor: ThreadPoolExecutor):
    global SHUTDOWN
    logger.info("DB consumer starting...")
    buffer_gem: List[Tuple[Any, ...]] = []
    buffer_main: List[Tuple[Any, ...]] = []
    last_flush = time.time()

    async def flush_buffers():
        nonlocal buffer_gem, buffer_main, last_flush
        gem_to_commit = buffer_gem
        main_to_commit = buffer_main
        buffer_gem = []
        buffer_main = []

        # run both DB operations in the thread pool concurrently
        results = []
        try:
            loop = asyncio.get_event_loop()
            tasks = []
            if gem_to_commit:
                tasks.append(
                    loop.run_in_executor(
                        executor, db_execute_many_upsert, gem_to_commit
                    )
                )
            if main_to_commit:
                tasks.append(
                    loop.run_in_executor(
                        executor, db_insert_main_relevancy, main_to_commit
                    )
                )
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # handle exceptions in results
                committed = 0
                for r in results:
                    if isinstance(r, Exception):
                        logger.exception("DB worker raised an exception.")
                    else:
                        committed += int(r)
                logger.info(f"DB: committed approx {committed} rows (gem + main).")
                
                # --- FIRE AND FORGET REDIS TASKS ---
                # Now that DB commit is successful, queue the workers
                if gem_to_commit:
                    count_queued = 0
                    for row in gem_to_commit:
                        # row: (page_no, bid_number, detail_url, ...)
                        # bid_number is row[1], detail_url is row[2]
                        try:
                            # We send a dict as expected by process_tender task
                            payload = {
                                "bid_number": row[1], 
                                "detail_url": row[2]
                            }
                            process_tender.delay(payload)
                            
                            # Relevancy is now calculated locally in the loop above.
                            
                            count_queued += 1
                        except Exception as e:
                            logger.error(f"Failed to queue Redis task for {row[1]}: {e}")
                    logger.info(f"Queued {count_queued} tasks to Redis/Docker.")
                # -----------------------------------
        except Exception:
            logger.exception("Exception while flushing buffers.")
        finally:
            last_flush = time.time()

    last_snapshot = time.time()
    csv_rows_for_snapshot: List[Tuple[Any, ...]] = []

    while not (SHUTDOWN and queue.empty()):
        try:
            try:
                item = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                item = None

            if item:
                gem_row, main_row = item
                buffer_gem.append(gem_row)
                buffer_main.append(main_row)
                csv_rows_for_snapshot.append(gem_row)
                queue.task_done()

            # flush conditions
            if len(buffer_gem) >= BATCH_SIZE or len(buffer_main) >= BATCH_SIZE:
                await flush_buffers()

            if (time.time() - last_flush) >= BATCH_TIMEOUT and (
                buffer_gem or buffer_main
            ):
                await flush_buffers()

            if (
                time.time() - last_snapshot
            ) >= CSV_SNAPSHOT_EVERY and csv_rows_for_snapshot:
                ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                df = pd.DataFrame(
                    [
                        {
                            "page_no": r[0],
                            "bid_number": r[1],
                            "detail_url": r[2],
                            "items": r[3],
                            "quantity": r[4],
                            "department": r[5],
                            "start_date": r[6],
                            "end_date": r[7],
                            "relevance": r[8],
                            "relevance_score": r[9],
                            "match_count": r[10],
                            "match_relevency": r[11],
                            "matches": r[12],
                            "matches_status": r[13],
                            "relevency_result": r[14],
                            "main_relevency_score": r[15],
                            "dept": r[16],
                            "ra_no": r[17] if len(r) > 17 else "",
                            "ra_url": r[18] if len(r) > 18 else "",
                            "Representation_json": r[19] if len(r) > 19 else None,
                            "Corrigendum_json": r[20] if len(r) > 20 else None,
                        }
                        for r in csv_rows_for_snapshot
                    ]
                )

                snapshot_file = f"snapshot_{ts}.csv"
                df.to_csv(snapshot_file, index=False)
                logger.info(f"Snapshot saved: {snapshot_file}")
                csv_rows_for_snapshot = []
                last_snapshot = time.time()

        except Exception:
            logger.exception("DB consumer error.")
            await asyncio.sleep(1)

    # final flush
    try:
        if buffer_gem or buffer_main:
            await flush_buffers()
    except Exception:
        logger.exception("Final DB flush failed.")

    logger.info("DB consumer shutting down.")


# ---------------------------
# SIGNAL HANDLING
# ---------------------------
def handle_signal():
    global SHUTDOWN
    logger.info("Received stop signal — shutting down...")
    SHUTDOWN = True


# ---------------------------
# MAIN
# ---------------------------
async def main():
    queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    executor = ThreadPoolExecutor(max_workers=DB_WORKER_THREADS)

    # Ensure output directories exist
    os.makedirs(REP_DIR, exist_ok=True)
    os.makedirs(CORR_DIR, exist_ok=True)

    # Scraper interval in seconds (tunable)
    SCRAPER_INTERVAL = int(os.getenv("SCRAPER_INTERVAL", "300"))

    scraper_task = asyncio.create_task(
        scraper_worker(queue, interval_seconds=SCRAPER_INTERVAL)
    )
    consumer_task = asyncio.create_task(db_consumer(queue, executor))

    await asyncio.gather(scraper_task, consumer_task)


if __name__ == "__main__":
    try:
        # register signals
        for sig in ("SIGINT", "SIGTERM"):
            try:
                asyncio.get_event_loop().add_signal_handler(
                    getattr(signal, sig), handle_signal
                )
            except Exception:
                # not all event loops support add_signal_handler (Windows)
                pass

        asyncio.run(main())
    except KeyboardInterrupt:
        handle_signal()
        time.sleep(1)
        logger.info("Shutdown requested via KeyboardInterrupt")
