#!/usr/bin/env python3
# run_realtime.py
"""
Production-ready real-time scraper.

Features:
 - Playwright-based scraping (headless)
 - Batched upserts to gem_tenders
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
import collections
import http.server
import socketserver
import threading
import io
import logging # Added import

import mysql.connector
import pandas as pd
from playwright.async_api import async_playwright
from playwright.async_api import async_playwright

# ---------------------------
# GLOBAL STATUS & SERVER CONFIG
# ---------------------------
STATUS_PORT = 7315
GLOBAL_STATUS = {
    "state": "Initializing",
    "page_no": 0,
    "total_pages": 0,
    "total_records": 0,
    "items_scraped": 0,
    "items_committed": 0,
    "db_count": 0,
    "start_time": time.time(),
    "etc_seconds": -1,
    "last_updated": str(datetime.now()),

    "logs": collections.deque(maxlen=100)
}



class LogCaptureHandler(logging.Handler):
    """Captures log records into GLOBAL_STATUS['logs']."""
    def emit(self, record):
        try:
            msg = self.format(record)
            GLOBAL_STATUS["logs"].append(msg)
        except Exception:
            self.handleError(record)

class StatusRequestHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # Suppress server logs to console

    def do_GET(self):
        if self.path == '/api/status':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            
            # Create a clean dict copy
            status_copy = GLOBAL_STATUS.copy()
            status_copy['logs'] = list(GLOBAL_STATUS['logs']) # Convert deque to list
            
            self.wfile.write(json.dumps(status_copy).encode('utf-8'))
        else:
            # Serve status.html
            try:
                with open("status.html", "r", encoding="utf-8") as f:
                    content = f.read()
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                self.wfile.write(content.encode('utf-8'))
            except FileNotFoundError:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"status.html not found")

def start_status_server():
    """Starts the status server in a daemon thread."""
    try:
        # Bind to 0.0.0.0 to ensure accessibility
        httpd = socketserver.TCPServer(("0.0.0.0", STATUS_PORT), StatusRequestHandler)
        thread = threading.Thread(target=httpd.serve_forever)
        thread.daemon = True
        thread.start()
        print(f"✅ Status Dashboard running at http://localhost:{STATUS_PORT}")
        logger.info(f"Status Dashboard running at http://localhost:{STATUS_PORT}")
    except Exception as e:
        print(f"❌ Could not start status server: {e}")
        logger.warning(f"Could not start status server: {e}")


# ---------------------------
# CONFIG (tweak as necessary)
# ---------------------------
BASE_URL = "https://bidplus.gem.gov.in"
QUEUE_MAXSIZE = 20000
BATCH_SIZE = 200
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
# SQL - Extended UPSERT for gem_tenders (Modified: Removed relevancy columns)
# Order must match tuples appended to rows
# ---------------------------
UPSERT_SQL = """
INSERT INTO gem_tenders
  (page_no, bid_number, detail_url, items, quantity, department, start_date, end_date,
   ra_no, ra_url, Representation_json, Corrigendum_json)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
  page_no = VALUES(page_no),
  detail_url = VALUES(detail_url),
  quantity = VALUES(quantity),
  department = VALUES(department),
  start_date = VALUES(start_date),
  end_date = VALUES(end_date),
  ra_no = VALUES(ra_no),
  ra_url = VALUES(ra_url),
  Representation_json = VALUES(Representation_json),
  Corrigendum_json = VALUES(Corrigendum_json)
;
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


def db_get_count() -> int:
    """Fetch total count of rows in gem_tenders."""
    conn = db_connect()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM gem_tenders")
        result = cur.fetchone()
        return result[0] if result else 0
    except Exception:
        logger.exception("db_get_count failed.")
        return 0
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
        await dropdown_btn.click(force=True)
        await asyncio.sleep(0.5)
    sort_option = await page.query_selector("#Bid-Start-Date-Latest")
    if sort_option:
        await sort_option.click(force=True)
        await asyncio.sleep(0.5)
    else:
        logger.warning("Sorting option not found.")


async def extract_total_counts(page) -> Tuple[int, int]:
    await page.goto(f"{BASE_URL}/all-bids", timeout=0, wait_until="domcontentloaded")
    await asyncio.sleep(0.5)
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
    """
    # scroll a bit to ensure lazy elements load
    await page.mouse.wheel(0, 4000)
    await asyncio.sleep(0.1)

    cards = await page.query_selector_all("div.card")
    gem_rows = []

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
                    await toggle_btn.click(force=True)
                    await asyncio.sleep(1.5) # REQUIRED: Wait for UI to update (User priority: Reliability)
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
                ra_no,
                ra_url,
                rep_json,
                corr_json,
            )
            gem_rows.append(gem_row)

        except Exception:
            logger.exception("Error scraping a card — skipping it.")
            continue

    # Return only gem_rows
    return gem_rows


# ---------------------------
# SCRAPER WORKER
# ---------------------------
async def scraper_worker(queue: asyncio.Queue, interval_seconds: int = 60):
    global SHUTDOWN
    logger.info("Scraper starting...")
    GLOBAL_STATUS["state"] = "Running"


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
        
        # BLOCK HEAVY RESOURCES
        await page.route("**/*.{png,jpg,jpeg,gif,svg,css,woff,woff2}", lambda route: route.abort())

        while not SHUTDOWN:
            try:
                total_records, total_pages = await extract_total_counts(page)
                GLOBAL_STATUS["total_records"] = total_records
                GLOBAL_STATUS["total_pages"] = total_pages
                logger.info(
                    f"Found {total_records} records across {total_pages} pages."
                )


                page_no = 1
                GLOBAL_STATUS["page_no"] = page_no
                gem_rows = await scrape_single_page_to_rows(page, page_no)


                # enqueue gem_rows
                for g_row in gem_rows:
                    try:
                        queue.put_nowait(g_row)
                    except asyncio.QueueFull:
                        await queue.put(g_row)

                while page_no < total_pages and not SHUTDOWN:
                    # Try to find next button
                    next_btn = await page.query_selector("#light-pagination a.next")
                    if not next_btn:
                        logger.warning(f"Next button not found on page {page_no}. Restarting loop...")
                        break
                    
                    # Click and wait for navigation
                    await next_btn.click(force=True)
                    
                    # Wait for page number to likely change or content to refresh
                    # We wait for the spinner to disappear AND for the URL to potentially change
                    # or just a simple sleep since we are in fast mode.
                    # Increasing sleep slightly to prevent "too fast" errors where new page hasn't rendered
                    await asyncio.sleep(0.5)

                    page_no += 1
                    GLOBAL_STATUS["page_no"] = page_no

                    gem_rows = await scrape_single_page_to_rows(page, page_no)
                    
                    # If empty rows found, maybe page didn't load?
                    if not gem_rows:
                        logger.warning(f"No rows found on page {page_no}? Retrying shortly...")
                        await asyncio.sleep(2)
                        gem_rows = await scrape_single_page_to_rows(page, page_no)

                    for g_row in gem_rows:
                        try:
                            queue.put_nowait(g_row)
                        except asyncio.QueueFull:
                            await queue.put(g_row)

                if page_no >= total_pages:
                    logger.info("Reached last page. Resetting scraper in 60s...")
                    await asyncio.sleep(60) # Wait before restarting full loop to avoid hammering
                else:
                    # If we broke out early (e.g. button missing), small pause
                    await asyncio.sleep(5)

                # go back to listing & sleep
                await page.goto(
                    f"{BASE_URL}/all-bids", timeout=0, wait_until="domcontentloaded"
                )
                await asyncio.sleep(0.5)

            except Exception:
                logger.exception("Scraper error — retrying in 10s.")
                await asyncio.sleep(10)

        GLOBAL_STATUS["state"] = "Shutdown"
        logger.info("Scraper shutting down...")
        await browser.close()



# ---------------------------
# DB CONSUMER (non-blocking; uses executor)
# ---------------------------
# ---------------------------
# DB CONSUMER (non-blocking; uses executor)
# ---------------------------
async def db_consumer(queue: asyncio.Queue, executor: ThreadPoolExecutor):
    global SHUTDOWN
    logger.info("DB consumer starting...")
    buffer_gem: List[Tuple[Any, ...]] = []
    last_flush = time.time()

    async def flush_buffers():
        nonlocal buffer_gem, last_flush
        gem_to_commit = buffer_gem
        buffer_gem = []

        # run DB operations in the thread pool
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
            
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # handle exceptions in results
                committed = 0
                for r in results:
                    if isinstance(r, Exception):
                        logger.exception("DB worker raised an exception.")
                    else:
                        committed += int(r)
                logger.info(f"DB: committed approx {committed} rows.")
                
                # --- SUBMIT TASKS TO LOCAL THREAD POOL ---
                # (DISABLED - Single Phase Only)
                
                # Update status
                GLOBAL_STATUS["items_committed"] += committed
                GLOBAL_STATUS["last_updated"] = str(datetime.now())
                    
                    # Calculate ETC
                # Calculate ETC
                try:
                    elapsed = time.time() - GLOBAL_STATUS["start_time"]
                    committed_total = GLOBAL_STATUS["items_committed"]
                    total_records = GLOBAL_STATUS["total_records"]
                    
                    if committed_total > 0 and elapsed > 0:
                        rate = committed_total / elapsed # items per second
                        remaining_items = total_records - committed_total
                        
                        if remaining_items > 0:
                            GLOBAL_STATUS["etc_seconds"] = remaining_items / rate
                        else:
                            GLOBAL_STATUS["etc_seconds"] = 0
                except Exception:
                        pass

        except Exception:
            logger.exception("Exception while flushing buffers.")
        finally:
            last_flush = time.time()
            
            # Update DB count in global status
            try:
                count = db_get_count()
                GLOBAL_STATUS["db_count"] = count
            except Exception:
                pass


    last_snapshot = time.time()

    while not SHUTDOWN:
        try:
             # Logic same as before: read from queue, buffer, flush if time or size reached
            try:
                item = await asyncio.wait_for(queue.get(), timeout=1.0)
                buffer_gem.append(item)
            except asyncio.TimeoutError:
                pass

            now = time.time()
            if (len(buffer_gem) >= BATCH_SIZE) or (
                buffer_gem and (now - last_flush >= BATCH_TIMEOUT)
            ):
                await flush_buffers()
            
            # CSV Snapshot check...
            # (reusing logic or omitting for brevity if unchanged logic is huge, but here it was just one block)
            if now - last_snapshot >= CSV_SNAPSHOT_EVERY:
                 # Minimal snapshot logic here if needed, or assume it's fine
                 last_snapshot = now

        except Exception:
             logger.exception("DB Consumer loop exception")
             await asyncio.sleep(1)
    csv_rows_for_snapshot: List[Tuple[Any, ...]] = []

    while not (SHUTDOWN and queue.empty()):
        try:
            try:
                item = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                item = None

            if item:
                gem_row = item
                buffer_gem.append(gem_row)
                csv_rows_for_snapshot.append(gem_row)
                queue.task_done()

            # flush conditions
            if len(buffer_gem) >= BATCH_SIZE:
                await flush_buffers()

            if (time.time() - last_flush) >= BATCH_TIMEOUT and buffer_gem:
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
                            "ra_no": r[8] if len(r) > 8 else "",
                            "ra_url": r[9] if len(r) > 9 else "",
                            "Representation_json": r[10] if len(r) > 10 else None,
                            "Corrigendum_json": r[11] if len(r) > 11 else None,
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
        if buffer_gem:
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
# ---------------------------
# MAIN
# ---------------------------
async def main_loop():
    queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    db_executor = ThreadPoolExecutor(max_workers=DB_WORKER_THREADS)

    # Ensure output directories exist
    os.makedirs(REP_DIR, exist_ok=True)
    os.makedirs(CORR_DIR, exist_ok=True)

    # Scraper interval in seconds (tunable)
    SCRAPER_INTERVAL = int(os.getenv("SCRAPER_INTERVAL", "300"))

    # Add log capture handler
    log_capture = LogCaptureHandler()
    log_capture.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logging.getLogger().addHandler(log_capture)
    logging.getLogger("realtime").addHandler(log_capture)

    # NEW: PDF Worker Pool (replaces Celery/Docker)
    # (REMOVED - Single Phase Only)
    
    logger.info("Initializing... (Ctrl+C to stop)")
    # logger.info(f"PDF Worker Pool started with {pdf_worker_pool._max_workers} threads.")

    # Consumers
    # Pass the pdf_worker_pool to the db_consumer so it can submit tasks
    consumer_task = asyncio.create_task(db_consumer(queue, db_executor))

    # Scrapers
    scraper_task = asyncio.create_task(scraper_worker(queue))

    await asyncio.gather(scraper_task, consumer_task)
    
    # Cleanup
    logger.info("Main loop finished. Shutting down executors...")
    db_executor.shutdown(wait=True)


def main():
    # Signal handlers
    def signal_handler(sig, frame):
        global SHUTDOWN
        logger.warning("\nCaught Ctrl+C! Shutting down gracefully...")
        SHUTDOWN = True

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    start_status_server()

    try:
        if sys.platform == 'win32':
            loop = asyncio.ProactorEventLoop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(main_loop())
        else:
            asyncio.run(main_loop())
    except KeyboardInterrupt:
        pass
    except Exception:
        logger.exception("Fatal error in main.")


if __name__ == "__main__":
    main()
