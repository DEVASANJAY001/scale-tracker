import requests
import json
import time
import random
import os
import psycopg2
from psycopg2 import extras
from datetime import datetime

# Configuration
BASE_URL = "https://draw.ar-lottery01.com/WinGo/WinGo_30S/GetHistoryIssuePage.json"
PAGE_SIZE = 10
MAX_PAGES = 50
POLL_INTERVAL = 15  # seconds

# Database Configuration
# DATABASE_URL should be set in environment variables
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:YOUR_PASSWORD@db.jjovirnswldbnfokyyuk.supabase.co:5432/postgres")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://bigmumbaiy.com/#/saasLottery/WinGo?gameCode=WinGo_30S&lottery=WinGo",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://bigmumbaiy.com",
}

def get_db_connection():
    """Create a connection to the Supabase PostgreSQL database."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"[-] Error connecting to database: {e}")
        return None

def init_db():
    """Initialize the database table if it doesn't exist."""
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS wingo_history (
                    period_id TEXT PRIMARY KEY,
                    result_number TEXT,
                    size TEXT,
                    color TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()
            print("[+] Database table initialized (or already exists).")
    except Exception as e:
        print(f"[-] Error initializing database: {e}")
    finally:
        conn.close()

def get_timestamp():
    """Generate a Unix timestamp in milliseconds."""
    return int(time.time() * 1000)

def fetch_page(page_index):
    """Fetch a single page of historical data."""
    params = {
        "pageIndex": page_index,
        "pageSize": PAGE_SIZE,
        "ts": get_timestamp()
    }
    
    try:
        response = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"[-] HTTP Error {response.status_code} for page {page_index}: {e}")
        return None
    except Exception as e:
        print(f"[-] Error fetching page {page_index}: {e}")
        return None

def parse_record(item):
    """Extract required fields from a record."""
    period_id = item.get("issueNumber")
    result_number = item.get("number")
    
    if period_id is None or result_number is None:
        return None

    # Calculate Size (Big: 5-9, Small: 0-4)
    try:
        num = int(result_number)
        size = "Big" if num >= 5 else "Small"
    except (ValueError, TypeError):
        size = "Unknown"

    # Extraction Color
    color = item.get("color", "Unknown")

    return {
        "period_id": str(period_id),
        "result_number": str(result_number),
        "size": size,
        "color": color
    }

def save_to_db(records):
    """Save records to the Supabase PostgreSQL database."""
    if not records:
        return
    
    conn = get_db_connection()
    if not conn:
        return

    try:
        with conn.cursor() as cur:
            # Prepare records for insertion (UPSERT)
            # period_id is the primary key, so we handle conflicts
            insert_query = """
                INSERT INTO wingo_history (period_id, result_number, size, color)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (period_id) DO NOTHING;
            """
            
            data = [(r['period_id'], r['result_number'], r['size'], r['color']) for r in records]
            
            extras.execute_batch(cur, insert_query, data)
            conn.commit()
            print(f"[+] Successfully saved/updated {len(records)} records in Supabase.")
    except Exception as e:
        print(f"[-] Error saving to database: {e}")
    finally:
        conn.close()

def get_latest_period_id():
    """Get the latest period ID stored in the database."""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT period_id FROM wingo_history ORDER BY period_id DESC LIMIT 1")
            row = cur.fetchone()
            return row[0] if row else None
    except Exception as e:
        print(f"[-] Error fetching latest record: {e}")
        return None
    finally:
        conn.close()

def scrape_historical():
    """Initial scrape of 50 pages."""
    print(f"[*] Starting historical scrape (50 pages)...")
    total_new_records = 0

    for page in range(1, MAX_PAGES + 1):
        print(f"[*] Fetching page {page}/{MAX_PAGES}...")
        data = fetch_page(page)
        
        if not data:
            # Implement back-off
            delay = random.uniform(2, 5)
            print(f"[!] Fetch failed. Retrying in {delay:.2f}s...")
            time.sleep(delay)
            data = fetch_page(page)
            if not data: continue

        # The API returns data in 'data' -> 'list'
        items = []
        if data and isinstance(data, dict):
            data_obj = data.get("data")
            if data_obj and isinstance(data_obj, dict):
                items = data_obj.get("list") or []

        if not items:
            print(f"[!] No items found on page {page}. Raw response: {data}")
            break

        print(f"[*] Processing {len(items)} items from page {page}...")
        page_records = []
        for item in items:
            record = parse_record(item)
            if record:
                page_records.append(record)

        if page_records:
            save_to_db(page_records)
            total_new_records += len(page_records)

        # Add jittered delay between requests to avoid detection
        time.sleep(random.uniform(1.0, 2.5))

    print(f"[+] Historical scrape complete. Total records processed: {total_new_records}")

def monitor_mode():
    """Enter real-time polling mode."""
    print(f"[*] Entering Monitor Mode. Polling every {POLL_INTERVAL} seconds...")
    
    # Keep track of known IDs locally to avoid redundant DB calls every poll
    # but initially seed from DB if needed.
    known_latest_id = get_latest_period_id()
    print(f"[*] Starting monitor from latest known ID: {known_latest_id}")

    while True:
        try:
            # Use page 1 to get the newest result
            data = fetch_page(1)
            items = []
            if data and isinstance(data, dict):
                data_obj = data.get("data")
                if data_obj and isinstance(data_obj, dict):
                    items = data_obj.get("list") or []

            new_records = []
            for item in items:
                record = parse_record(item)
                if record:
                    # If we don't know the latest ID yet, or this record is newer
                    if known_latest_id is None or record["period_id"] > known_latest_id:
                        print(f"[!] New Result Found: Period {record['period_id']} -> {record['result_number']} ({record['size']}/{record['color']})")
                        new_records.append(record)
            
            if new_records:
                save_to_db(new_records)
                # Update known latest ID (assuming records are somewhat sequential)
                known_latest_id = max(r['period_id'] for r in new_records)
            
            # Use fixed interval of 15s (default POLL_INTERVAL)
            wait_time = POLL_INTERVAL
            next_check = datetime.now().strftime("%H:%M:%S")
            print(f"[*] Last check completed at {next_check}. Next check in {wait_time}s...")
            time.sleep(wait_time)

        except KeyboardInterrupt:
            print("\n[*] Monitor Mode stopped by user.")
            break
        except Exception as e:
            print(f"[-] Error in monitor loop: {e}")
            time.sleep(10)

if __name__ == "__main__":
    print("\n" + "="*50)
    print("        WINGO 30S DATA COLLECTOR (SUPABASE)")
    print("="*50)
    
    # Initialize DB
    init_db()

    # Initial setup (can be skipped if DB already has records)
    # For simplicity, we run it once or check if DB is empty
    if get_latest_period_id() is None:
        scrape_historical()
    
    # Start Monitor Mode
    monitor_mode()
