import os
import time
import random
import string
import requests
import subprocess
import threading
from datetime import datetime, timedelta
from fastapi import FastAPI, Response, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2 import pool
import uvicorn

# ==========================================
#               CONFIGURATION
# ==========================================

# 1. Video Database Config
VIDEO_DB_URI = os.getenv("DATABASE_URL", "postgres://avnadmin:AVNS_d9GncXE-Fge9t5p3XlY@pg-7cbbad8-tanyasinghagrawal-62c1.j.aivencloud.com:26734/defaultdb?sslmode=require")
WORKERS = 2

# 2. Premium User Database Config
PREMIUM_DB_URI = "postgres://avnadmin:AVNS_BeV0WUMWjhrpjw4x4_j@pg-344d515a-mritunjaysinghagrawal-209d.g.aivencloud.com:22418/defaultdb?sslmode=require"

# Target Domains
DOMAINS = [
    "cdn.desitales2.com",
    "cdn.kamareels2.com",
    "cdn.freesexkahani.com"
]

# ==========================================
#            DATABASE POOLS SETUP
# ==========================================

try:
    # Pool for Videos
    video_db_pool = psycopg2.pool.ThreadedConnectionPool(1, WORKERS + 10, VIDEO_DB_URI)
    print("✅ Video PostgreSQL Connection Pool Initialized.")
    
    # Pool for Premium Users
    premium_db_pool = psycopg2.pool.ThreadedConnectionPool(1, 10, PREMIUM_DB_URI)
    print("✅ Premium PostgreSQL Connection Pool Initialized.")
except Exception as e:
    print(f"❌ DB Pool Error: {e}")
    exit(1)

# --- Global RAM Cache for Videos ---
JSON_CACHE = None
CACHE_LOCK = threading.Lock()

# ==========================================
#               FASTAPI SETUP
# ==========================================

app = FastAPI(title="Unified Multi-Domain API Tracker")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================
#          UTILITY FUNCTIONS
# ==========================================

def generate_video_id(length=15):
    """Generates a random 15-character alphanumeric ID"""
    characters = string.ascii_lowercase + string.digits
    return ''.join(random.choice(characters) for _ in range(length))

# ==========================================
#          BACKGROUND THREAD FUNCTIONS
# ==========================================

def run_db_migrations():
    """Video DB Schema setup and Migration for Old Data"""
    conn = video_db_pool.getconn()
    cur = conn.cursor()
    try:
        # 1. Update existing table to support video_id and domain
        cur.execute("""
            CREATE TABLE IF NOT EXISTS video_metadata_scan (
                id SERIAL PRIMARY KEY,
                video_num INTEGER,
                video_url TEXT,
                size_mb FLOAT,
                resolution TEXT,
                status TEXT,
                duration FLOAT,
                meta_bytes INTEGER,
                thumbnail BYTEA,
                is_processed BOOLEAN DEFAULT FALSE,
                video_id VARCHAR(15) UNIQUE,
                domain VARCHAR(100)
            );
        """)
        
        # 2. Add columns agar purani table hai aur columns missing hain (Safe Alter)
        try:
            cur.execute("ALTER TABLE video_metadata_scan ADD COLUMN video_id VARCHAR(15) UNIQUE;")
        except psycopg2.errors.DuplicateColumn:
            conn.rollback() # If already exists
        
        try:
            cur.execute("ALTER TABLE video_metadata_scan ADD COLUMN domain VARCHAR(100);")
        except psycopg2.errors.DuplicateColumn:
            conn.rollback()

        # 3. FIX FOR ON CONFLICT ERROR: Drop old unique constraint and add composite one
        try:
            # Purana video_num ka single unique constraint hatayenge
            cur.execute("ALTER TABLE video_metadata_scan DROP CONSTRAINT IF EXISTS video_metadata_scan_video_num_key;")
            conn.commit()
        except Exception:
            conn.rollback()
            
        try:
            # Naya (domain, video_num) ka combined constraint lagayenge
            cur.execute("ALTER TABLE video_metadata_scan DROP CONSTRAINT IF EXISTS unique_domain_video;")
            cur.execute("ALTER TABLE video_metadata_scan ADD CONSTRAINT unique_domain_video UNIQUE(domain, video_num);")
            conn.commit()
        except Exception:
            conn.rollback()

        # 4. Create table for tracking Domain scan status
        cur.execute("""
            CREATE TABLE IF NOT EXISTS domain_scan_logs (
                domain VARCHAR(100) PRIMARY KEY,
                last_scan_time TIMESTAMP,
                last_video_num INTEGER DEFAULT 0
            );
        """)
        conn.commit()

        # 5. Migrate Old Data (Assign 15-char IDs to old videos and set default domain)
        print("🔍 Checking for old data migration...")
        cur.execute("UPDATE video_metadata_scan SET domain = 'cdn.desitales2.com' WHERE domain IS NULL;")
        conn.commit()

        cur.execute("SELECT id FROM video_metadata_scan WHERE video_id IS NULL;")
        rows_to_update = cur.fetchall()
        
        if rows_to_update:
            print(f"🔄 Migrating {len(rows_to_update)} old videos to 15-char format...")
            for row in rows_to_update:
                new_id = generate_video_id()
                # Ensure unique ID handling
                while True:
                    try:
                        cur.execute("UPDATE video_metadata_scan SET video_id = %s WHERE id = %s;", (new_id, row[0]))
                        conn.commit()
                        break
                    except psycopg2.errors.UniqueViolation:
                        conn.rollback()
                        new_id = generate_video_id() # Try again with a new ID
            print("✅ Migration complete.")

    except Exception as e:
        conn.rollback()
        print(f"❌ Migration Error: {e}")
    finally:
        cur.close()
        video_db_pool.putconn(conn)
def refresh_videos_cache():
    """Updates the RAM Cache from the Video Database"""
    global JSON_CACHE
    conn = video_db_pool.getconn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT video_id, video_url, size_mb, resolution, duration, meta_bytes, domain 
            FROM video_metadata_scan 
            WHERE is_processed = TRUE AND status = 'Success'
            ORDER BY id DESC;
        """)
        rows = cur.fetchall()
        
        data = []
        for r in rows:
            data.append({
                "id": r[0], # Naya 15-char ID bhejenge
                "url": r[1],
                "size_mb": r[2],
                "resolution": r[3] if r[3] else "720x1280",
                "duration": r[4],
                "meta_bytes_required": r[5],
                "domain": r[6]
            })
            
        with CACHE_LOCK:
            JSON_CACHE = data
        print(f"[Cache] RAM Cache updated! Total videos: {len(data)}")
    except Exception as e:
        print(f"[Cache Error] {e}")
    finally:
        cur.close()
        video_db_pool.putconn(conn)

def get_moov_size(url):
    """Fetches required metadata bytes for smart playback"""
    try:
        headers = {'Range': 'bytes=0-2097152'} 
        r = requests.get(url, headers=headers, timeout=10)
        data = r.content
        offset = 0
        while offset < len(data) - 8:
            size = int.from_bytes(data[offset:offset+4], byteorder='big')
            box_type = data[offset+4:offset+8].decode('ascii', errors='ignore')
            if box_type == 'moov':
                return offset + size + 65536 
            if size < 8: break
            offset += size
        return 512 * 1024 
    except:
        return 512 * 1024

def extract_thumbnail_and_duration(url):
    """Extracts duration & WebP thumbnail"""
    duration, thumbnail_data = 0.0, None
    try:
        dur_cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", url]
        dur_res = subprocess.run(dur_cmd, capture_output=True, text=True, timeout=15)
        duration = float(dur_res.stdout.strip())
    except: pass

    try:
        thumb_cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "error", 
            "-ss", "00:00:01", "-i", url, "-vframes", "1", 
            "-c:v", "libwebp", "-q:v", "40", "-vf", "scale=480:-1", 
            "-f", "webp", "pipe:1"
        ]
        thumb_res = subprocess.run(thumb_cmd, capture_output=True, timeout=20)
        thumbnail_data = thumb_res.stdout
    except: pass
    return duration, thumbnail_data

def background_processor():
    """Worker Thread: Processes videos sitting in queue"""
    while True:
        conn = None
        try:
            conn = video_db_pool.getconn()
            cur = conn.cursor()
            
            cur.execute("""
                SELECT id, video_url, video_id, domain FROM video_metadata_scan 
                WHERE status = 'Success' AND is_processed = FALSE 
                ORDER BY id ASC LIMIT 1 FOR UPDATE SKIP LOCKED;
            """)
            row = cur.fetchone()
            
            if not row:
                conn.commit()
                # Yahan se cur.close() aur putconn hata diya kyunki finally automatically ye karega
                time.sleep(10) 
                continue
                
            db_id, v_url, v_id, domain = row
            meta_bytes = get_moov_size(v_url)
            duration, thumb_bytes = extract_thumbnail_and_duration(v_url)
            
            if thumb_bytes and len(thumb_bytes) > 0:
                cur.execute("""
                    UPDATE video_metadata_scan 
                    SET duration = %s, meta_bytes = %s, thumbnail = %s, is_processed = TRUE 
                    WHERE id = %s;
                """, (duration, meta_bytes, thumb_bytes, db_id))
                print(f"[Processed] {domain} | ID: {v_id} | Dur: {duration}s")
                conn.commit()
                refresh_videos_cache()
            else:
                cur.execute("UPDATE video_metadata_scan SET is_processed = TRUE, status = 'Media Error' WHERE id = %s", (db_id,))
                conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            time.sleep(5)
        finally:
            if conn:
                cur.close()
                video_db_pool.putconn(conn)

def auto_discover_videos():
    """Smarter Crawler: Multiple domains, smart batching, 24h checks with Safe DB Connections"""
    while True:
        for domain in DOMAINS:
            # ========================================================
            # STEP 1: Quick DB check
            # ========================================================
            conn = video_db_pool.getconn()
            cur = conn.cursor()
            try:
                cur.execute("SELECT last_scan_time, last_video_num FROM domain_scan_logs WHERE domain = %s;", (domain,))
                scan_log = cur.fetchone()
                
                now = datetime.now()
                is_first_scan = False
                should_scan = False
                last_num = 0
                
                if not scan_log:
                    # Naya domain, first time scan
                    is_first_scan = True
                    should_scan = True
                    cur.execute("INSERT INTO domain_scan_logs (domain, last_scan_time, last_video_num) VALUES (%s, %s, 0);", (domain, now))
                    conn.commit()
                    print(f"🚀 [Crawler] Starting FIRST TIME scan for {domain}...")
                else:
                    last_scan_time, last_num = scan_log
                    if not last_scan_time or (now - last_scan_time).total_seconds() >= 86400:
                        should_scan = True
                        print(f"⏱️ [Crawler] 24hrs completed. Starting routine scan for {domain} from {last_num + 1}...")
            except Exception as e:
                print(f"[Crawler DB Status Error for {domain}] {e}")
                should_scan = False
            finally:
                cur.close()
                video_db_pool.putconn(conn)
            
            # ========================================================
            # STEP 2: Network Scanning 
            # ========================================================
            if should_scan:
                current_num = last_num + 1
                consecutive_misses = 0
                found_any_video = False
                
                while True:
                    folder = (current_num // 1000) * 1000
                    video_url = f"https://{domain}/{folder}/{current_num}/{current_num}.mp4"
                    
                    status_success = False
                    size_mb = 0
                    
                    try:
                        head = requests.head(video_url, timeout=10)
                        if head.status_code == 200 and 'video' in head.headers.get('Content-Type', ''):
                            size_mb = round(int(head.headers.get('Content-Length', 0)) / (1024 * 1024), 2)
                            status_success = True
                    except Exception as e:
                        print(f"[Crawler Link Error] {domain} - {current_num}: {e}")
                        consecutive_misses += 1 

                    # ========================================================
                    # STEP 3: Quick DB Write
                    # ========================================================
                    save_conn = video_db_pool.getconn()
                    save_cur = save_conn.cursor()
                    try:
                        if status_success:
                            v_id = generate_video_id()
                            save_cur.execute("""
                                INSERT INTO video_metadata_scan (video_num, video_url, size_mb, status, is_processed, video_id, domain)
                                VALUES (%s, %s, %s, 'Success', FALSE, %s, %s)
                                ON CONFLICT (domain, video_num) DO NOTHING;
                            """, (current_num, video_url, size_mb, v_id, domain))
                            save_conn.commit()
                            
                            print(f"  [+] Found: {domain} -> ID: {v_id} (Num: {current_num})")
                            found_any_video = True
                            consecutive_misses = 0 
                            last_num = current_num 
                        else:
                            if not status_success:
                                consecutive_misses += 1
                                
                            save_cur.execute("""
                                INSERT INTO video_metadata_scan (video_num, video_url, size_mb, status, is_processed, video_id, domain)
                                VALUES (%s, %s, 0, 'Not Found', TRUE, %s, %s)
                                ON CONFLICT (domain, video_num) DO NOTHING;
                            """, (current_num, video_url, generate_video_id(), domain))
                            save_conn.commit()
                    except Exception as db_e:
                        save_conn.rollback() 
                        print(f"[Crawler DB Write Error] {domain} - {current_num}: {db_e}")
                    finally:
                        save_cur.close()
                        video_db_pool.putconn(save_conn)

                    # ========================================================
                    # LIVE DASHBOARD UPDATE: Har 10 URL baad tracker update karega
                    # ========================================================
                    if current_num % 10 == 0:
                        log_conn = video_db_pool.getconn()
                        log_cur = log_conn.cursor()
                        try:
                            log_cur.execute("UPDATE domain_scan_logs SET last_video_num = %s WHERE domain = %s;", (current_num, domain))
                            log_conn.commit()
                        except:
                            log_conn.rollback()
                        finally:
                            log_cur.close()
                            video_db_pool.putconn(log_conn)

                    # ========================================================
                    # STOPPING LOGIC (Upgraded for 2k+ Videos)
                    # ========================================================
                    if is_first_scan:
                        # 500 miss hone par ya 5000 URLs scan karne ke baad rukega
                        if (found_any_video and consecutive_misses >= 500) or current_num >= 5000:
                            print(f"🛑 [Crawler] First scan stopped for {domain}. Last Found: {last_num}. (Misses: {consecutive_misses})")
                            break
                    else:
                        if consecutive_misses >= 100:
                            print(f"🛑 [Crawler] Routine scan finished for {domain}. Last Found: {last_num}. (Misses: 100)")
                            break

                    current_num += 1
                    time.sleep(0.5) 
                
                # ========================================================
                # STEP 4: Update Domain Last Scan Logs (Final Time)
                # ========================================================
                log_conn = video_db_pool.getconn()
                log_cur = log_conn.cursor()
                try:
                    log_cur.execute("UPDATE domain_scan_logs SET last_scan_time = %s, last_video_num = %s WHERE domain = %s;", (datetime.now(), last_num, domain))
                    log_conn.commit()
                except Exception as log_e:
                    log_conn.rollback()
                finally:
                    log_cur.close()
                    video_db_pool.putconn(log_conn)

        print(f"💤 [Crawler] Sleeping for 5 minutes before next check...")
        time.sleep(300)

def keep_alive_premium_db():
    """Ping Premium DB to keep connection alive"""
    while True:
        try:
            time.sleep(300) # 5 Minutes
            conn = premium_db_pool.getconn()
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM echelon_heaven_hub_bot_premium LIMIT 1;")
            conn.commit()
            cur.close()
            premium_db_pool.putconn(conn)
        except Exception as e:
            print(f"⚠️ Premium Keep-alive ping failed: {e}")
            if 'conn' in locals() and conn:
                premium_db_pool.putconn(conn)


# ==========================================
#              FASTAPI EVENTS
# ==========================================

@app.on_event("startup")
def startup_event():
    # 1. Background function for DB setup
    def initial_setup():
        time.sleep(3) # ✨ MAGIC FIX: Server ko start hone ka time dega
        print("⏳ Running DB Migrations in background...")
        run_db_migrations()
        print("⏳ Refreshing cache...")
        refresh_videos_cache()
        print("✅ Initial setup complete!")

    setup_thread = threading.Thread(target=initial_setup, daemon=True)
    setup_thread.start()
    
    # 2. Start Video Workers (With Delay)
    def delayed_worker():
        time.sleep(5) # Worker 5 sec baad jagenge
        background_processor()

    for _ in range(WORKERS):
        t = threading.Thread(target=delayed_worker, daemon=True)
        t.start()
        
    # 3. Start Multi-Domain Crawler (With Delay)
    def delayed_crawler():
        time.sleep(10) # Crawler 10 sec baad aayega taaki server load na ho
        auto_discover_videos()

    crawler_thread = threading.Thread(target=delayed_crawler, daemon=True)
    crawler_thread.start()

    # 4. Start Premium DB Keep-Alive
    keep_alive_thread = threading.Thread(target=keep_alive_premium_db, daemon=True)
    keep_alive_thread.start()
# ==========================================
#                 ROUTES
# ==========================================

@app.get("/", response_class=HTMLResponse)
def dashboard():
    conn = video_db_pool.getconn()
    cur = conn.cursor()
    try:
        # Fetch stats grouped by Domain
        cur.execute("""
            SELECT domain, 
                   COUNT(*) as total_found, 
                   SUM(CASE WHEN is_processed = TRUE AND status = 'Success' THEN 1 ELSE 0 END) as processed 
            FROM video_metadata_scan 
            WHERE status = 'Success'
            GROUP BY domain;
        """)
        domain_stats = cur.fetchall()

        # Fetch Crawler Logs
        cur.execute("SELECT domain, last_scan_time, last_video_num FROM domain_scan_logs;")
        scan_logs = {row[0]: (row[1], row[2]) for row in cur.fetchall()}

        domain_html = ""
        total_valid = 0
        total_processed = 0

        for dom, total, processed in domain_stats:
            total_valid += total
            total_processed += processed
            log_time, last_num = scan_logs.get(dom, ("Never", 0))
            if isinstance(log_time, datetime):
                log_time = log_time.strftime('%Y-%m-%d %H:%M:%S')

            domain_html += f"""
            <div class="card domain-card">
                <h3>🌐 {dom}</h3>
                <p><strong>Total Valid:</strong> {total}</p>
                <p><strong>Processed:</strong> {processed}</p>
                <p><strong>Last Scan ID:</strong> #{last_num}</p>
                <p class="time">Last Checked: {log_time}</p>
            </div>
            """

        html = f"""
        <html><head><title>Multi-Domain Unified Dashboard</title>
        <style>
            body{{font-family: Arial, sans-serif; text-align: center; margin: 0; padding: 20px; background: #1a1a2e; color: #fff;}}
            h1 {{color: #e94560;}}
            .stats-container {{ display: flex; justify-content: center; flex-wrap: wrap; gap: 20px; margin-bottom: 30px; }}
            .card{{background: #16213e; padding: 20px; border-radius: 12px; min-width: 200px; box-shadow: 0 4px 6px rgba(0,0,0,0.3);}}
            .domain-card {{ background: #0f3460; border-left: 5px solid #e94560; text-align: left; }}
            .premium-card{{background: #00b8a9; padding: 20px; border-radius: 12px; margin: 20px auto; max-width: 400px; color: #fff;}}
            .time {{ font-size: 0.8em; color: #a1a1aa; }}
            h3 {{ margin-top: 0; }}
        </style></head>
        <body>
            <h1>🚀 Media Discovery & Processing Hub</h1>
            
            <div class="stats-container">
                <div class="card"><h3>Total Global Videos</h3><h2>{total_valid}</h2></div>
                <div class="card"><h3>Globally Processed</h3><h2>{total_processed}</h2></div>
                <div class="card"><h3>Global Queue</h3><h2>{total_valid - total_processed}</h2></div>
            </div>

            <h2>Domain Statistics</h2>
            <div class="stats-container">
                {domain_html if domain_html else "<p>No domains scanned yet. Crawler is running...</p>"}
            </div>
            
            <div class="premium-card">
                <h3>💎 Premium API Status</h3>
                <p>✅ Running & Active</p>
                <p><small>Endpoint: /verify?user_id=12345</small></p>
            </div>
        </body></html>
        """
        return HTMLResponse(content=html)
    finally:
        cur.close()
        video_db_pool.putconn(conn)

@app.get("/thumbnail/{video_id}")
def get_thumbnail(video_id: str):
    """Ab thumbnail old id (int) ki jagah naye 15-char video_id (str) se aayega"""
    conn = video_db_pool.getconn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT thumbnail FROM video_metadata_scan WHERE video_id = %s AND thumbnail IS NOT NULL;", (video_id,))
        row = cur.fetchone()
        if row and row[0]:
            return Response(content=row[0], media_type="image/webp")
        return Response(status_code=404, content="Thumbnail not found or processing.")
    finally:
        cur.close()
        video_db_pool.putconn(conn)

@app.get("/videos.json", response_class=JSONResponse)
def get_videos_json():
    if JSON_CACHE is None:
        refresh_videos_cache()
    return JSONResponse(content=JSON_CACHE)

@app.get("/verify")
def verify_premium_user(user_id: str = Query(None)):
    """
    API Endpoint: /verify?user_id=123456789
    """
    if not user_id:
        return JSONResponse(status_code=400, content={"error": "user_id parameter missing hai. Example: /verify?user_id=12345"})
        
    try:
        uid = int(user_id)
    except ValueError:
        return JSONResponse(status_code=400, content={"error": "Invalid user_id format. Sirf numbers allowed hain."})

    conn = None
    try:
        conn = premium_db_pool.getconn()
        cur = conn.cursor()
        
        query = "SELECT expiry_time FROM echelon_heaven_hub_bot_premium WHERE user_id = %s;"
        cur.execute(query, (uid,))
        result = cur.fetchone()
        cur.close()
        
        if not result:
            return {
                "user_id": uid,
                "status": "not_premium",
                "message": "User ka record database me nahi mila."
            }
            
        expiry_time = result[0]
        current_time = datetime.now()
        
        if expiry_time and expiry_time.replace(tzinfo=None) > current_time:
            return {
                "user_id": uid,
                "status": "premium",
                "expiry_date": expiry_time.isoformat(),
                "message": "User ka premium active hai."
            }
        else:
            return {
                "user_id": uid,
                "status": "not_premium",
                "expiry_date": expiry_time.isoformat() if expiry_time else None,
                "message": "User ka premium expire ho chuka hai."
            }

    except Exception as e:
        print(f"❌ Premium API Error: {e}")
        return JSONResponse(status_code=500, content={"error": "Internal Server Error", "details": str(e)})
        
    finally:
        if conn:
            premium_db_pool.putconn(conn)

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False)
