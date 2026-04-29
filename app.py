import os
import time
import requests
import subprocess
import threading
from datetime import datetime
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

# ==========================================
#            DATABASE POOLS SETUP
# ==========================================

try:
    # Pool for Videos
    video_db_pool = psycopg2.pool.ThreadedConnectionPool(1, WORKERS + 5, VIDEO_DB_URI)
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

app = FastAPI(title="Unified API: Video Tracker & Premium Verifier")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================
#          BACKGROUND THREAD FUNCTIONS
# ==========================================

def run_db_migrations():
    """Video DB Schema setup"""
    conn = video_db_pool.getconn()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS video_metadata_scan (
                id SERIAL PRIMARY KEY,
                video_num INTEGER UNIQUE,
                video_url TEXT,
                size_mb FLOAT,
                resolution TEXT,
                status TEXT,
                duration FLOAT,
                meta_bytes INTEGER,
                thumbnail BYTEA,
                is_processed BOOLEAN DEFAULT FALSE
            );
        """)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Migration Error: {e}")
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
            SELECT video_num, video_url, size_mb, resolution, duration, meta_bytes 
            FROM video_metadata_scan 
            WHERE is_processed = TRUE AND status = 'Success'
            ORDER BY video_num DESC;
        """)
        rows = cur.fetchall()
        
        data = []
        for r in rows:
            data.append({
                "id": str(r[0]),
                "url": r[1],
                "size_mb": r[2],
                "resolution": r[3] if r[3] else "720x1280",
                "duration": r[4],
                "meta_bytes_required": r[5]
            })
            
        with CACHE_LOCK:
            JSON_CACHE = data
        print("[Cache] RAM Cache updated successfully!")
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
        conn = video_db_pool.getconn()
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT video_num, video_url FROM video_metadata_scan 
                WHERE status = 'Success' AND is_processed = FALSE 
                ORDER BY video_num ASC LIMIT 1 FOR UPDATE SKIP LOCKED;
            """)
            row = cur.fetchone()
            
            if not row:
                conn.commit()
                cur.close()
                video_db_pool.putconn(conn)
                time.sleep(10) 
                continue
                
            v_num, v_url = row
            meta_bytes = get_moov_size(v_url)
            duration, thumb_bytes = extract_thumbnail_and_duration(v_url)
            
            if thumb_bytes and len(thumb_bytes) > 0:
                cur.execute("""
                    UPDATE video_metadata_scan 
                    SET duration = %s, meta_bytes = %s, thumbnail = %s, is_processed = TRUE 
                    WHERE video_num = %s;
                """, (duration, meta_bytes, thumb_bytes, v_num))
                print(f"[Processed] {v_num} | Dur: {duration}s | Meta: {meta_bytes}B")
                conn.commit()
                refresh_videos_cache()
            else:
                cur.execute("UPDATE video_metadata_scan SET is_processed = TRUE, status = 'Media Error' WHERE video_num = %s", (v_num,))
                conn.commit()
        except Exception as e:
            conn.rollback()
            time.sleep(5)
        finally:
            cur.close()
            video_db_pool.putconn(conn)

def auto_discover_videos():
    """Crawler: Checks next 50 URLs every 2 hours"""
    while True:
        conn = video_db_pool.getconn()
        cur = conn.cursor()
        try:
            cur.execute("SELECT MAX(video_num) FROM video_metadata_scan;")
            max_num = cur.fetchone()[0]
            start_num = max_num + 1 if max_num else 21
            end_num = start_num + 50
            
            print(f"[Crawler] Starting scan from {start_num} to {end_num - 1}...")

            for current_num in range(start_num, end_num):
                folder = (current_num // 1000) * 1000
                video_url = f"https://cdn.desitales2.com/{folder}/{current_num}/{current_num}.mp4"
                
                try:
                    head = requests.head(video_url, timeout=10)
                    if head.status_code == 200 and 'video' in head.headers.get('Content-Type', ''):
                        size_mb = round(int(head.headers.get('Content-Length', 0)) / (1024 * 1024), 2)
                        
                        cur.execute("""
                            INSERT INTO video_metadata_scan (video_num, video_url, size_mb, status, is_processed)
                            VALUES (%s, %s, %s, 'Success', FALSE)
                            ON CONFLICT (video_num) DO NOTHING;
                        """, (current_num, video_url, size_mb))
                        print(f"  [+] Found new video: {current_num}")
                    else:
                        cur.execute("""
                            INSERT INTO video_metadata_scan (video_num, video_url, size_mb, status, is_processed)
                            VALUES (%s, %s, 0, 'Not Found', TRUE)
                            ON CONFLICT (video_num) DO NOTHING;
                        """, (current_num, video_url))
                    conn.commit()
                except Exception as e:
                    print(f"[Crawler Link Error] {current_num}: {e}")
                    
            print(f"[Crawler] Scan complete. Sleeping for 2 hours...")
        except Exception as e:
            print(f"[Crawler DB Error] {e}")
        finally:
            cur.close()
            video_db_pool.putconn(conn)
            
        time.sleep(7200)

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
            print(f"🔄 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Premium DB keep-alive ping successful.")
        except Exception as e:
            print(f"⚠️ Premium Keep-alive ping failed: {e}")
            if 'conn' in locals() and conn:
                premium_db_pool.putconn(conn)


# ==========================================
#              FASTAPI EVENTS
# ==========================================

@app.on_event("startup")
def startup_event():
    # 1. Setup Video DB
    run_db_migrations()
    refresh_videos_cache()
    
    # 2. Start Video Workers
    for _ in range(WORKERS):
        t = threading.Thread(target=background_processor, daemon=True)
        t.start()
        
    # 3. Start Video Crawler
    crawler_thread = threading.Thread(target=auto_discover_videos, daemon=True)
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
        cur.execute("SELECT MAX(video_num) FROM video_metadata_scan;")
        latest_checked = cur.fetchone()[0] or 0
        
        cur.execute("SELECT COUNT(*) FROM video_metadata_scan WHERE status = 'Success';")
        success = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM video_metadata_scan WHERE is_processed = TRUE AND status = 'Success';")
        processed = cur.fetchone()[0]
        
        html = f"""
        <html><head><title>Unified Dashboard</title>
        <style>body{{font-family: Arial; text-align: center; margin-top: 50px; background: #222; color: #fff;}}
        .card{{background: #333; padding: 20px; border-radius: 10px; display: inline-block; margin: 10px;}}
        .premium-card{{background: #4CAF50; padding: 20px; border-radius: 10px; margin: 20px auto; max-width: 400px;}}
        </style></head>
        <body>
            <h1>Video Discovery & Processing Status</h1>
            <div class="card"><h3>Last Checked Link ID</h3><p>#{latest_checked}</p></div>
            <div class="card"><h3>Total Valid Videos</h3><p>{success}</p></div>
            <div class="card"><h3>Fully Processed</h3><p>{processed}</p></div>
            <div class="card"><h3>Remaining Process Queue</h3><p>{success - processed}</p></div>
            
            <div class="premium-card">
                <h3>Premium API Status</h3>
                <p>✅ Running & Active</p>
                <p><small>Endpoint: /verify?user_id=12345</small></p>
            </div>
        </body></html>
        """
        return HTMLResponse(content=html)
    finally:
        cur.close()
        video_db_pool.putconn(conn)

@app.get("/thumbnail/{video_num}")
def get_thumbnail(video_num: int):
    conn = video_db_pool.getconn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT thumbnail FROM video_metadata_scan WHERE video_num = %s AND thumbnail IS NOT NULL;", (video_num,))
        row = cur.fetchone()
        if row and row[0]:
            return Response(content=row[0], media_type="image/webp")
        return Response(status_code=404, content="Not processed yet.")
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
    Ye user_id check karega aur premium status return karega.
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
    # Runs the unified FastAPI app on port 8000
    uvicorn.run("combined_app:app", host="0.0.0.0", port=8000, reload=False)
