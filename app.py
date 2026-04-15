import os
import time
import requests
import subprocess
import threading
from fastapi import FastAPI, Response, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
import psycopg2
from psycopg2 import pool
import uvicorn

# --- Config ---
DB_URI = os.getenv("DATABASE_URL", "postgres://avnadmin:AVNS_d9GncXE-Fge9t5p3XlY@pg-7cbbad8-tanyasinghagrawal-62c1.j.aivencloud.com:26734/defaultdb?sslmode=require")
WORKERS = 2

# --- Database Pool ---
try:
    db_pool = psycopg2.pool.ThreadedConnectionPool(1, WORKERS + 5, DB_URI)
except Exception as e:
    print(f"DB Error: {e}")
    exit(1)

app = FastAPI()

def run_db_migrations():
    """Modifies DB schema dynamically without breaking old data."""
    conn = db_pool.getconn()
    cur = conn.cursor()
    try:
        cur.execute("ALTER TABLE video_metadata_scan DROP COLUMN IF EXISTS timestamp;")
        cur.execute("ALTER TABLE video_metadata_scan ADD COLUMN IF NOT EXISTS duration FLOAT;")
        cur.execute("ALTER TABLE video_metadata_scan ADD COLUMN IF NOT EXISTS meta_bytes INTEGER;")
        cur.execute("ALTER TABLE video_metadata_scan ADD COLUMN IF NOT EXISTS thumbnail BYTEA;")
        cur.execute("ALTER TABLE video_metadata_scan ADD COLUMN IF NOT EXISTS is_processed BOOLEAN DEFAULT FALSE;")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Migration Error: {e}")
    finally:
        cur.close()
        db_pool.putconn(conn)

def get_moov_size(url):
    """Smartly fetches only the required metadata bytes (moov atom) for the MSE player."""
    try:
        headers = {'Range': 'bytes=0-2097152'} # Check first 2MB
        r = requests.get(url, headers=headers, timeout=10)
        data = r.content
        offset = 0
        while offset < len(data) - 8:
            size = int.from_bytes(data[offset:offset+4], byteorder='big')
            box_type = data[offset+4:offset+8].decode('ascii', errors='ignore')
            if box_type == 'moov':
                return offset + size + 65536 # Add 64KB buffer for initial segments
            if size < 8:
                break
            offset += size
        return 512 * 1024 # Fallback to 512KB
    except Exception:
        return 512 * 1024

def extract_thumbnail_and_duration(url):
    """Extracts WebP thumbnail (<50kb) and duration without full download."""
    duration = 0.0
    thumbnail_data = None
    
    # 1. Get Duration
    try:
        dur_cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", url]
        dur_res = subprocess.run(dur_cmd, capture_output=True, text=True, timeout=15)
        duration = float(dur_res.stdout.strip())
    except:
        pass

    # 2. Get Thumbnail (Scale width to max 480, height proportional, libwebp quality 40)
    try:
        thumb_cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "error", 
            "-ss", "00:00:01", "-i", url, 
            "-vframes", "1", 
            "-c:v", "libwebp", "-q:v", "40", "-vf", "scale=480:-1", 
            "-f", "webp", "pipe:1"
        ]
        thumb_res = subprocess.run(thumb_cmd, capture_output=True, timeout=20)
        thumbnail_data = thumb_res.stdout
    except:
        pass

    return duration, thumbnail_data

def background_processor():
    """Background thread to process unprocessed videos sequentially."""
    while True:
        conn = db_pool.getconn()
        cur = conn.cursor()
        try:
            # Fetch one un_processed record securely
            cur.execute("""
                SELECT video_num, video_url FROM video_metadata_scan 
                WHERE status = 'Success' AND is_processed = FALSE 
                ORDER BY video_num ASC LIMIT 1 FOR UPDATE SKIP LOCKED;
            """)
            row = cur.fetchone()
            
            if not row:
                conn.commit()
                cur.close()
                db_pool.putconn(conn)
                time.sleep(10) # No work, wait
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
                print(f"[Processed] {v_num} | Dur: {duration}s | Meta: {meta_bytes}B | Thumb: {len(thumb_bytes)//1024}KB")
            else:
                # Mark processed anyway to avoid infinite loop on broken links, but log it
                cur.execute("UPDATE video_metadata_scan SET is_processed = TRUE, status = 'Media Error' WHERE video_num = %s", (v_num,))
                print(f"[Failed processing] {v_num}")
                
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"[Worker Error] {e}")
            time.sleep(5)
        finally:
            cur.close()
            db_pool.putconn(conn)

@app.on_event("startup")
def startup_event():
    run_db_migrations()
    # Start background workers
    for _ in range(WORKERS):
        t = threading.Thread(target=background_processor, daemon=True)
        t.start()

@app.get("/", response_class=HTMLResponse)
def dashboard():
    conn = db_pool.getconn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM video_metadata_scan;")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM video_metadata_scan WHERE status = 'Success';")
        success = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM video_metadata_scan WHERE is_processed = TRUE;")
        processed = cur.fetchone()[0]
        
        remaining = success - processed
        
        html = f"""
        <html><head><title>Video Scan Dashboard</title>
        <style>body{{font-family: Arial; text-align: center; margin-top: 50px; background: #222; color: #fff;}}
        .card{{background: #333; padding: 20px; border-radius: 10px; display: inline-block; margin: 10px;}}
        </style></head>
        <body>
            <h1>Video Metadata & Processing Status</h1>
            <div class="card"><h3>Total Discovered</h3><p>{total}</p></div>
            <div class="card"><h3>Valid URLs</h3><p>{success}</p></div>
            <div class="card"><h3>Processed (Thumb/Meta)</h3><p>{processed}</p></div>
            <div class="card"><h3>Remaining Process Queue</h3><p>{remaining}</p></div>
        </body></html>
        """
        return HTMLResponse(content=html)
    finally:
        cur.close()
        db_pool.putconn(conn)

@app.get("/thumbnail/{video_num}")
def get_thumbnail(video_num: int):
    conn = db_pool.getconn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT thumbnail FROM video_metadata_scan WHERE video_num = %s AND thumbnail IS NOT NULL;", (video_num,))
        row = cur.fetchone()
        if row and row[0]:
            return Response(content=row[0], media_type="image/webp")
        return Response(status_code=404, content="Thumbnail not found or not processed yet.")
    finally:
        cur.close()
        db_pool.putconn(conn)

@app.get("/videos.json", response_class=JSONResponse)
def get_videos_json():
    conn = db_pool.getconn()
    cur = conn.cursor()
    try:
        # Exporting only processed & successful videos
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
                "id": r[0],
                "url": r[1],
                "size_mb": r[2],
                "resolution": r[3],
                "duration": r[4],
                "meta_bytes_required": r[5]
            })
        return JSONResponse(content=data)
    finally:
        cur.close()
        db_pool.putconn(conn)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
