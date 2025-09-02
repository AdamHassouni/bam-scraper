import os, re, sys, hashlib, sqlite3, io
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup
import pandas as pd

# -----------------------------
# Config
# -----------------------------
PAGE_URL = "https://www.bkam.ma/Marches/Principaux-indicateurs/Marche-obligataire/Marche-des-bons-de-tresor/Marche-secondaire/Taux-de-reference-des-bons-du-tresor"
USER_AGENT = "Mozilla/5.0 (compatible; BKAM-Scraper/1.0; +https://example.org)"
DB_PATH = os.environ.get("BKAM_DB", "bkam_state.sqlite")
OUT_DIR = os.environ.get("BKAM_OUT", "out_bkam")
TIMEOUT = 30

TELEGRAM_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.environ.get("TG_CHAT_ID")

os.makedirs(OUT_DIR, exist_ok=True)

# -----------------------------
# Notifications
# -----------------------------
def notify(msg: str):
    """Send a Telegram notification if credentials exist"""
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                data={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
                timeout=15
            )
        except Exception:
            pass

# -----------------------------
# DB state
# -----------------------------
def get_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS seen (
        key TEXT PRIMARY KEY,
        value TEXT,
        ts TEXT
    )""")
    return con

def get_seen_hash(con, key: str) -> str | None:
    row = con.execute("SELECT value FROM seen WHERE key=?", (key,)).fetchone()
    return row[0] if row else None

def set_seen_hash(con, value: str, key: str):
    con.execute("REPLACE INTO seen(key, value, ts) VALUES(?,?,?)",
                (key, value, datetime.now(timezone.utc).isoformat()))
    con.commit()

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

# -----------------------------
# Scraping
# -----------------------------
def fetch_reference_page() -> str:
    """Fetch BKAM reference page HTML"""
    r = requests.get(PAGE_URL, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text

def extract_csv_url(html: str) -> str | None:
    """Find candidate CSV download link in page HTML"""
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        text = (a.get_text() or "").strip().lower()
        href = a["href"]
        if "csv" in href.lower() or "csv" in text:
            if "telechargement" in text or "download" in text or href.lower().endswith(".csv"):
                from urllib.parse import urljoin
                return href if href.startswith("http") else urljoin(PAGE_URL, href)
    return None

def download_csv(csv_url: str) -> bytes | None:
    """Try to download CSV and validate"""
    r = requests.get(csv_url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
    if r.status_code == 200 and "csv" in r.headers.get("Content-Type","").lower():
        return r.content
    head = r.content[:256].decode("utf-8", errors="ignore")
    if "," in head or ";" in head:
        return r.content
    return None

def parse_reference_table(html: str) -> pd.DataFrame:
    """Fallback: parse HTML table if no CSV available"""
    dfs = pd.read_html(html, flavor="bs4")
    dfs.sort(key=lambda df: df.shape[1], reverse=True)
    return dfs[0]

# -----------------------------
# Saving
# -----------------------------
def save_reference_data(df: pd.DataFrame, raw_bytes: bytes | None, tag: str) -> str:
    """Save raw + clean CSV, return path of clean file"""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if raw_bytes is not None:
        raw_path = os.path.join(OUT_DIR, f"bkam_raw_{tag}_{ts}.csv")
        with open(raw_path, "wb") as f:
            f.write(raw_bytes)
    clean = df.copy()
    clean.columns = [re.sub(r"\s+", " ", str(c)).strip() for c in clean.columns]
    clean_path = os.path.join(OUT_DIR, f"bkam_clean_{tag}_{ts}.csv")
    clean.to_csv(clean_path, index=False, encoding="utf-8-sig")
    return clean_path

# -----------------------------
# Main update logic
# -----------------------------
def update_reference_data(dedupe: bool = True) -> str | None:
    """
    Scrape BAM website, save CSV(s).
    Returns: path to CLEAN CSV if updated, else None.
    """
    con = get_db()
    html = fetch_reference_page()

    # Try CSV first
    csv_url = extract_csv_url(html)
    if csv_url:
        raw_bytes = download_csv(csv_url)
        if raw_bytes:
            current_hash = sha256_bytes(raw_bytes)
            last_hash = get_seen_hash(con, key="reference_csv_hash")
            if dedupe and last_hash == current_hash:
                print("No change detected (CSV).")
                return None
            df = pd.read_csv(io.BytesIO(raw_bytes))
            clean_path = save_reference_data(df, raw_bytes, tag="csv")
            set_seen_hash(con, current_hash, key="reference_csv_hash")
            notify(f"BKAM T-Bond reference rates UPDATED (CSV). Saved: {clean_path}")
            return clean_path

    # Fallback to HTML
    df = parse_reference_table(html)
    table_bytes = df.to_csv(index=False).encode("utf-8")
    current_hash = sha256_bytes(table_bytes)
    last_hash = get_seen_hash(con, key="reference_html_hash")
    if dedupe and last_hash == current_hash:
        print("No change detected (HTML).")
        return None
    clean_path = save_reference_data(df, raw_bytes=None, tag="html")
    set_seen_hash(con, current_hash, key="reference_html_hash")
    notify(f"BKAM T-Bond reference rates UPDATED (HTML). Saved: {clean_path}")
    return clean_path

if __name__ == "__main__":
    try:
        updated_file = update_reference_data()
        if updated_file:
            print(f"[INFO] New data saved at {updated_file}")
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        notify(f"[ERROR] BKAM scraper: {e}")
        sys.exit(1)
