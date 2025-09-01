

import os, re, sys, time, hashlib, sqlite3, io
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup
import pandas as pd

PAGE_URL = "https://www.bkam.ma/Marches/Principaux-indicateurs/Marche-obligataire/Marche-des-bons-de-tresor/Marche-secondaire/Taux-de-reference-des-bons-du-tresor"
USER_AGENT = "Mozilla/5.0 (compatible; BKAM-Scraper/1.0; +https://example.org)"
DB_PATH = os.environ.get("BKAM_DB", "bkam_state.sqlite")
OUT_DIR = os.environ.get("BKAM_OUT", "out_bkam")
TIMEOUT = 30

TELEGRAM_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.environ.get("TG_CHAT_ID")

os.makedirs(OUT_DIR, exist_ok=True)

def notify(msg: str):
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                data={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
                timeout=15
            )
        except Exception:
            pass

def get_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS seen (
        key TEXT PRIMARY KEY,
        value TEXT,
        ts TEXT
    )""")
    return con

def get_seen_hash(con, key="reference_csv_hash"):
    row = con.execute("SELECT value FROM seen WHERE key=?", (key,)).fetchone()
    return row[0] if row else None

def set_seen_hash(con, value, key="reference_csv_hash"):
    con.execute("REPLACE INTO seen(key, value, ts) VALUES(?,?,?)",
                (key, value, datetime.now(timezone.utc).isoformat()))
    con.commit()

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def find_csv_link(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    candidates = []
    for a in soup.find_all("a", href=True):
        text = (a.get_text() or "").strip().lower()
        href = a["href"]
        if "csv" in href.lower() or "csv" in text:
            if "telechargement" in text or "download" in text or href.lower().endswith(".csv"):
                candidates.append(href)
    if not candidates:
        return None
    csv_href = candidates[0]
    if csv_href.startswith("http"):
        return csv_href
    from urllib.parse import urljoin
    return urljoin(PAGE_URL, csv_href)

def fetch(url: str) -> requests.Response:
    return requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)

def parse_table_from_html(html: str) -> pd.DataFrame:
    dfs = pd.read_html(html, flavor="bs4")
    dfs.sort(key=lambda df: df.shape[1], reverse=True)
    return dfs[0]

def save_artifacts(df: pd.DataFrame, raw_bytes: bytes | None, tag: str):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if raw_bytes is not None:
        csv_path = os.path.join(OUT_DIR, f"bkam_reference_rates_raw_{tag}_{ts}.csv")
        with open(csv_path, "wb") as f:
            f.write(raw_bytes)
    else:
        csv_path = os.path.join(OUT_DIR, f"bkam_reference_rates_parsed_{tag}_{ts}.csv")
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    clean = df.copy()
    clean.columns = [re.sub(r"\s+", " ", str(c)).strip() for c in clean.columns]
    clean_path = os.path.join(OUT_DIR, f"bkam_reference_rates_clean_{tag}_{ts}.csv")
    clean.to_csv(clean_path, index=False, encoding="utf-8-sig")
    return csv_path, clean_path

def main():
    con = get_db()
    r = fetch(PAGE_URL)
    r.raise_for_status()
    html = r.text

    csv_url = find_csv_link(html)
    raw_bytes = None
    tag = "csv"
    if csv_url:
        r_csv = fetch(csv_url)
        if r_csv.status_code == 200 and r_csv.headers.get("Content-Type","").lower().find("csv") != -1:
            raw_bytes = r_csv.content
        else:
            content = r_csv.content
            head = content[:256].decode("utf-8", errors="ignore")
            if "," in head or ";" in head:
                raw_bytes = content

    if raw_bytes is not None:
        current_hash = sha256_bytes(raw_bytes)
        last_hash = get_seen_hash(con)
        if last_hash == current_hash:
            print("No change detected (CSV).")
            return
        df = pd.read_csv(io.BytesIO(raw_bytes))
        csv_path, clean_path = save_artifacts(df, raw_bytes, tag="csv")
        set_seen_hash(con, current_hash)
        msg = f"BKAM T-Bond reference rates UPDATED (CSV). Saved:\n{csv_path}\n{clean_path}"
        print(msg)
        notify(msg)
        return

    df = parse_table_from_html(html)
    table_bytes = df.to_csv(index=False).encode("utf-8")
    current_hash = sha256_bytes(table_bytes)
    last_hash = get_seen_hash(con, key="reference_html_hash")
    if last_hash == current_hash:
        print("No change detected (HTML table).")
        return
    csv_path, clean_path = save_artifacts(df, raw_bytes=None, tag="html")
    set_seen_hash(con, current_hash, key="reference_html_hash")
    msg = f"BKAM T-Bond reference rates UPDATED (HTML). Saved:\n{csv_path}\n{clean_path}"
    print(msg)
    notify(msg)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        notify(f"[ERROR] BKAM scraper: {e}")
        sys.exit(1)
