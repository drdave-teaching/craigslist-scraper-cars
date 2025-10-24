# cloud_function/main.py (titles-only -> .txt)
import os, io, datetime as dt, requests, time
from bs4 import BeautifulSoup
from google.cloud import storage
from flask import Request, make_response, jsonify

BUCKET_NAME  = os.environ["BUCKET_NAME"]
BASE_SITE    = os.environ.get("BASE_SITE", "https://newhaven.craigslist.org")
SEARCH_PATH  = os.environ.get("SEARCH_PATH", "/search/cta")
MAX_PAGES    = int(os.environ.get("MAX_PAGES", "1"))
DELAY_SECS   = float(os.environ.get("DELAY_SECS", "1.0"))
USER_AGENT   = os.environ.get("USER_AGENT", "UConn-OPIM-Student-Scraper/1.0")

HDRS = {"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.8"}

def _page_url(base: str, path: str, page: int) -> str:
    if page == 0:
        return f"{base}{path}?hasPic=1&srchType=T"
    return f"{base}{path}?hasPic=1&srchType=T&s={page*120}"

def entrypoint(request: Request):
    titles = []
    for p in range(MAX_PAGES):
        url = _page_url(BASE_SITE, SEARCH_PATH, p)
        r = requests.get(url, headers=HDRS, timeout=25)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for el in soup.select(".result-title, a.titlestring"):
            t = el.get_text(strip=True)
            if t:
                titles.append(t)
        if p < MAX_PAGES - 1:
            time.sleep(DELAY_SECS)

    ts = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    object_name = f"scrapes/titles_{ts}.txt"
    buf = io.StringIO("\n".join(titles) + "\n")
    storage.Client().bucket(BUCKET_NAME).blob(object_name)\
        .upload_from_string(buf.getvalue(), content_type="text/plain")

    return jsonify({"ok": True, "items": len(titles), "txt": object_name})
