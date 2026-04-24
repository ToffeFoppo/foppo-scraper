from http.server import BaseHTTPRequestHandler
import os
import re
import json
import asyncio
import logging
from datetime import datetime, timezone
from dataclasses import dataclass

import httpx
from bs4 import BeautifulSoup
from rapidfuzz import fuzz
import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("scraper")

MAKE_WEBHOOK_URL   = os.environ.get("MAKE_PRICE_WEBHOOK_URL", "")
GOOGLE_SHEET_ID    = os.environ.get("GOOGLE_SHEET_ID", "")
GOOGLE_CREDENTIALS = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
WC_BASE_URL        = os.environ.get("WC_BASE_URL", "")
WC_CONSUMER_KEY    = os.environ.get("WC_CONSUMER_KEY", "")
WC_CONSUMER_SECRET = os.environ.get("WC_CONSUMER_SECRET", "")

TARGET_BRANDS   = ["samsung", "oneplus", "honor"]
FUZZY_THRESHOLD = 82

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fi-FI,fi;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
}


@dataclass
class RepairEntry:
    source:      str
    raw_title:   str
    model_key:   str
    repair_type: str
    price:       object
    url:         str


SCREEN_KW  = ["näytön", "näyttö", "screen", "display", "nayton", "naytto"]
BATTERY_KW = ["akun", "akku", "battery", "akunvaihto"]

_NOISE = re.compile(
    r"\b(näytön|näyttö|vaihto|korjaus|huolto|screen|display|replacement|"
    r"akun|akku|battery|nayton|naytto|akunvaihto|korjaukset|"
    r"puhelinhuolto|edullisesti|nopeasti|huollot|repair|fix)\b",
    re.IGNORECASE,
)


def classify_repair(text):
    t = text.lower()
    if any(k in t for k in SCREEN_KW):  return "screen"
    if any(k in t for k in BATTERY_KW): return "battery"
    return None


def normalise_model(raw):
    s = _NOISE.sub("", raw)
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


def is_target_brand(model_key):
    return any(b in model_key for b in TARGET_BRANDS)


def parse_price(text):
    if not text:
        return None
    text = text.replace("\xa0", " ")
    m = re.search(r"(\d+)[.,](\d{2})", text)
    if m:
        return float(f"{m.group(1)}.{m.group(2)}")
    m = re.search(r"(\d+)\s*€", text)
    if m:
        return float(m.group(1))
    return None


def make_soup(html):
    return BeautifulSoup(html, "html.parser")


ITAPSA_CATEGORIES = {
    "samsung": {
        "screen":  "https://itapsa.com/tuotteet/huolto/samsung-huollot/samsung-nayton-korjaus/",
        "battery": "https://itapsa.com/tuotteet/huolto/samsung-huollot/samsung-akun-vaihto/",
    },
    "oneplus": {
        "screen":  "https://itapsa.com/tuotteet/huolto/oneplus-huollot/oneplus-nayton-korjaus/",
        "battery": "https://itapsa.com/tuotteet/huolto/oneplus-huollot/oneplus-akun-vaihto/",
    },
    "honor": {
        "screen":  "https://itapsa.com/tuotteet/huolto/huawei-honor-huollot/",
        "battery": "https://itapsa.com/tuotteet/huolto/huawei-honor-huollot/",
    },
}


async def _fonum_get_slugs(client):
    url = "https://www.fonum.fi/hinnasto"
    slugs = []
    try:
        r = await client.get(url, headers=HEADERS, timeout=25)
        r.raise_for_status()
        page = make_soup(r.text)
        tag  = page.find("script", {"id": "__NEXT_DATA__"})
        if tag:
            data  = json.loads(tag.string)
            props = data.get("props", {}).get("pageProps", {})
            for key in ("devices", "models", "brands", "repairItems"):
                items = props.get(key)
                if isinstance(items, list):
                    for item in items:
                        if isinstance(item, dict):
                            slug = item.get("slug", "")
                            if "huolto" in slug:
                                slugs.append({
                                    "slug":  slug.replace("/hinnasto/", "").strip("/"),
                                    "model": item.get("model") or item.get("name") or slug,
                                })
                    if slugs:
                        break
        if not slugs:
            for a in page.find_all("a", href=True):
                href = a["href"]
                if "/hinnasto/" in href and href.count("/") >= 2:
                    slug = href.split("/hinnasto/")[-1].strip("/")
                    if slug and slug != "-huolto":
                        slugs.append({"slug": slug, "model": a.get_text(strip=True)})
    except Exception as exc:
        log.warning(f"[fonum] discovery: {exc}")
    return slugs


async def _fonum_page(client, slug):
    url    = f"https://www.fonum.fi/hinnasto/{slug}"
    screen = battery = None
    try:
        r = await client.get(url, headers=HEADERS, timeout=20)
        if r.status_code == 404:
            return None, None, url
        r.raise_for_status()
        page = make_soup(r.text)
        for h2 in page.find_all("h2"):
            heading    = h2.get_text(strip=True).lower()
            price_text = ""
            for sib in h2.find_next_siblings():
                t = sib.get_text(" ", strip=True)
                if "€" in t or re.search(r"\d+[.,]\d{2}", t):
                    price_text = t
                    break
                if sib.name in ("h2", "h3"):
                    break
            price = parse_price(price_text)
            if not price:
                continue
            if "näytön" in heading or "näytönvaihto" in heading:
                screen = price
            elif "akun" in heading or "akunvaihto" in heading:
                battery = price
    except Exception as exc:
        log.warning(f"[fonum] {slug}: {exc}")
    return screen, battery, url


async def discover_fonum(client):
    entries = []
    slugs   = await _fonum_get_slugs(client)
    target  = [m for m in slugs if any(b in (m["slug"] + m["model"]).lower() for b in TARGET_BRANDS)]
    tasks   = {m["slug"]: _fonum_page(client, m["slug"]) for m in target}
    results = {slug: await coro for slug, coro in tasks.items()}
    for m in target:
        slug          = m["slug"]
        screen, battery, url = results[slug]
        model_key     = normalise_model(m["model"])
        if not is_target_brand(model_key):
            continue
        if screen is not None:
            entries.append(RepairEntry("fonum", m["model"], model_key, "screen",  screen,  url))
        if battery is not None:
            entries.append(RepairEntry("fonum", m["model"], model_key, "battery", battery, url))
    log.info(f"[fonum] {len(entries)} entries")
    return entries


async def _itapsa_category(client, url, repair_type):
    entries  = []
    page_num = 1
    while True:
        paged = url if page_num == 1 else f"{url.rstrip('/')}/page/{page_num}/"
        try:
            r = await client.get(paged, headers=HEADERS, timeout=20)
            if r.status_code == 404:
                break
            r.raise_for_status()
        except Exception as exc:
            log.warning(f"[itapsa] {paged}: {exc}")
            break
        page     = make_soup(r.text)
        products = page.select("li.product, article.product")
        if not products:
            break
        for prod in products:
            title_el = prod.select_one(".woocommerce-loop-product__title, h2, h3")
            price_el = prod.select_one("span.woocommerce-Price-amount.amount bdi, span.woocommerce-Price-amount")
            link_el  = prod.select_one("a.woocommerce-LoopProduct-link")
            if not title_el:
                continue
            raw_title = title_el.get_text(strip=True)
            detected  = classify_repair(raw_title)
            if detected and detected != repair_type:
                continue
            model_key = normalise_model(raw_title)
            if not is_target_brand(model_key):
                continue
            entries.append(RepairEntry(
                "itapsa", raw_title, model_key, repair_type,
                parse_price(price_el.get_text()) if price_el else None,
                link_el["href"] if link_el and link_el.get("href") else paged,
            ))
        if not page.select_one("a.next.page-numbers"):
            break
        page_num += 1
    return entries


async def discover_itapsa(client):
    tasks   = [_itapsa_category(client, url, rt) for brand in ITAPSA_CATEGORIES.values() for rt, url in brand.items()]
    results = await asyncio.gather(*tasks)
    entries = [e for batch in results for e in batch]
    log.info(f"[itapsa] {len(entries)} entries")
    return entries


async def discover_ihelp(client):
    entries  = []
    base_url = "https://ihelp.fi/tuote-osasto/huolto-hinnasto/"
    hdrs     = {**HEADERS, "Cookie": "cookielawinfo-checkbox-necessary=yes", "Referer": "https://ihelp.fi/"}
    page_num = 1
    while True:
        paged = base_url if page_num == 1 else f"{base_url}page/{page_num}/"
        try:
            r = await client.get(paged, headers=hdrs, timeout=25)
            r.raise_for_status()
        except Exception as exc:
            log.warning(f"[ihelp] page {page_num}: {exc}")
            break
        page     = make_soup(r.text)
        products = page.select("li.product, .product-type-simple, .type-product")
        if not products:
            break
        for prod in products:
            title_el    = prod.select_one("h2, .woocommerce-loop-product__title")
            price_el    = prod.select_one("span.woocommerce-Price-amount")
            link_el     = prod.select_one("a")
            if not title_el:
                continue
            raw_title   = title_el.get_text(strip=True)
            repair_type = classify_repair(raw_title)
            if not repair_type:
                continue
            model_key   = normalise_model(raw_title)
            if not is_target_brand(model_key):
                continue
            entries.append(RepairEntry(
                "ihelp", raw_title, model_key, repair_type,
                parse_price(price_el.get_text()) if price_el else None,
                link_el["href"] if link_el and link_el.get("href") else base_url,
            ))
        if not page.select_one("a.next.page-numbers"):
            break
        page_num += 1
    log.info(f"[ihelp] {len(entries)} entries")
    return entries


def fuzzy_merge(all_entries):
    all_keys = list({e.model_key for e in all_entries})
    parent   = {k: k for k in all_keys}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for i, ka in enumerate(all_keys):
        for kb in all_keys[i+1:]:
            if fuzz.token_sort_ratio(ka, kb) >= FUZZY_THRESHOLD:
                union(ka, kb)

    clusters = {}
    for key in all_keys:
        clusters.setdefault(find(key), []).append(key)

    canonical_map = {}
    for members in clusters.values():
        canonical = max(members, key=len)
        for m in members:
            canonical_map[m] = canonical

    merged = {}
    for entry in all_entries:
        canonical = canonical_map.get(entry.model_key, entry.model_key)
        merged.setdefault(canonical, {}).setdefault(entry.repair_type, {})[entry.source] = entry
    return merged


async def lookup_wc_products(client, model_keys):
    mapping = {}
    base    = f"{WC_BASE_URL.rstrip('/')}/wp-json/wc/v3/products"
    auth    = (WC_CONSUMER_KEY, WC_CONSUMER_SECRET)

    async def search(model_key):
        words       = [w for w in model_key.split() if len(w) > 2]
        search_term = " ".join(words[-3:]).title()
        try:
            r = await client.get(base, params={"search": search_term, "per_page": 10}, auth=auth, timeout=15)
            r.raise_for_status()
            for product in r.json():
                name_norm = normalise_model(product.get("name", ""))
                score     = fuzz.token_sort_ratio(model_key, name_norm)
                if score >= FUZZY_THRESHOLD:
                    repair = classify_repair(product.get("name", ""))
                    if repair:
                        key = f"{model_key}|{repair}"
                        if key not in mapping:
                            mapping[key] = product["id"]
        except Exception as exc:
            log.warning(f"[wc] {search_term}: {exc}")

    await asyncio.gather(*[search(k) for k in model_keys])
    return mapping


def get_sheet():
    creds_dict = json.loads(GOOGLE_CREDENTIALS)
    scopes     = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds      = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc         = gspread.authorize(creds)
    sh         = gc.open_by_key(GOOGLE_SHEET_ID)
    try:
        ws = sh.worksheet("PriceLog")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet("PriceLog", rows=5000, cols=10)
        ws.append_row(["Timestamp","CanonicalModel","RepairType","Fonum","iTapsa","iHelp","MinCompetitorPrice","PreviousMin","WC_ProductID"])
    return ws


def get_previous_prices(ws):
    prev = {}
    for row in ws.get_all_records():
        key = f"{row.get('CanonicalModel')}|{row.get('RepairType')}"
        try:
            prev[key] = float(row["MinCompetitorPrice"]) if row["MinCompetitorPrice"] else None
        except (ValueError, TypeError):
            prev[key] = None
    return prev


async def fire_webhook(client, payload):
    try:
        await client.post(MAKE_WEBHOOK_URL, json=payload, timeout=15)
    except Exception as exc:
        log.error(f"[webhook] {exc}")


async def run_scraper():
    ws       = get_sheet()
    previous = get_previous_prices(ws)

    async with httpx.AsyncClient(follow_redirects=True) as client:
        fonum, itapsa, ihelp = await asyncio.gather(
            discover_fonum(client),
            discover_itapsa(client),
            discover_ihelp(client),
        )

    merged = fuzzy_merge(fonum + itapsa + ihelp)

    async with httpx.AsyncClient() as client:
        wc_map = await lookup_wc_products(client, list(merged.keys()))

    ts       = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    new_rows = []
    webhooks = []

    for canonical_model, repair_map in merged.items():
        for repair_type, source_map in repair_map.items():
            prices    = {src: e.price for src, e in source_map.items() if e.price is not None}
            valid     = list(prices.values())
            min_price = min(valid) if valid else None
            key       = f"{canonical_model}|{repair_type}"
            prev_min  = previous.get(key)
            wc_id     = wc_map.get(key)

            new_rows.append([ts, canonical_model, repair_type,
                prices.get("fonum"), prices.get("itapsa"), prices.get("ihelp"),
                min_price, prev_min, wc_id or ""])

            if min_price is not None and prev_min is not None and min_price != prev_min:
                cheapest = min(prices, key=lambda s: prices[s])
                webhooks.append({
                    "model":                  canonical_model,
                    "repair_type":            repair_type,
                    "competitor":             cheapest,
                    "competitor_price":       min_price,
                    "previous_price":         prev_min,
                    "suggested_price":        min_price,
                    "source_url":             source_map[cheapest].url,
                    "woocommerce_product_id": wc_id,
                    "all_prices":             prices,
                })

    if new_rows:
        ws.append_rows(new_rows, value_input_option="USER_ENTERED")

    async with httpx.AsyncClient() as client:
        await asyncio.gather(*[fire_webhook(client, p) for p in webhooks])

    return {"raw_entries": len(fonum+itapsa+ihelp), "canonical_models": len(merged), "price_changes": len(webhooks), "timestamp": ts}


class handler(BaseHTTPRequestHandler):

    def do_GET(self):
        cron_secret = os.environ.get("CRON_SECRET", "")
        auth        = self.headers.get("Authorization", "")
        if cron_secret and auth != f"Bearer {cron_secret}":
            self._send(401, {"error": "Unauthorized"})
            return
        try:
            summary = asyncio.run(run_scraper())
            self._send(200, {"status": "ok", **summary})
        except Exception as exc:
            log.exception("Scraper crashed")
            self._send(500, {"status": "error", "detail": str(exc)})

    def _send(self, code, data):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass
