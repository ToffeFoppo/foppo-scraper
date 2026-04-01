"""
api/scrape.py  —  Fully Automated Competitor Price Scraper
Vercel Serverless Function, triggered daily by Vercel Cron.

Architecture (zero manual slug config):
  1. DISCOVERY  — each scraper crawls competitor sites and returns
                  a flat list of {model, repair_type, price, url}
  2. NORMALIZATION — model names are cleaned to a canonical form
  3. FUZZY MATCH  — rapidfuzz links the same model across all three sites
  4. WC LOOKUP    — your own WooCommerce products are found via REST API search
  5. DIFF         — compare against Google Sheet baseline
  6. ALERT        — fire Make.com webhook on any price change
"""

import os, re, json, asyncio, logging
from datetime import datetime, timezone
from dataclasses import dataclass, field

import httpx
from bs4 import BeautifulSoup
from rapidfuzz import fuzz, process
import gspread
from google.oauth2.service_account import Credentials

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("scraper")

MAKE_WEBHOOK_URL   = os.environ["MAKE_PRICE_WEBHOOK_URL"]
GOOGLE_SHEET_ID    = os.environ["GOOGLE_SHEET_ID"]
GOOGLE_CREDENTIALS = os.environ["GOOGLE_CREDENTIALS_JSON"]
WC_BASE_URL        = os.environ["WC_BASE_URL"]          # https://foppo.fi
WC_CONSUMER_KEY    = os.environ["WC_CONSUMER_KEY"]
WC_CONSUMER_SECRET = os.environ["WC_CONSUMER_SECRET"]

# Only track these brands (case-insensitive substring match on normalised name)
TARGET_BRANDS = ["samsung", "oneplus", "honor"]

# Fuzzy match threshold — 0-100. 82 works well for model names.
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

# ─────────────────────────────────────────────────────────────────────────────
# DATA MODEL
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class RepairEntry:
    source: str          # "fonum" | "itapsa" | "ihelp"
    raw_title: str       # original title from site
    model_key: str       # normalised canonical key, e.g. "samsung galaxy s24"
    repair_type: str     # "screen" | "battery"
    price: float | None
    url: str


# ─────────────────────────────────────────────────────────────────────────────
# NORMALISATION HELPERS
# ─────────────────────────────────────────────────────────────────────────────

# Finnish repair-type keyword → canonical type
SCREEN_KW  = ["näytön", "näyttö", "screen", "display", "nayton", "naytto"]
BATTERY_KW = ["akun", "akku", "battery", "akunvaihto", "akuvaihto"]

def classify_repair(text: str) -> str | None:
    t = text.lower()
    if any(k in t for k in SCREEN_KW):  return "screen"
    if any(k in t for k in BATTERY_KW): return "battery"
    return None


# Noise words to strip before fuzzy comparison
_NOISE = re.compile(
    r"\b(näytön|näyttö|vaihto|korjaus|huolto|screen|display|replacement|"
    r"akun|akku|battery|nayton|naytto|akunvaihto|korjaukset|"
    r"puhelinhuolto|edullisesti|nopeasti|huollot|repair|fix)\b",
    re.IGNORECASE,
)

def normalise_model(raw: str) -> str:
    """
    'Samsung Galaxy S24 Näytön Vaihto' → 'samsung galaxy s24'
    'Galaxy A55 akun vaihto'           → 'galaxy a55'        (→ fuzzy-matches 'samsung galaxy a55')
    """
    s = _NOISE.sub("", raw)
    s = re.sub(r"[^\w\s]", " ", s)   # strip punctuation
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


def is_target_brand(model_key: str) -> bool:
    return any(b in model_key for b in TARGET_BRANDS)


def parse_price(text: str) -> float | None:
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


def soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


# ─────────────────────────────────────────────────────────────────────────────
# ① FONUM DISCOVERY
#
# Strategy: Fonum is a Next.js / Contentful site.
# The main hinnasto page embeds __NEXT_DATA__ JSON that contains every
# brand→model mapping used to populate the dropdown selector.
# We extract that JSON, iterate all model slugs, then fetch each model page
# concurrently to get screen + battery prices.
# ─────────────────────────────────────────────────────────────────────────────

async def _fonum_get_model_slugs(client: httpx.AsyncClient) -> list[dict]:
    """
    Returns list of {brand, model, slug} from Fonum's embedded Next.js data.
    Falls back to an empty list if the JSON structure changes.
    """
    url = "https://www.fonum.fi/hinnasto"
    slugs = []
    try:
        r = await client.get(url, headers=HEADERS, timeout=25)
        r.raise_for_status()
        page = soup(r.text)

        # Next.js embeds all page props in <script id="__NEXT_DATA__">
        next_data_tag = page.find("script", {"id": "__NEXT_DATA__"})
        if not next_data_tag:
            log.warning("[fonum] __NEXT_DATA__ not found — trying link-scan fallback")
            return await _fonum_slug_fallback(client, page)

        data = json.loads(next_data_tag.string)

        # Drill into pageProps — the exact path depends on Fonum's page structure.
        # Common patterns tried in order:
        props = data.get("props", {}).get("pageProps", {})

        # Pattern A: devices/models array at top level
        for key in ("devices", "models", "brands", "repairItems", "hinnastoItems"):
            items = props.get(key)
            if isinstance(items, list):
                for item in items:
                    s = _fonum_extract_slug(item)
                    if s:
                        slugs.append(s)
                if slugs:
                    break

        # Pattern B: nested under a content/page key
        if not slugs:
            for val in props.values():
                if isinstance(val, list) and len(val) > 5:
                    for item in val:
                        s = _fonum_extract_slug(item)
                        if s:
                            slugs.append(s)
                    if len(slugs) > 5:
                        break

        if slugs:
            log.info(f"[fonum] discovered {len(slugs)} models from __NEXT_DATA__")
        else:
            log.warning("[fonum] no models in __NEXT_DATA__ — using link-scan fallback")
            return await _fonum_slug_fallback(client, page)

    except Exception as exc:
        log.warning(f"[fonum] discovery error: {exc}")

    return slugs


def _fonum_extract_slug(item: dict) -> dict | None:
    """Try to extract {brand, model, slug} from a Fonum JSON item."""
    if not isinstance(item, dict):
        return None
    slug = item.get("slug") or item.get("url") or item.get("href") or ""
    brand = item.get("brand") or item.get("make") or ""
    model = item.get("model") or item.get("name") or item.get("title") or ""
    if slug and ("huolto" in slug or "hinnasto" in slug):
        # normalise slug to just the path segment
        slug = slug.replace("/hinnasto/", "").strip("/")
        return {"brand": brand, "model": model, "slug": slug}
    return None


async def _fonum_slug_fallback(
    client: httpx.AsyncClient, page: BeautifulSoup
) -> list[dict]:
    """
    Fallback: scan all <a> tags on the hinnasto page for model-page links.
    Pattern: /hinnasto/{slug}  where slug contains a brand name.
    """
    found = []
    for a in page.find_all("a", href=True):
        href = a["href"]
        if "/hinnasto/" in href and href.count("/") >= 2:
            slug = href.split("/hinnasto/")[-1].strip("/")
            if slug and slug != "-huolto":
                found.append({"brand": "", "model": a.get_text(strip=True), "slug": slug})
    log.info(f"[fonum] fallback found {len(found)} links")
    return found


async def _fonum_scrape_model_page(
    client: httpx.AsyncClient, slug: str
) -> tuple[float | None, float | None, str]:
    """Fetch one Fonum model page, return (screen_price, battery_price, url)."""
    url = f"https://www.fonum.fi/hinnasto/{slug}"
    screen = battery = None
    try:
        r = await client.get(url, headers=HEADERS, timeout=20)
        if r.status_code == 404:
            return None, None, url
        r.raise_for_status()
        page = soup(r.text)

        for h2 in page.find_all("h2"):
            heading = h2.get_text(strip=True).lower()
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
        log.warning(f"[fonum] page error {slug}: {exc}")
    return screen, battery, url


async def discover_fonum(client: httpx.AsyncClient) -> list[RepairEntry]:
    entries = []
    model_slugs = await _fonum_get_model_slugs(client)

    # Filter to target brands only
    target_slugs = [
        m for m in model_slugs
        if any(b in (m["slug"] + m.get("brand", "")).lower() for b in TARGET_BRANDS)
    ]
    log.info(f"[fonum] scraping {len(target_slugs)} target-brand model pages")

    # Fetch all pages concurrently (Fonum is well-behaved, no rate limit needed)
    tasks = {m["slug"]: _fonum_scrape_model_page(client, m["slug"]) for m in target_slugs}
    results = {slug: await coro for slug, coro in tasks.items()}

    for m in target_slugs:
        slug = m["slug"]
        screen, battery, url = results[slug]
        model_key = normalise_model(m.get("model") or slug.replace("-huolto", "").replace("-", " "))

        if not is_target_brand(model_key):
            continue

        if screen is not None:
            entries.append(RepairEntry("fonum", m.get("model",""), model_key, "screen", screen, url))
        if battery is not None:
            entries.append(RepairEntry("fonum", m.get("model",""), model_key, "battery", battery, url))

    log.info(f"[fonum] discovered {len(entries)} entries")
    return entries


# ─────────────────────────────────────────────────────────────────────────────
# ② ITAPSA DISCOVERY
#
# Strategy: iTapsa has WooCommerce subcategory pages per brand+repair_type:
#   /tuotteet/huolto/samsung-huollot/samsung-nayton-korjaus/   ← all Galaxy screen
#   /tuotteet/huolto/samsung-huollot/samsung-akun-vaihto/      ← all Galaxy battery
#   /tuotteet/huolto/oneplus-huollot/oneplus-nayton-korjaus/
#   ...etc.
# Each page lists models as WooCommerce li.product cards with title + price.
# We paginate through each subcategory to get every model automatically.
# ─────────────────────────────────────────────────────────────────────────────

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


async def _itapsa_scrape_category(
    client: httpx.AsyncClient, url: str, repair_type: str
) -> list[RepairEntry]:
    """Paginate a WooCommerce category page and return all RepairEntry objects."""
    entries = []
    page_num = 1

    while True:
        paged_url = url if page_num == 1 else f"{url.rstrip('/')}/page/{page_num}/"
        try:
            r = await client.get(paged_url, headers=HEADERS, timeout=20)
            if r.status_code == 404:
                break
            r.raise_for_status()
        except Exception as exc:
            log.warning(f"[itapsa] {paged_url}: {exc}")
            break

        page = soup(r.text)
        products = page.select("li.product, article.product")
        if not products:
            break

        for prod in products:
            title_el = prod.select_one(
                ".woocommerce-loop-product__title, h2.woocommerce-loop-product__title, h3"
            )
            price_el  = prod.select_one("span.woocommerce-Price-amount.amount bdi, span.woocommerce-Price-amount")
            link_el   = prod.select_one("a.woocommerce-LoopProduct-link")

            if not title_el:
                continue

            raw_title = title_el.get_text(strip=True)
            price     = parse_price(price_el.get_text()) if price_el else None
            url_prod  = link_el["href"] if link_el and link_el.get("href") else paged_url

            # Determine repair type from title (for honor page which mixes both)
            detected = classify_repair(raw_title)
            if detected and detected != repair_type:
                continue  # wrong repair type on mixed page

            model_key = normalise_model(raw_title)
            if not is_target_brand(model_key) and not any(
                b in url for b in TARGET_BRANDS
            ):
                continue

            entries.append(RepairEntry("itapsa", raw_title, model_key, repair_type, price, url_prod))

        if not page.select_one("a.next.page-numbers"):
            break
        page_num += 1

    return entries


async def discover_itapsa(client: httpx.AsyncClient) -> list[RepairEntry]:
    tasks = []
    for brand, repairs in ITAPSA_CATEGORIES.items():
        for repair_type, url in repairs.items():
            tasks.append(_itapsa_scrape_category(client, url, repair_type))

    results = await asyncio.gather(*tasks)
    entries = [e for batch in results for e in batch]
    log.info(f"[itapsa] discovered {len(entries)} entries")
    return entries


# ─────────────────────────────────────────────────────────────────────────────
# ③ IHELP DISCOVERY
#
# Strategy: Paginate the full hinnasto category, classify each product
# by repair type from its title. Already robust — just needs brand filter.
# ─────────────────────────────────────────────────────────────────────────────

async def discover_ihelp(client: httpx.AsyncClient) -> list[RepairEntry]:
    entries = []
    base_url = "https://ihelp.fi/tuote-osasto/huolto-hinnasto/"
    ihelp_headers = {
        **HEADERS,
        "Cookie": "cookielawinfo-checkbox-necessary=yes; cookielawinfo-checkbox-analytics=no",
        "Referer": "https://ihelp.fi/",
    }
    page_num = 1

    while True:
        paged_url = base_url if page_num == 1 else f"{base_url}page/{page_num}/"
        try:
            r = await client.get(paged_url, headers=ihelp_headers, timeout=25)
            if r.status_code in (404, 415):
                # Try without trailing slash
                r = await client.get(paged_url.rstrip("/"), headers=ihelp_headers, timeout=25)
            r.raise_for_status()
        except Exception as exc:
            log.warning(f"[ihelp] page {page_num}: {exc}")
            break

        page = soup(r.text)
        products = page.select("li.product, .product-type-simple, .type-product")
        if not products:
            break

        for prod in products:
            title_el = prod.select_one(
                "h2, .woocommerce-loop-product__title, .entry-title"
            )
            price_el = prod.select_one("span.woocommerce-Price-amount")
            link_el  = prod.select_one("a")

            if not title_el:
                continue

            raw_title    = title_el.get_text(strip=True)
            repair_type  = classify_repair(raw_title)
            if not repair_type:
                continue

            model_key = normalise_model(raw_title)
            if not is_target_brand(model_key):
                continue

            entries.append(RepairEntry(
                source      = "ihelp",
                raw_title   = raw_title,
                model_key   = model_key,
                repair_type = repair_type,
                price       = parse_price(price_el.get_text()) if price_el else None,
                url         = link_el["href"] if link_el and link_el.get("href") else base_url,
            ))

        if not page.select_one("a.next.page-numbers"):
            break
        page_num += 1

    log.info(f"[ihelp] discovered {len(entries)} entries")
    return entries


# ─────────────────────────────────────────────────────────────────────────────
# ④ FUZZY MATCHING  —  merge entries across all three sources
#
# Groups entries by repair_type, then uses rapidfuzz to cluster model_keys
# that refer to the same physical model. Returns a unified dict:
#   { canonical_model_key: { repair_type: { source: RepairEntry } } }
# ─────────────────────────────────────────────────────────────────────────────

def fuzzy_merge(all_entries: list[RepairEntry]) -> dict:
    """
    Returns:
      {
        "samsung galaxy s24": {
          "screen":  {"fonum": RepairEntry, "itapsa": RepairEntry, "ihelp": RepairEntry},
          "battery": {"fonum": RepairEntry, ...},
        },
        ...
      }
    """
    # Collect all unique model_keys
    all_keys = list({e.model_key for e in all_entries})

    # Union-Find to cluster similar keys
    parent = {k: k for k in all_keys}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            # Keep the longer/more descriptive key as canonical
            parent[rb] = ra if len(ra) >= len(rb) else ra

    # Cluster keys that are similar enough
    for i, key_a in enumerate(all_keys):
        for key_b in all_keys[i+1:]:
            score = fuzz.token_sort_ratio(key_a, key_b)
            if score >= FUZZY_THRESHOLD:
                union(key_a, key_b)

    # Pick canonical key as the longest in each cluster
    clusters: dict[str, list[str]] = {}
    for key in all_keys:
        root = find(key)
        clusters.setdefault(root, []).append(key)

    canonical_map = {}  # old_key → canonical_key
    for root, members in clusters.items():
        canonical = max(members, key=len)
        for m in members:
            canonical_map[m] = canonical

    # Build the merged result
    merged: dict[str, dict] = {}
    for entry in all_entries:
        canonical = canonical_map.get(entry.model_key, entry.model_key)
        merged.setdefault(canonical, {}).setdefault(entry.repair_type, {})[entry.source] = entry

    return merged


# ─────────────────────────────────────────────────────────────────────────────
# ⑤ WOOCOMMERCE PRODUCT LOOKUP  (your own products, auto-discovered)
#
# Uses the WC REST API to search products by the canonical model name.
# Returns a dict: { "{model_key}|{repair_type}": wc_product_id }
# ─────────────────────────────────────────────────────────────────────────────

async def lookup_wc_products(
    client: httpx.AsyncClient, model_keys: list[str]
) -> dict[str, int]:
    """
    For each model key, search WooCommerce for a matching product
    whose name contains the model name. Returns best match per key.
    """
    mapping = {}
    base = f"{WC_BASE_URL.rstrip('/')}/wp-json/wc/v3/products"
    auth = (WC_CONSUMER_KEY, WC_CONSUMER_SECRET)

    async def search_model(model_key: str):
        # Use the most distinctive part of the model name as the search term
        # e.g. "samsung galaxy s24" → "Galaxy S24"
        words = [w for w in model_key.split() if len(w) > 2]
        search_term = " ".join(words[-3:]).title()  # last 3 words, title-cased

        try:
            r = await client.get(
                base,
                params={"search": search_term, "per_page": 10, "status": "publish"},
                auth=auth,
                timeout=15,
            )
            r.raise_for_status()
            products = r.json()
        except Exception as exc:
            log.warning(f"[wc_lookup] {search_term}: {exc}")
            return

        for product in products:
            name_norm = normalise_model(product.get("name", ""))
            score = fuzz.token_sort_ratio(model_key, name_norm)
            if score >= FUZZY_THRESHOLD:
                repair = classify_repair(product.get("name", ""))
                if repair:
                    key = f"{model_key}|{repair}"
                    # Prefer higher-scoring match
                    if key not in mapping:
                        mapping[key] = product["id"]
                        log.info(f"[wc_lookup] {key} → product #{product['id']} (score {score})")

    await asyncio.gather(*[search_model(k) for k in model_keys])
    return mapping


# ─────────────────────────────────────────────────────────────────────────────
# ⑥ GOOGLE SHEETS  —  price baseline
# ─────────────────────────────────────────────────────────────────────────────

def get_sheet():
    creds_dict = json.loads(GOOGLE_CREDENTIALS)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(GOOGLE_SHEET_ID)
    try:
        ws = sh.worksheet("PriceLog")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet("PriceLog", rows=5000, cols=12)
        ws.append_row([
            "Timestamp", "CanonicalModel", "RepairType",
            "Fonum", "iTapsa", "iHelp",
            "MinCompetitorPrice", "PreviousMin", "WC_ProductID",
        ])
    return ws


def get_previous_prices(ws) -> dict:
    records = ws.get_all_records()
    prev = {}
    for row in records:
        key = f"{row.get('CanonicalModel')}|{row.get('RepairType')}"
        try:
            prev[key] = float(row["MinCompetitorPrice"]) if row["MinCompetitorPrice"] else None
        except (ValueError, TypeError):
            prev[key] = None
    return prev


# ─────────────────────────────────────────────────────────────────────────────
# ⑦ MAKE.COM WEBHOOK
# ─────────────────────────────────────────────────────────────────────────────

async def fire_webhook(client: httpx.AsyncClient, payload: dict):
    try:
        r = await client.post(MAKE_WEBHOOK_URL, json=payload, timeout=15)
        log.info(f"[webhook] {payload['model']} {payload['repair_type']}: HTTP {r.status_code}")
    except Exception as exc:
        log.error(f"[webhook] failed: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# ⑧ MAIN ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────

async def run_scraper():
    ws       = get_sheet()
    previous = get_previous_prices(ws)

    async with httpx.AsyncClient(follow_redirects=True) as client:
        # --- Discovery (all three sites in parallel) ---
        fonum_entries, itapsa_entries, ihelp_entries = await asyncio.gather(
            discover_fonum(client),
            discover_itapsa(client),
            discover_ihelp(client),
        )

    all_entries = fonum_entries + itapsa_entries + ihelp_entries
    log.info(f"[main] total raw entries: {len(all_entries)}")

    # --- Fuzzy merge ---
    merged = fuzzy_merge(all_entries)
    log.info(f"[main] merged into {len(merged)} unique canonical models")

    # --- WooCommerce product lookup ---
    async with httpx.AsyncClient() as client:
        wc_map = await lookup_wc_products(client, list(merged.keys()))

    # --- Diff + sheet update + webhook ---
    ts       = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    new_rows = []
    webhooks = []

    for canonical_model, repair_map in merged.items():
        for repair_type, source_map in repair_map.items():
            prices = {
                src: entry.price
                for src, entry in source_map.items()
                if entry.price is not None
            }
            valid     = list(prices.values())
            min_price = min(valid) if valid else None

            key      = f"{canonical_model}|{repair_type}"
            prev_min = previous.get(key)
            wc_id    = wc_map.get(key)

            new_rows.append([
                ts,
                canonical_model,
                repair_type,
                prices.get("fonum"),
                prices.get("itapsa"),
                prices.get("ihelp"),
                min_price,
                prev_min,
                wc_id or "",
            ])

            if min_price is not None and prev_min is not None and min_price != prev_min:
                cheapest_src = min(prices, key=lambda s: prices[s])
                ref_entry    = source_map[cheapest_src]
                webhooks.append({
                    "model":                canonical_model,
                    "repair_type":          repair_type,
                    "competitor":           cheapest_src,
                    "competitor_price":     min_price,
                    "previous_price":       prev_min,
                    "suggested_price":      min_price,
                    "source_url":           ref_entry.url,
                    "woocommerce_product_id": wc_id,
                    "all_prices":           prices,
                    "all_raw_titles": {
                        src: e.raw_title for src, e in source_map.items()
                    },
                })
                log.info(f"[change] {canonical_model} {repair_type}: {prev_min} → {min_price}")
            elif min_price is not None and prev_min is None:
                log.info(f"[new] {canonical_model} {repair_type}: first seen {min_price}")

    if new_rows:
        ws.append_rows(new_rows, value_input_option="USER_ENTERED")

    async with httpx.AsyncClient() as client:
        await asyncio.gather(*[fire_webhook(client, p) for p in webhooks])

    return {
        "raw_entries":        len(all_entries),
        "canonical_models":   len(merged),
        "price_changes":      len(webhooks),
        "wc_products_found":  len(wc_map),
        "timestamp":          ts,
    }


# ─────────────────────────────────────────────────────────────────────────────
# VERCEL HANDLER
# ─────────────────────────────────────────────────────────────────────────────

def handler(request, response):
    auth         = request.headers.get("Authorization", "")
    cron_secret  = os.environ.get("CRON_SECRET", "")
    if cron_secret and auth != f"Bearer {cron_secret}":
        response.status_code = 401
        return response.json({"error": "Unauthorized"})
    try:
        summary = asyncio.run(run_scraper())
        return response.json({"status": "ok", **summary})
    except Exception as exc:
        log.exception("Scraper crashed")
        response.status_code = 500
        return response.json({"status": "error", "detail": str(exc)})
