from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import os
import json
import httpx

WC_BASE_URL        = os.environ.get("WC_BASE_URL", "")
WC_CONSUMER_KEY    = os.environ.get("WC_CONSUMER_KEY", "")
WC_CONSUMER_SECRET = os.environ.get("WC_CONSUMER_SECRET", "")
APPROVE_SECRET     = os.environ.get("APPROVE_SECRET", "")


def _update_woocommerce(product_id, new_price, model):
    wc_url  = f"{WC_BASE_URL.rstrip('/')}/wp-json/wc/v3/products/{product_id}"
    payload = {"regular_price": str(new_price), "sale_price": str(new_price)}
    r = httpx.put(wc_url, json=payload, auth=(WC_CONSUMER_KEY, WC_CONSUMER_SECRET), timeout=20)
    r.raise_for_status()
    return r.json()


class handler(BaseHTTPRequestHandler):

    def do_GET(self):
        parsed     = urlparse(self.path)
        params     = parse_qs(parsed.query)
        product_id = params.get("product_id", [None])[0]
        new_price  = params.get("new_price",  [None])[0]
        token      = params.get("token",      [""])[0]
        model      = params.get("model",      ["Unknown"])[0]
        if APPROVE_SECRET and token != APPROVE_SECRET:
            self._send(403, {"error": "Invalid token"})
            return
        if not product_id or not new_price:
            self._send(400, {"error": "Missing product_id or new_price"})
            return
        try:
            updated = _update_woocommerce(product_id, new_price, model)
            self._send(200, {"status": "updated", "model": model, "product_id": product_id, "new_price": new_price, "wc_name": updated.get("name")})
        except Exception as exc:
            self._send(500, {"error": str(exc)})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body   = self.rfile.read(length)
        try:
            data = json.loads(body)
        except Exception:
            self._send(400, {"error": "Invalid JSON"})
            return
        product_id = data.get("woocommerce_product_id")
        new_price  = data.get("new_price")
        model      = data.get("model", "Unknown")
        if not product_id or new_price is None:
            self._send(400, {"error": "Missing product_id or new_price"})
            return
        try:
            updated = _update_woocommerce(product_id, new_price, model)
            self._send(200, {"status": "updated", "model": model, "product_id": product_id, "new_price": new_price, "wc_name": updated.get("name")})
        except Exception as exc:
            self._send(500, {"error": str(exc)})

    def _send(self, code, data):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass
