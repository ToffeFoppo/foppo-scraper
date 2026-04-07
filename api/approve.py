"""
api/approve.py
Vercel Serverless Function — called when you click the Approve button in Telegram.
Receives the model + new price + WooCommerce product ID, then updates the product price.
"""

import os
import json
import logging

import httpx

log = logging.getLogger("approve")

WC_BASE_URL        = os.environ.get("WC_BASE_URL", "")
WC_CONSUMER_KEY    = os.environ.get("WC_CONSUMER_KEY", "")
WC_CONSUMER_SECRET = os.environ.get("WC_CONSUMER_SECRET", "")
APPROVE_SECRET     = os.environ.get("APPROVE_SECRET", "")


def handler(request, response):
    if request.method != "POST":
        # Also handle GET for direct URL clicking from Telegram button
        if request.method == "GET":
            params     = request.args
            product_id = params.get("product_id")
            new_price  = params.get("new_price")
            token      = params.get("token", "")
            model      = params.get("model", "Unknown")

            if APPROVE_SECRET and token != APPROVE_SECRET:
                response.status_code = 403
                return response.json({"error": "Invalid token"})

            if not product_id or new_price is None:
                response.status_code = 400
                return response.json({"error": "Missing product_id or new_price"})

            return _update_woocommerce(product_id, new_price, model, response)

        response.status_code = 405
        return response.json({"error": "Method not allowed"})

    # POST handler
    try:
        body_bytes = request.body if isinstance(request.body, bytes) else request.body.encode()
        data = json.loads(body_bytes)
    except Exception:
        response.status_code = 400
        return response.json({"error": "Invalid JSON"})

    product_id = data.get("woocommerce_product_id")
    new_price  = data.get("new_price")
    model      = data.get("model", "Unknown")

    if not product_id or new_price is None:
        response.status_code = 400
        return response.json({"error": "Missing product_id or new_price"})

    return _update_woocommerce(product_id, new_price, model, response)


def _update_woocommerce(product_id, new_price, model, response):
    wc_url  = f"{WC_BASE_URL.rstrip('/')}/wp-json/wc/v3/products/{product_id}"
    payload = {
        "regular_price": str(new_price),
        "sale_price":    str(new_price),
    }

    try:
        r = httpx.put(
            wc_url,
            json=payload,
            auth=(WC_CONSUMER_KEY, WC_CONSUMER_SECRET),
            timeout=20,
        )
        r.raise_for_status()
        updated = r.json()
        log.info(f"[approve] {model} product #{product_id} → {new_price} €")
        response.status_code = 200
        return response.json({
            "status":     "updated",
            "model":      model,
            "product_id": product_id,
            "new_price":  new_price,
            "wc_name":    updated.get("name"),
        })

    except httpx.HTTPStatusError as exc:
        log.error(f"[approve] WooCommerce error: {exc.response.text}")
        response.status_code = 502
        return response.json({
            "error":  "WooCommerce update failed",
            "detail": exc.response.text,
        })
    except Exception as exc:
        log.exception("[approve] unexpected error")
        response.status_code = 500
        return response.json({"error": str(exc)})
