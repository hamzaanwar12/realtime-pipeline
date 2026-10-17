"""Clickstream event schema, validation and a fake generator."""
import json
import math
import random
import time
import uuid

TYPES = ("page_view", "add_to_cart", "checkout", "purchase")
FIELDS = {"event_id": str, "user_id": str, "type": str, "ts": float, "amount": float}


def make_event(now: float | None = None) -> dict:
    t = random.choices(TYPES, weights=(70, 18, 8, 4))[0]
    return {"event_id": str(uuid.uuid4()), "user_id": f"u{random.randint(1, 500)}", "type": t,
            "ts": now or time.time(), "amount": round(random.uniform(5, 300), 2) if t == "purchase" else 0.0}


def parse(raw: bytes) -> dict | None:
    """Return a clean event, or None for anything malformed (sent to the dead-letter table)."""
    try:
        e = json.loads(raw)
        clean = {k: typ(e[k]) for k, typ in FIELDS.items()}
    except (ValueError, KeyError, TypeError):
        return None
    ok = (clean["type"] in TYPES and math.isfinite(clean["amount"]) and 0 <= clean["amount"] < 1e9
          and math.isfinite(clean["ts"]) and 946_684_800 <= clean["ts"] < 4_102_444_800  # 2000..2100
          and 0 < len(clean["event_id"]) <= 64 and 0 < len(clean["user_id"]) <= 64)
    return clean if ok else None
