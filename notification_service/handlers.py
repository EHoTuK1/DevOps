from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict

LOG = logging.getLogger("notification_service")



def handle_event(event: Dict[str, Any]) -> None:
    data = event.get("data") or {}
    LOG.info(
        "NOTIFY: user_id=%s order_id=%s total=%s %s",
        data.get("user_id"),
        data.get("order_id"),
        data.get("total"),
        data.get("currency"),
    )
    return
