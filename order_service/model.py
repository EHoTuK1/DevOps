from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict
import uuid
import json


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True, slots=True)
class Order:
    order_id: str
    user_id: str
    total: Decimal
    currency: str = "RUB"
    created_at: str = field(default_factory=utc_now_iso)

    @staticmethod
    def new(user_id: str, total: Decimal, currency: str = "RUB") -> Order:
        return Order(
            order_id=str(uuid.uuid4()),
            user_id=user_id,
            total=total,
            currency=currency,
        )

    def to_event(self) -> Dict[str, Any]:
        data = asdict(self)
        data["total"] = str(self.total)

        return {
            "event_type": "OrderCreated",
            "event_id": str(uuid.uuid4()),
            "timestamp": utc_now_iso(),
            "data": data,
        }
    
if __name__ == "__main__":
    order = Order.new(user_id="user123", total=Decimal("99.99"))
    event = order.to_event()
    print(json.dumps(event, indent=2))
