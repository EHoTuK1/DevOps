from __future__ import annotations

import json
import logging
import os
import time
import random 
from decimal import Decimal

import redis

from model import Order


LOG = logging.getLogger("order_service")


def build_redis_client() -> redis.Redis:
    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", "6379"))

    return redis.Redis(
        host=host,
        port=port,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=5,
        health_check_interval=10,
    )


def wait_for_redis(r: redis.Redis, retries: int = 50, delay_s: float = 0.2) -> None:
    last_exc: Exception | None = None
    for _ in range(retries):
        try:
            r.ping()
            return
        except Exception as e:
            last_exc = e
            time.sleep(delay_s)
    raise RuntimeError(f"Redis не ответил после {retries} попыток") from last_exc


def publish_order_created(r: redis.Redis, channel: str, order: Order) -> None:
    payload = order.to_event()
    msg = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    receivers = r.publish(channel, msg)
    LOG.info("Опубликовано событие OrderCreated: order_id=%s channel=%s receivers=%d", order.order_id, channel, receivers)


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    channel = os.getenv("REDIS_CHANNEL", "orders.events")

    r = build_redis_client()
    wait_for_redis(r)

    user_id = os.getenv("ORDER_USER_ID", str(random.randint(1, 1000)))
    total = Decimal(os.getenv("ORDER_TOTAL", str(round(random.uniform(5, 100), 2))))
    currency = os.getenv("ORDER_CURRENCY", random.choice(["USD", "EUR", "RUB"]))

    order = Order.new(user_id=user_id, total=total, currency=currency)
    LOG.info("Сделан заказ: %s", order)

    publish_order_created(r, channel, order)


if __name__ == "__main__":
    main()
