from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, Optional

import redis

from handlers import handle_event

LOG = logging.getLogger("notification_service")


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


def wait_for_redis(r: redis.Redis, retries: int = 200, delay_s: float = 0.25) -> None:
    last_exc: Optional[Exception] = None
    for _ in range(retries):
        try:
            r.ping()
            return
        except Exception as e:
            last_exc = e
            time.sleep(delay_s)
    raise RuntimeError(f"Redis не ответил после {retries} попыток") from last_exc


def parse_json_message(raw: str) -> Dict[str, Any]:
    obj = json.loads(raw)
    if not isinstance(obj, dict):
        raise ValueError("Содержимое не распозналось как словарь")
    return obj


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    channel = os.getenv("REDIS_CHANNEL", "orders.events")

    r = build_redis_client()
    wait_for_redis(r)

    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe(channel)
    LOG.info("Подписался на канал=%s", channel)

    while True:
        try:
            msg = pubsub.get_message(timeout=0.3)
            if msg is None:
                continue

            raw = msg.get("data")
            if not isinstance(raw, str):
                LOG.warning("Неверный тип данных: %r", type(raw))
                continue

            try:
                event = parse_json_message(raw)
            except Exception as e:
                LOG.warning("Неверный JSON: %s; raw=%r", e, raw)
                continue

            handle_event(event)

        except KeyboardInterrupt:
            LOG.info("Остановлено пользователем")
            break
        except Exception as e:
            LOG.exception("Ошибка внутри цикла оповещалки: %s", e)
            time.sleep(0.5)

    try:
        pubsub.close()
    except Exception:
        pass


if __name__ == "__main__":
    main()
