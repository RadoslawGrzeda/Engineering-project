import time
import json
import logging
import os
import threading

from kafka import KafkaProducer
from dotenv import load_dotenv

import scheduler
from db_provider import DbProvider
from transaction_generator import TransactionGenerator

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

HIPER_INTERVAL = (
    float(os.getenv("HIPER_MIN_INTERVAL", "1")),
    float(os.getenv("HIPER_MAX_INTERVAL", "3")),
)
SUPER_INTERVAL = (
    float(os.getenv("SUPER_MIN_INTERVAL", "4")),
    float(os.getenv("SUPER_MAX_INTERVAL", "8")),
)

PRODUCTS_REFRESH_INTERVAL = float(os.getenv("PRODUCTS_REFRESH_INTERVAL", "300"))


def run_shop(generator: TransactionGenerator, producer: KafkaProducer, topic: str, interval: tuple):
    shop_code = generator.shop_code
    shop_format = generator.shop_format
    # Środnia z konfigurowalnego zakresu HIPER/SUPER staje się średnią
    # procesu Poissona — zachowuje sens dotychczasowej konfiguracji .env.
    mean_interval = sum(interval) / 2
    logger.info(
        f"[{shop_code}/{shop_format}] Thread started — Poisson mean interval {mean_interval:.2f}s"
    )

    while True:
        if not scheduler.is_active():
            logger.info(
                f"[{shop_code}/{shop_format}] Inactive period "
                f"(Sundays off / outside {scheduler.ACTIVE_HOUR_START}:00–{scheduler.ACTIVE_HOUR_END}:00) "
                f"— sleeping until the window opens"
            )
            scheduler.wait_until_active()
            continue

        # Burst z tego samego sklepu (wiele kas) — wysyłane bez przerwy,
        # więc trafiają do Kafki praktycznie w tym samym momencie.
        batch_size = scheduler.next_batch_size()
        for _ in range(batch_size):
            try:
                transaction = generator.generate_transaction()
                producer.send(topic, value=transaction)
                txn_id = transaction["transaction"]["transaction_id"][:8]
                logger.info(f"[{shop_code}/{shop_format}] Transaction {txn_id}... sent")
            except Exception as e:
                logger.error(f"[{shop_code}/{shop_format}] Error generating transaction: {e}")

        time.sleep(scheduler.next_interval(mean_interval))


def refresh_products_loop(db: DbProvider, interval: float):
    logger.info(f"Product refresh thread started — every {interval}s")
    while True:
        time.sleep(interval)
        try:
            db.refresh_products()
        except Exception as e:
            logger.error(f"Failed to refresh products: {e}")


def main():
    db = DbProvider()
    shops = db.get_shops()

    if not shops:
        logger.error("No active shops found in database. Exiting.")
        return

    if not db.products:
        logger.error("No products found in database. Exiting.")
        return

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "transactions")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    logger.info(f"Starting transaction generators for {len(shops)} shops on topic '{topic}'")

    threads = []
    for shop_code, shop_data in shops.items():
        if not shop_data["pos_list"]:
            logger.warning(f"[{shop_code}] No POS devices — skipping")
            continue

        generator = TransactionGenerator(shop_code, shop_data, db)
        interval = HIPER_INTERVAL if shop_data["format"] == "HIPER" else SUPER_INTERVAL

        t = threading.Thread(
            target=run_shop,
            args=(generator, producer, topic, interval),
            name=f"shop-{shop_code}",
            daemon=True,
        )
        threads.append(t)
        t.start()

    refresh_thread = threading.Thread(
        target=refresh_products_loop,
        args=(db, PRODUCTS_REFRESH_INTERVAL),
        name="product-refresh",
        daemon=True,
    )
    refresh_thread.start()

    logger.info(f"All {len(threads)} shop threads started. Press Ctrl+C to stop.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        producer.flush()
        producer.close()
        logger.info("Producer closed.")


if __name__ == "__main__":
    main()