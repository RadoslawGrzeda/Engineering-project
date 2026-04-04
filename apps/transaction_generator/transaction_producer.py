import time
import json
import logging
import os
import threading
from random import uniform

from kafka import KafkaProducer
from dotenv import load_dotenv

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


def run_shop(generator: TransactionGenerator, producer: KafkaProducer, topic: str, interval: tuple):
    shop_code = generator.shop_code
    shop_format = generator.shop_format
    logger.info(f"[{shop_code}/{shop_format}] Thread started — interval {interval[0]}-{interval[1]}s")

    while True:
        try:
            transaction = generator.generate_transaction()
            producer.send(topic, value=transaction)
            # print(json.dumps(transaction, indent=2, ensure_ascii=False))
            txn_id = transaction["transaction"]["transaction_id"][:8]
            logger.info(f"[{shop_code}/{shop_format}] Transaction {txn_id}... sent")
        except Exception as e:
            logger.error(f"[{shop_code}/{shop_format}] Error generating transaction: {e}")

        sleep_time = uniform(interval[0], interval[1])
        time.sleep(sleep_time)


def main():
    db = DbProvider()
    shops = db.get_shops()

    if not shops:
        logger.error("No active shops found in database. Exiting.")
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

        generator = TransactionGenerator(shop_code, shop_data, db.products, db.loyalty_cards)
        interval = HIPER_INTERVAL if shop_data["format"] == "HIPER" else SUPER_INTERVAL

        t = threading.Thread(
            target=run_shop,
            args=(generator, producer, topic, interval),
            name=f"shop-{shop_code}",
            daemon=True,
        )
        threads.append(t)
        t.start()

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