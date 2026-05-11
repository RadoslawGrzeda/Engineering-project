from kafka import KafkaConsumer
from kafka.errors import CommitFailedError
import json
import os
import logging

import psycopg2
import psycopg2.pool
from dotenv import load_dotenv
from apps.geocoding_crm_address.address_geocoder import AddressGeocoder
from pathlib import Path

load_dotenv(Path(__file__).parent / ".env")


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

UPDATE_SQL = """
    UPDATE client.address
    SET geo_coordinates_x_value = %s,
        geo_coordinates_y_value = %s,
        updated_at = NOW()
    WHERE person_id = %s
      AND address_street = %s
      AND geo_coordinates_x_value IS NULL
"""


class GeocodingConsumer:

    def __init__(self):
        self.geocoder = AddressGeocoder()
        self.KAFKA_BROKER_URL = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
        self.KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
        self.KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID')
        self.POSTGRES_URL = os.getenv('POSTGRES_CONNECTION')

        self.consumer = KafkaConsumer(
            self.KAFKA_TOPIC,
            bootstrap_servers=self.KAFKA_BROKER_URL,
            group_id=self.KAFKA_GROUP_ID,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            max_poll_records=10,
            max_poll_interval_ms=600000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

        self.pool = psycopg2.pool.SimpleConnectionPool(1, 3, self.POSTGRES_URL)

    def _get_conn(self):
        conn = self.pool.getconn()
        conn.autocommit = True
        return conn

    def _put_conn(self, conn):
        self.pool.putconn(conn)

    def _update_coordinates(self, conn, person_id: str, address: str, lat: float, lon: float):
        with conn.cursor() as cur:
            cur.execute(UPDATE_SQL, (lat, lon, person_id, address))
            return cur.rowcount

    def run(self):
        logger.info("Geocoding consumer started, listening on topic: %s", self.KAFKA_TOPIC)

        try:
            for message in self.consumer:
                data = message.value
                person_id = data.get('person_id')
                address = data.get('address_address')

                if not person_id or not address:
                    logger.warning("Missing person_id or address_address in message, skipping")
                    self.consumer.commit()
                    continue

                conn = self._get_conn()
                try:
                    lat, lon = self.geocoder.geocode_address(address)
                    if lat is None or lon is None:
                        logger.warning(
                            "No geocode result for address='%s' person_id=%s, marking as 0,0",
                            address, person_id
                        )
                        lat, lon = 0.0, 0.0

                    updated = self._update_coordinates(conn, person_id, address, lat, lon)
                    if updated > 0:
                        logger.info(
                            "Updated coordinates for person_id=%s address='%s' -> (%s, %s)",
                            person_id, address, lat, lon
                        )
                    else:
                        logger.info(
                            "No rows updated for person_id=%s address='%s' (already geocoded)",
                            person_id, address
                        )
                except psycopg2.OperationalError:
                    logger.exception("DB connection error for person_id=%s", person_id)
                    self.pool.putconn(conn, close=True)
                    conn = None
                except Exception:
                    logger.exception("Unexpected error processing person_id=%s", person_id)
                finally:
                    if conn:
                        self._put_conn(conn)

                try:
                    self.consumer.commit()
                except CommitFailedError:
                    logger.warning(
                        "Commit failed — consumer was removed from group, will rejoin on next poll"
                    )

        except Exception:
            logger.exception("Fatal error in geocoding consumer loop")
            raise
        finally:
            self.pool.closeall()
            self.consumer.close()
            logger.info("Geocoding consumer stopped")


if __name__ == "__main__":
    consumer = GeocodingConsumer()
    consumer.run()
