from kafka import KafkaConsumer
from kafka.errors import CommitFailedError
import json
import os

import psycopg2
import psycopg2.pool
from dotenv import load_dotenv
from service.geocoding_crm_address.address_geocoder import AddressGeocoder
from service.logger_config import get_logger
from pathlib import Path

load_dotenv(Path(__file__).parent / ".env")


logger = get_logger("service.geocoding_crm_address.consumer", service="geocoding_crm_address")

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
        logger.info("Geocoding consumer started", extra={
            "class": self.__class__.__name__,
            "method": "run",
            "topic": self.KAFKA_TOPIC,
        })

        try:
            for message in self.consumer:
                data = message.value
                person_id = data.get('person_id')
                address = data.get('address_street')

                if not person_id or not address:
                    logger.warning("Missing person_id or address in message, skipping", extra={
                        "class": self.__class__.__name__,
                        "method": "run",
                    })
                    self.consumer.commit()
                    continue

                conn = self._get_conn()
                try:
                    lat, lon = self.geocoder.geocode_address(address)
                    if lat is None or lon is None:
                        logger.warning("No geocode result for address, marking as 0,0", extra={
                            "class": self.__class__.__name__,
                            "method": "run",
                            "person_id": person_id,
                            "address": address,
                        })
                        lat, lon = 0.0, 0.0

                    updated = self._update_coordinates(conn, person_id, address, lat, lon)
                    if updated > 0:
                        logger.info("Updated coordinates", extra={
                            "class": self.__class__.__name__,
                            "method": "run",
                            "person_id": person_id,
                            "address": address,
                            "lat": lat,
                            "lon": lon,
                        })
                    else:
                        logger.info("No rows updated (already geocoded)", extra={
                            "class": self.__class__.__name__,
                            "method": "run",
                            "person_id": person_id,
                            "address": address,
                        })
                except psycopg2.OperationalError as e:
                    logger.error("DB connection error", extra={
                        "class": self.__class__.__name__,
                        "method": "run",
                        "person_id": person_id,
                        "error_type": type(e).__name__,
                        "error": str(e),
                    }, exc_info=True)
                    self.pool.putconn(conn, close=True)
                    conn = None
                except Exception as e:
                    logger.error("Unexpected error processing message", extra={
                        "class": self.__class__.__name__,
                        "method": "run",
                        "person_id": person_id,
                        "error_type": type(e).__name__,
                        "error": str(e),
                    }, exc_info=True)
                finally:
                    if conn:
                        self._put_conn(conn)

                try:
                    self.consumer.commit()
                except CommitFailedError as e:
                    logger.warning("Commit failed — consumer was removed from group, will rejoin on next poll", extra={
                        "class": self.__class__.__name__,
                        "method": "run",
                        "error_type": type(e).__name__,
                        "error": str(e),
                    })

        except Exception as e:
            logger.error("Fatal error in geocoding consumer loop", extra={
                "class": self.__class__.__name__,
                "method": "run",
                "error_type": type(e).__name__,
                "error": str(e),
            }, exc_info=True)
            raise
        finally:
            self.pool.closeall()
            self.consumer.close()
            logger.info("Geocoding consumer stopped", extra={
                "class": self.__class__.__name__,
                "method": "run",
            })


if __name__ == "__main__":
    consumer = GeocodingConsumer()
    consumer.run()
