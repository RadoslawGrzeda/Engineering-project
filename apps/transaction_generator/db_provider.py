import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os
from collections import defaultdict

load_dotenv()

logger = logging.getLogger(__name__)


class DbProvider:

    def __init__(self):
        self.conn_params = {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": os.getenv("DB_PORT", "5433"),
            "dbname": os.getenv("DB_NAME", "ods"),
            "user": os.getenv("DB_USER", "ods"),
            "password": os.getenv("DB_PASSWORD", "ods"),
        }
        self.shops = {}
        self.products = []
        self.loyalty_cards = []
        self._load_all()

    def _get_connection(self):
        return psycopg2.connect(**self.conn_params)

    def _load_all(self):
        logger.info("Loading reference data from PostgreSQL...")
        conn = self._get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                self._load_shops(cur)
                self._load_pos(cur)
                self._load_printers(cur)
                self._load_cashiers(cur)
                self._load_products(cur)
                self._load_loyalty_cards(cur)
        finally:
            conn.close()

        active_shops = [code for code, data in self.shops.items() if data.get("pos_list")]
        logger.info(f"Loaded {len(active_shops)} shops with POS data: {active_shops}")
        logger.info(f"Loaded {len(self.products)} products, {len(self.loyalty_cards)} loyalty cards")

    def _load_shops(self, cur):
        cur.execute("""
            SELECT s.site_unique_code,
                   sf.site_format_unique_code AS format
            FROM store.site s
            JOIN store.site_info si ON s.site_unique_code = si.site_unique_code AND si.site_status_code = 'ACTIVE' AND si.is_current = TRUE
            JOIN store.site_format sf ON s.site_unique_code = sf.site_unique_code AND sf.is_current = TRUE
        """)
        for row in cur.fetchall():
            self.shops[row["site_unique_code"]] = {
                "format": row["format"],
                "pos_list": [],
                "printer_list": [],
                "cashier_list": [],
            }

    def _load_pos(self, cur):
        cur.execute("""
            SELECT pos_id, pos_serial_number, shop_id
            FROM transaction.pos
            WHERE is_current = TRUE
        """)
        for row in cur.fetchall():
            shop_id = row["shop_id"]
            if shop_id in self.shops:
                self.shops[shop_id]["pos_list"].append({
                    "pos_id": row["pos_id"],
                    "pos_serial_number": row["pos_serial_number"],
                })

    def _load_printers(self, cur):
        cur.execute("""
            SELECT printer_id, printer_serial_number, shop_id
            FROM transaction.printer
            WHERE is_current = TRUE
        """)
        for row in cur.fetchall():
            shop_id = row["shop_id"]
            if shop_id in self.shops:
                self.shops[shop_id]["printer_list"].append({
                    "printer_id": row["printer_id"],
                    "printer_serial_number": row["printer_serial_number"],
                })

    def _load_cashiers(self, cur):
        cur.execute("""
            SELECT cashier_id, location_code
            FROM transaction.cashier
        """)
        for row in cur.fetchall():
            loc = row["location_code"]
            if loc in self.shops:
                self.shops[loc]["cashier_list"].append({
                    "cashier_id": row["cashier_id"],
                })

    def _load_products(self, cur):
        cur.execute("""
            SELECT p.art_key, pi.ean, pi.price_net, pi.price_gross, pi.vat_rate
            FROM product.product p
            JOIN product.pos_information pi ON p.art_key = pi.art_key AND pi.is_current = TRUE
        """)
        self.products = [dict(row) for row in cur.fetchall()]

    def _load_loyalty_cards(self, cur):
        cur.execute("""
            SELECT identifier_id
            FROM client.loyalty_status
            WHERE end_date >= CURRENT_DATE OR end_date IS NULL
        """)
        self.loyalty_cards = [row["identifier_id"] for row in cur.fetchall()]

    def get_shops(self):
        return self.shops

    def refresh(self):
        self.shops = {}
        self.products = []
        self.loyalty_cards = []
        self._load_all()