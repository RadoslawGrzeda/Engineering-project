import json
import uuid
import logging
from datetime import datetime
from random import randint, choice, choices
from decimal import Decimal, ROUND_HALF_UP

logger = logging.getLogger(__name__)

PAYMENT_METHODS = ["CARD", "CASH", "MOBILE", "VOUCHER"]
PAYMENT_WEIGHTS = [55, 30, 10, 5]

TRANSACTION_STATUSES = ["COMPLETED", "FAILED"]
TRANSACTION_STATUS_WEIGHTS = [97, 3]

METADATA_COMMENTS = [
    "Reklamacja klienta",
    "Zwrot czesciowy",
    "Platnosc odroczona",
    "Brak paragonu",
    "Korekta ceny",
    "Promocja sezonowa",
    "Karta podarunkowa",
    "Zamowienie specjalne",
    "Dostawa do domu",
    "Odbiór osobisty",
]


class TransactionGenerator:

    def __init__(self, shop_code: str, shop_data: dict, db):
        self.shop_code = shop_code
        self.shop_format = shop_data["format"]
        self.pos_list = shop_data["pos_list"]
        self.printer_list = shop_data["printer_list"]
        self.cashier_list = shop_data["cashier_list"]
        self.db = db

    @property
    def products(self):
        return self.db.products

    @property
    def loyalty_cards(self):
        return self.db.loyalty_cards

    @staticmethod
    def _uid():
        return uuid.uuid4().hex[:16]

    def generate_transaction(self) -> dict:
        correlation_id = self._uid()
        now = datetime.now()
        transaction_id = self._uid()

        pos_id = choice(self.pos_list)["pos_id"] if self.pos_list else None
        printer_id = choice(self.printer_list)["printer_id"] if self.printer_list else None
        cashier_id = choice(self.cashier_list)["cashier_id"] if self.cashier_list else None

        # 35-40% chance for loyalty card
        identifier_no = None
        if self.loyalty_cards and randint(1, 100) <= 37:
            identifier_no = choice(self.loyalty_cards)

        has_loyalty = identifier_no is not None
        lines = self._generate_lines(has_loyalty)
        payment = self._generate_payment(lines)
        status = self._generate_status()
        metadata = self._generate_metadata()

        return {
            "correlation_id": correlation_id,
            "transaction": {
                "transaction_id": transaction_id,
                "date": now.isoformat(),
                "location_code": self.shop_code,
                "identifier_no": identifier_no,
                "pos_id": pos_id,
                "printer_id": printer_id,
                "metadata_id": metadata["metadata_id"] if metadata else None,
                "currency_code": "PLN",
                "cashier_id": cashier_id,
                "creation_date": now.isoformat(),
                "correlation_id": correlation_id,
            },
            "lines": lines,
            "payment": payment,
            "status": status,
            "metadata": metadata,
        }

    def _generate_lines(self, has_loyalty: bool) -> list:
        if self.shop_format == "HIPER":
            roll = randint(1, 100)
            if roll <= 30:
                num_lines = randint(1, 5)
            elif roll <= 80:
                num_lines = randint(6, 12)
            else:
                num_lines = randint(13, 20)
        else:
            roll = randint(1, 100)
            if roll <= 30:
                num_lines = randint(1, 3)
            elif roll <= 80:
                num_lines = randint(4, 8)
            else:
                num_lines = randint(9, 12)

        lines = []
        selected_products = choices(self.products, k=num_lines)

        for i in range(num_lines):
            quantity = self._random_quantity()

            product = selected_products[i]
            prd_code = product["ean"]
            price_net = Decimal(str(product["price_net"]))
            vat_rate = Decimal(str(product["vat_rate"]))

            # per-line discount: 35% chance with loyalty card, 10% without
            discount_pct = Decimal("0")
            discount_chance = 35 if has_loyalty else 10
            if randint(1, 100) <= discount_chance:
                discount_pct = Decimal(str(randint(5, 25))) / Decimal("100")

            unit_price_net = price_net
            discounted_price = (unit_price_net * (Decimal("1") - discount_pct)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

            line_net_value = (discounted_price * Decimal(str(quantity))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            total_tax_amount = (line_net_value * vat_rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            total_line_value = line_net_value + total_tax_amount
            discount_value = ((unit_price_net - discounted_price) * Decimal(str(quantity))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

            lines.append({
                "transaction_line_id": self._uid(),
                "prd_code": prd_code,
                "quantity": quantity,
                "unit_price_net": float(unit_price_net),
                "tax_rate": float(vat_rate),
                "line_net_value": float(line_net_value),
                "total_line_value": float(total_line_value),
                "total_tax_amount": float(total_tax_amount),
                "discount_value": float(discount_value),
            })

        return lines

    def _random_quantity(self) -> int:
        roll = randint(1, 100)
        if roll <= 70:
            return 1
        elif roll <= 90:
            return randint(2, 3)
        else:
            return randint(4, 10)

    def _generate_payment(self, lines: list) -> dict:
        total_net_value = Decimal(str(sum(l["line_net_value"] for l in lines))).quantize(Decimal("0.01"))
        total_value = Decimal(str(sum(l["total_line_value"] for l in lines))).quantize(Decimal("0.01"))
        discount_value = Decimal(str(sum(l["discount_value"] for l in lines))).quantize(Decimal("0.01"))
        total_payment = total_value
        method = choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS, k=1)[0]

        return {
            "payment_id": self._uid(),
            "method": method,
            "total_value": float(total_value),
            "total_net_value": float(total_net_value),
            "total_payment": float(total_payment),
            "discount_value": float(discount_value),
        }

    def _generate_status(self) -> dict:
        is_cancelled = randint(1, 100) <= 3
        status = choices(TRANSACTION_STATUSES, weights=TRANSACTION_STATUS_WEIGHTS, k=1)[0]
        payment_status = status

        if is_cancelled:
            status = "CANCELLED"
            payment_status = "REFUNDED"

        transaction_status = "FINALIZED" if status == "COMPLETED" else status

        return {
            "transaction_status_id": self._uid(),
            "status": status,
            "cancelled": is_cancelled,
            "payment_status": payment_status,
            "transaction_status": transaction_status,
            "is_current": True,
        }

    def _generate_metadata(self) -> dict:
        metadata_id = self._uid()
        has_comment = randint(1, 100) <= 15

        return {
            "metadata_id": metadata_id,
            "comments": choice(METADATA_COMMENTS) if has_comment else None,
            "import_field": None,
            "shared": randint(1, 100) <= 10,
            "user_loan": randint(1, 100) <= 5,
        }
