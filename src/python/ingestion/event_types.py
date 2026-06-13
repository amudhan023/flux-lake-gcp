from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
import uuid


@dataclass
class PaymentEvent:
    transaction_id: str
    merchant_id: str
    customer_id: str
    amount: float
    currency: str
    gateway: str
    region: str
    timestamp: str
    event_type: str = "payment_created"

    @classmethod
    def generate(cls, merchant_range: int = 500, customer_range: int = 10000) -> "PaymentEvent":
        import random
        currencies = ["USD", "EUR", "GBP", "SGD"]
        gateways = ["stripe", "adyen", "braintree", "paypal"]
        regions = ["us-east", "us-west", "eu-west", "ap-south"]
        return cls(
            transaction_id=f"txn_{uuid.uuid4().hex[:16]}",
            merchant_id=f"merch_{random.randint(1, merchant_range)}",
            customer_id=f"cust_{random.randint(1, customer_range)}",
            amount=round(random.randint(100, 5_000_000) / 100, 2),
            currency=random.choice(currencies),
            gateway=random.choice(gateways),
            region=random.choice(regions),
            timestamp=datetime.utcnow().isoformat() + "Z",
        )


@dataclass
class RefundEvent:
    refund_id: str
    original_tx_id: str
    merchant_id: str
    refund_amount: float
    reason_code: str
    timestamp: str
    event_type: str = "refund_initiated"

    @classmethod
    def generate(cls, original_tx_id: str, merchant_id: str, original_amount: float) -> "RefundEvent":
        import random
        reason_codes = ["customer_request", "fraud", "duplicate", "product_not_received"]
        return cls(
            refund_id=f"ref_{uuid.uuid4().hex[:16]}",
            original_tx_id=original_tx_id,
            merchant_id=merchant_id,
            refund_amount=round(original_amount * random.uniform(0.1, 1.0), 2),
            reason_code=random.choice(reason_codes),
            timestamp=datetime.utcnow().isoformat() + "Z",
        )


@dataclass
class ChargebackEvent:
    dispute_id: str
    original_tx_id: str
    merchant_id: str
    dispute_amount: float
    status: str
    timestamp: str
    event_type: str = "chargeback_filed"

    @classmethod
    def generate(cls, original_tx_id: str, merchant_id: str, original_amount: float) -> "ChargebackEvent":
        import random
        statuses = ["filed", "under_review", "resolved_merchant", "resolved_customer"]
        return cls(
            dispute_id=f"chbk_{uuid.uuid4().hex[:16]}",
            original_tx_id=original_tx_id,
            merchant_id=merchant_id,
            dispute_amount=original_amount,
            status=random.choice(statuses),
            timestamp=datetime.utcnow().isoformat() + "Z",
        )


@dataclass
class SettlementEvent:
    batch_id: str
    merchant_id: str
    gross_amount: float
    fees: float
    net_amount: float
    timestamp: str
    event_type: str = "settlement_processed"

    @classmethod
    def generate(cls, merchant_id: str, gross_amount: float) -> "SettlementEvent":
        import random
        fee_rate = random.uniform(0.015, 0.03)
        fees = round(gross_amount * fee_rate, 2)
        return cls(
            batch_id=f"batch_{uuid.uuid4().hex[:12]}",
            merchant_id=merchant_id,
            gross_amount=gross_amount,
            fees=fees,
            net_amount=round(gross_amount - fees, 2),
            timestamp=datetime.utcnow().isoformat() + "Z",
        )
