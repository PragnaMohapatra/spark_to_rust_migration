"""
Data schemas for the financial transactions pipeline.

Each record averages ~500 bytes when JSON-serialized, which means
~10 million records ≈ 5 GB.  The schema is deliberately wide enough
to stress both Spark deserialization and Delta column pruning.

The pipeline uses a fixed pool of 2048 accounts (each identified by
a stable UUID).  Transactions pick sender/receiver from this pool,
and account-update events are emitted to a separate Kafka topic to
drive Delta Lake MERGE (upsert) logic.
"""

import uuid
import random
from dataclasses import dataclass
from typing import List

from faker import Faker

# ── Constants ─────────────────────────────────────────────────

NUM_ACCOUNTS = 2048

TRANSACTION_TYPES = ["WIRE", "ACH", "CARD", "P2P", "CHECK"]
STATUSES = ["COMPLETED", "PENDING", "FAILED", "REVERSED"]
CATEGORIES = ["RETAIL", "CORPORATE", "GOVERNMENT", "INTERBANK"]
CHANNELS = ["ONLINE", "BRANCH", "ATM", "MOBILE", "API"]
CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "SGD", "HKD", "INR"]
ACCOUNT_TYPES = ["CHECKING", "SAVINGS", "BUSINESS", "INVESTMENT", "TRUST"]
ACCOUNT_STATUSES = ["ACTIVE", "SUSPENDED", "CLOSED", "DORMANT"]
KYC_LEVELS = ["NONE", "BASIC", "ENHANCED", "FULL"]

# ── Kafka message schema (JSON keys) ─────────────────────────

TRANSACTION_FIELDS = [
    "transaction_id",        # UUID string (36 bytes)
    "timestamp",             # ISO-8601 datetime
    "sender_account_id",     # UUID from the 2048-account pool
    "receiver_account_id",   # UUID from the 2048-account pool
    "sender_account",        # 16-digit account number (from pool)
    "receiver_account",      # 16-digit account number (from pool)
    "amount",                # Decimal (2 dp)
    "currency",              # ISO-4217 (3 chars)
    "transaction_type",      # enum: WIRE, ACH, CARD, P2P, CHECK
    "status",                # enum: COMPLETED, PENDING, FAILED, REVERSED
    "sender_name",           # Full name (from pool)
    "receiver_name",         # Full name (from pool)
    "sender_bank_code",      # SWIFT / routing (8-11 chars, from pool)
    "receiver_bank_code",    # SWIFT / routing (from pool)
    "sender_country",        # ISO-3166 alpha-2 (from pool)
    "receiver_country",      # ISO-3166 alpha-2 (from pool)
    "fee",                   # Decimal (2 dp)
    "exchange_rate",         # Decimal (6 dp)
    "reference_id",          # External reference string
    "memo",                  # Free-text (up to 120 chars)
    "risk_score",            # Float 0.0 – 1.0
    "is_flagged",            # Boolean
    "category",              # enum: RETAIL, CORPORATE, GOVERNMENT, INTERBANK
    "channel",               # enum: ONLINE, BRANCH, ATM, MOBILE, API
    "ip_address",            # IPv4
    "device_fingerprint",    # SHA-256 hex (64 chars)
    "session_id",            # UUID string
]

ACCOUNT_UPDATE_FIELDS = [
    "account_id",            # UUID — primary key for upsert
    "account_number",        # 16-digit number
    "holder_name",           # Account holder full name
    "bank_code",             # SWIFT / routing
    "country",               # ISO-3166 alpha-2
    "currency",              # Primary currency
    "account_type",          # CHECKING, SAVINGS, BUSINESS, etc.
    "account_status",        # ACTIVE, SUSPENDED, CLOSED, DORMANT
    "balance",               # Current balance (updated on each event)
    "total_sent",            # Lifetime amount sent
    "total_received",        # Lifetime amount received
    "transaction_count",     # Lifetime number of transactions
    "risk_rating",           # Float 0.0 – 1.0
    "kyc_level",             # NONE, BASIC, ENHANCED, FULL
    "last_transaction_time", # ISO-8601 timestamp of last activity
    "updated_at",            # ISO-8601 timestamp of this update
    "created_at",            # ISO-8601 timestamp of account creation
]


# ── Account Pool ──────────────────────────────────────────────

@dataclass
class AccountInfo:
    """Pre-generated account with stable identity."""
    account_id: str          # UUID
    account_number: str      # 16-digit
    holder_name: str
    bank_code: str
    country: str
    currency: str
    account_type: str


def generate_account_pool(num_accounts: int = NUM_ACCOUNTS,
                          seed: int = 42) -> List["AccountInfo"]:
    """Create a deterministic pool of accounts.

    Uses a fixed seed so every run produces the same 2048 accounts,
    ensuring sender/receiver IDs are consistent across runs.
    """
    fake = Faker()
    Faker.seed(seed)
    rng = random.Random(seed)

    pool = []
    for _ in range(num_accounts):
        pool.append(AccountInfo(
            account_id=str(uuid.UUID(int=rng.getrandbits(128))),
            account_number=fake.numerify("################"),
            holder_name=fake.name(),
            bank_code=fake.bothify("????####???").upper(),
            country=fake.country_code(),
            currency=rng.choice(CURRENCIES),
            account_type=rng.choice(ACCOUNT_TYPES),
        ))

    return pool


# Module-level pool — generated once, reused by all threads
ACCOUNT_POOL: List[AccountInfo] = generate_account_pool()


@dataclass
class TransactionRecord:
    """Typed representation of a single financial transaction."""
    transaction_id: str
    timestamp: str
    sender_account_id: str
    receiver_account_id: str
    sender_account: str
    receiver_account: str
    amount: float
    currency: str
    transaction_type: str
    status: str
    sender_name: str
    receiver_name: str
    sender_bank_code: str
    receiver_bank_code: str
    sender_country: str
    receiver_country: str
    fee: float
    exchange_rate: float
    reference_id: str
    memo: str
    risk_score: float
    is_flagged: bool
    category: str
    channel: str
    ip_address: str
    device_fingerprint: str
    session_id: str
