"""
High-throughput financial transaction data generator.

Produces synthetic transaction records and publishes them to Kafka.
Designed to push ~5 GB of data per run using multiple producer threads.

Uses a fixed pool of 2048 accounts.  Each transaction picks a random
sender and receiver from the pool.  For every transaction, an account-
update event is also emitted to a separate Kafka topic so the accounts
Delta table can be upserted (MERGE) in near-real-time.
"""

import hashlib
import json
import logging
import random
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import orjson
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

# JSON sidecar for metrics (avoids DuckDB cross-process lock conflicts)
_METRICS_DIR = Path(__file__).resolve().parent.parent / "logs"
_GEN_JSON_PATH = _METRICS_DIR / "generator_metrics.json"

from .schemas import (
    ACCOUNT_POOL,
    ACCOUNT_STATUSES,
    CATEGORIES,
    CHANNELS,
    CURRENCIES,
    KYC_LEVELS,
    STATUSES,
    TRANSACTION_TYPES,
    AccountInfo,
)

logger = logging.getLogger(__name__)

# Thread-local Faker instances to avoid contention
_thread_local = threading.local()


def _get_faker() -> Faker:
    """Return a per-thread Faker instance (thread-safe)."""
    if not hasattr(_thread_local, "faker"):
        _thread_local.faker = Faker()
        Faker.seed(threading.get_ident())
    return _thread_local.faker


def generate_record() -> dict:
    """Generate a single financial transaction record (~500 bytes JSON).

    Picks sender and receiver from the 2048-account pool so account IDs
    are consistent and can be correlated with the accounts table.
    """
    fake = _get_faker()
    now = datetime.now(timezone.utc)
    ts = now - timedelta(seconds=random.randint(0, 86400))

    # Pick two distinct accounts from the pool
    sender_acct, receiver_acct = random.sample(ACCOUNT_POOL, 2)

    amount = round(random.uniform(0.01, 999999.99), 2)
    fee = round(amount * random.uniform(0.0001, 0.03), 2)

    txn_id = str(uuid.uuid4())
    session_bytes = uuid.uuid4().bytes
    device_fp = hashlib.sha256(session_bytes).hexdigest()

    return {
        "transaction_id": txn_id,
        "timestamp": ts.isoformat(),
        "sender_account_id": sender_acct.account_id,
        "receiver_account_id": receiver_acct.account_id,
        "sender_account": sender_acct.account_number,
        "receiver_account": receiver_acct.account_number,
        "amount": amount,
        "currency": random.choice(CURRENCIES),
        "transaction_type": random.choice(TRANSACTION_TYPES),
        "status": random.choice(STATUSES),
        "sender_name": sender_acct.holder_name,
        "receiver_name": receiver_acct.holder_name,
        "sender_bank_code": sender_acct.bank_code,
        "receiver_bank_code": receiver_acct.bank_code,
        "sender_country": sender_acct.country,
        "receiver_country": receiver_acct.country,
        "fee": fee,
        "exchange_rate": round(random.uniform(0.5, 1.5), 6),
        "reference_id": f"REF-{fake.numerify('##########')}",
        "memo": fake.text(max_nb_chars=120),
        "risk_score": round(random.uniform(0.0, 1.0), 4),
        "is_flagged": random.random() < 0.05,
        "category": random.choice(CATEGORIES),
        "channel": random.choice(CHANNELS),
        "ip_address": fake.ipv4(),
        "device_fingerprint": device_fp,
        "session_id": str(uuid.uuid4()),
    }


def generate_account_update(acct: AccountInfo, amount: float,
                            direction: str, ts: str) -> dict:
    """Generate an account-update event for the accounts Delta table.

    Called for both sender (direction="SENT") and receiver ("RECEIVED")
    on every transaction.  The Spark upsert job will MERGE these into
    the accounts table, updating balance and counters.
    """
    now = datetime.now(timezone.utc).isoformat()
    return {
        "account_id": acct.account_id,
        "account_number": acct.account_number,
        "holder_name": acct.holder_name,
        "bank_code": acct.bank_code,
        "country": acct.country,
        "currency": acct.currency,
        "account_type": acct.account_type,
        "account_status": random.choice(ACCOUNT_STATUSES),
        "balance": round(random.uniform(100, 5_000_000), 2),
        "total_sent": round(amount if direction == "SENT" else 0.0, 2),
        "total_received": round(amount if direction == "RECEIVED" else 0.0, 2),
        "transaction_count": 1,
        "risk_rating": round(random.uniform(0.0, 1.0), 4),
        "kyc_level": random.choice(KYC_LEVELS),
        "last_transaction_time": ts,
        "updated_at": now,
        "created_at": now,
    }


class TransactionGenerator:
    """Multi-threaded Kafka producer for financial transaction data.

    Publishes to two Kafka topics:
      1. transaction_topic — financial transaction events
      2. account_topic     — account update events (for Delta MERGE)

    For every transaction, two account-update events are emitted:
    one for the sender (direction=SENT) and one for the receiver
    (direction=RECEIVED).
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        account_topic: str,
        target_bytes: int,
        batch_size: int = 10_000,
        num_threads: int = 4,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.account_topic = account_topic
        self.target_bytes = target_bytes
        self.batch_size = batch_size
        self.num_threads = num_threads

        self._bytes_sent = 0
        self._records_sent = 0
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._batches_completed = 0
        self._error_count = 0
        self._batch_times_ms: list[float] = []
        self._publish_latencies_ms: list[float] = []

    def _create_producer(self) -> KafkaProducer:
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: orjson.dumps(v),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            linger_ms=50,
            batch_size=65536,
            buffer_memory=134217728,  # 128 MB
            compression_type="lz4",
            max_request_size=10485760,  # 10 MB
            retries=3,
        )

    def _produce_batch(self, producer: KafkaProducer, thread_id: int) -> int:
        """Produce one batch of records. Returns bytes sent.

        For each transaction, also emits two account-update events
        (sender + receiver) to the account_topic.
        """
        batch_start = time.perf_counter()
        batch_bytes = 0
        # Build a lookup from account_id → AccountInfo for fast access
        acct_lookup = {a.account_id: a for a in ACCOUNT_POOL}

        for _ in range(self.batch_size):
            if self._stop_event.is_set():
                break
            record = generate_record()
            serialized = orjson.dumps(record)
            batch_bytes += len(serialized)

            # Send transaction event
            producer.send(
                self.topic,
                key=record["transaction_id"],
                value=record,
            )

            # Send account-update events for sender and receiver
            sender_acct = acct_lookup[record["sender_account_id"]]
            receiver_acct = acct_lookup[record["receiver_account_id"]]

            sender_update = generate_account_update(
                sender_acct, record["amount"], "SENT", record["timestamp"],
            )
            receiver_update = generate_account_update(
                receiver_acct, record["amount"], "RECEIVED", record["timestamp"],
            )

            producer.send(
                self.account_topic,
                key=sender_update["account_id"],
                value=sender_update,
            )
            producer.send(
                self.account_topic,
                key=receiver_update["account_id"],
                value=receiver_update,
            )

        flush_start = time.perf_counter()
        producer.flush()
        publish_latency_ms = (time.perf_counter() - flush_start) * 1000
        batch_time_ms = (time.perf_counter() - batch_start) * 1000

        with self._lock:
            self._batch_times_ms.append(batch_time_ms)
            self._publish_latencies_ms.append(publish_latency_ms)

        return batch_bytes

    def _worker(self, thread_id: int) -> tuple:
        """Worker thread: produces batches until global target is reached."""
        producer = self._create_producer()
        thread_bytes = 0
        thread_records = 0

        try:
            while not self._stop_event.is_set():
                batch_bytes = self._produce_batch(producer, thread_id)
                thread_bytes += batch_bytes
                thread_records += self.batch_size

                with self._lock:
                    self._bytes_sent += batch_bytes
                    self._records_sent += self.batch_size
                    total = self._bytes_sent
                    total_records = self._records_sent
                    self._batches_completed += 1
                    total_batches = self._batches_completed

                elapsed = time.time() - self._run_start_time
                throughput = (total / 1e6) / elapsed if elapsed > 0 else 0

                # Write metrics to JSON sidecar (read by dashboard)
                self._write_metrics_json(
                    status="running",
                    elapsed_seconds=round(elapsed, 2),
                    throughput_mb_per_sec=round(throughput, 2),
                )

                if total >= self.target_bytes:
                    self._stop_event.set()

                progress_pct = min(100.0, (total / self.target_bytes) * 100)
                logger.info(
                    f"[Thread-{thread_id}] Batch done | "
                    f"Thread: {thread_bytes / 1e9:.2f} GB | "
                    f"Global: {total / 1e9:.2f} / {self.target_bytes / 1e9:.2f} GB "
                    f"({progress_pct:.1f}%)"
                )
        except KafkaError as e:
            logger.error(f"[Thread-{thread_id}] Kafka error: {e}")
            with self._lock:
                self._error_count += 1
            raise
        finally:
            producer.close()

        return thread_bytes, thread_records

    def run(self) -> dict:
        """Execute the data generation run. Returns summary stats."""
        logger.info(
            f"Starting generation: target={self.target_bytes / 1e9:.2f} GB, "
            f"threads={self.num_threads}, batch_size={self.batch_size}"
        )
        start_time = time.time()
        self._run_start_time = start_time
        self._batches_completed = 0
        self._error_count = 0
        self._batch_times_ms = []
        self._publish_latencies_ms = []

        # Write initial metrics
        self._write_metrics_json(status="running")

        with ThreadPoolExecutor(max_workers=self.num_threads) as pool:
            futures = {
                pool.submit(self._worker, tid): tid
                for tid in range(self.num_threads)
            }
            for future in as_completed(futures):
                tid = futures[future]
                try:
                    tb, tr = future.result()
                    logger.info(
                        f"[Thread-{tid}] Finished: {tb / 1e9:.2f} GB, {tr:,} records"
                    )
                except Exception as exc:
                    logger.error(f"[Thread-{tid}] Failed: {exc}")

        elapsed = time.time() - start_time
        throughput_mbps = (self._bytes_sent / 1e6) / elapsed if elapsed > 0 else 0

        summary = {
            "total_bytes": self._bytes_sent,
            "total_records": self._records_sent,
            "elapsed_seconds": round(elapsed, 2),
            "throughput_mb_per_sec": round(throughput_mbps, 2),
            "target_bytes": self.target_bytes,
        }
        logger.info(
            f"Generation complete: {self._bytes_sent / 1e9:.2f} GB, "
            f"{self._records_sent:,} records in {elapsed:.1f}s "
            f"({throughput_mbps:.1f} MB/s)"
        )

        # Final metrics
        self._write_metrics_json(
            status="completed",
            elapsed_seconds=round(elapsed, 2),
            throughput_mb_per_sec=round(throughput_mbps, 2),
        )

        return summary

    def _write_metrics_json(self, *, status: str = "running",
                            elapsed_seconds: float = 0.0,
                            throughput_mb_per_sec: float = 0.0):
        """Atomically write generator metrics to a JSON sidecar file."""
        bt = self._batch_times_ms[-200:]  # keep last 200
        pl = self._publish_latencies_ms[-200:]
        avg_bt = sum(bt) / len(bt) if bt else 0.0
        avg_pl = sum(pl) / len(pl) if pl else 0.0
        data = {
            "status": status,
            "start_time": self._run_start_time,
            "target_bytes": self.target_bytes,
            "bytes_sent": self._bytes_sent,
            "records_sent": self._records_sent,
            "batches_completed": self._batches_completed,
            "elapsed_seconds": elapsed_seconds,
            "throughput_mb_per_sec": throughput_mb_per_sec,
            "avg_batch_time_ms": round(avg_bt, 2),
            "avg_publish_latency_ms": round(avg_pl, 2),
            "batch_timings_ms": bt,
            "publish_latencies_ms": pl,
            "errors": self._error_count,
            "last_update": time.time(),
        }
        try:
            _METRICS_DIR.mkdir(parents=True, exist_ok=True)
            tmp = _GEN_JSON_PATH.with_suffix(".tmp")
            tmp.write_text(json.dumps(data), encoding="utf-8")
            tmp.replace(_GEN_JSON_PATH)
        except Exception:
            pass  # best-effort; don't crash the generator
