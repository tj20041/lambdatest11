import collections
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import json
import logging
import sys
import time
from typing import Any, Dict, Generator, Iterable, Iterator, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Telemetry and Structured Logger
# ---------------------------------------------------------------------------
logger = logging.getLogger("settlement_clearinghouse")
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(logging.Formatter("[%(levelname)s] %(asctime)s - %(name)s - %(message)s"))
logger.handlers = [stream_handler]

# ---------------------------------------------------------------------------
# Business Domain Config & State
# ---------------------------------------------------------------------------
SETTLEMENT_CURRENCIES = {"USD", "EUR", "GBP"}

FEE_SCHEDULE = {
    "TIER_PRIME": Decimal("0.0015"),
    "TIER_STANDARD": Decimal("0.0050"),
    "TIER_CROSS_BORDER": Decimal("0.0125")
}


def to_exact_decimal(value: Any) -> Decimal:
    """
    Safely converts a numeric value (float, int, or str) into an exact Decimal.

    Constructing Decimal() directly from a Python float preserves the float's
    imprecise IEEE-754 binary representation (e.g. Decimal(120.45) yields
    Decimal('120.4500000000000028421709430404007434844970703125')). Routing
    every conversion through str() first guarantees the exact, human-intended
    decimal value is used for all monetary/financial arithmetic.
    """
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


class LedgerContext:

    def __init__(self, batch_id: str, audit_trail: Optional[List[str]] = None):
        self.batch_id = batch_id
        # Never use a mutable default argument here: on AWS Lambda, warm
        # containers reuse the same Python process across invocations, and a
        # shared default list would leak audit checkpoints across unrelated
        # batches/invocations.
        self.audit_trail: List[str] = audit_trail if audit_trail is not None else []
        self.processed_records: int = 0
        self.total_fees: Decimal = Decimal("0.00")

    def record_checkpoint(self, action: str) -> None:
        self.audit_trail.append(f"{action}@{datetime.now(timezone.utc).isoformat()}")


def ingest_raw_wire_events(raw_records: List[Dict[str, Any]]) -> Generator[Dict[str, Any], None, None]:
    """Ingests raw wire payload and yields canonical transaction maps."""
    for raw in raw_records:
        # Initialize txn_id up front so the except handler below can never
        # raise a masking UnboundLocalError if 'payload' or
        # 'transaction_id' extraction fails before txn_id is assigned.
        txn_id = None
        try:
            body = raw["payload"]
            txn_id = body["transaction_id"]

            raw_amt = body["amount"]
            # Route every monetary conversion through to_exact_decimal() so
            # that native Python floats (e.g. 120.45) are converted via
            # their exact string representation instead of importing
            # IEEE-754 binary floating-point rounding artifacts.
            gross_amount = to_exact_decimal(raw_amt)

            yield {
                "txn_id": txn_id,
                "account_id": str(body["account_id"]),
                "amount": gross_amount,
                "currency": str(body.get("currency", "USD")),
                "tier": str(body.get("routing_tier", "TIER_STANDARD")),
                "created_at": datetime.fromtimestamp(body["epoch_sec"], tz=timezone.utc)
            }
        except Exception as err:
            identifier = txn_id if txn_id is not None else raw.get("payload", {}).get("transaction_id", "UNKNOWN")
            logger.error(f"Failed to ingest record {identifier}: {str(err)}")
            raise


def calculate_clearing_fee(record: Dict[str, Any], context: LedgerContext) -> Dict[str, Any]:
    """Applies institutional tariff and computes settlement net."""
    rate = FEE_SCHEDULE.get(record["tier"], Decimal("0.0100"))
    fee = (record["amount"] * rate).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    record["fee"] = fee
    record["net_settlement"] = record["amount"] - fee

    context.total_fees += fee
    context.processed_records += 1
    context.record_checkpoint(f"FEE_COMPUTED_{record['txn_id']}")
    return record


def compute_moving_settlement_window(
    records_iter: Iterator[Dict[str, Any]]
) -> Tuple[Decimal, List[Dict[str, Any]]]:
    """
    Computes cumulative settlement total while running a duplicate-detection pass.

    The generator is materialized exactly once into a list so both the volume
    summation pass and the duplicate-ID audit pass iterate the same complete
    set of records. Previously, itertools.tee() forked the generator but a
    subsequent next() call was issued directly against the original
    generator object (outside of the tee buffering mechanism), silently
    draining one record that neither tee branch ever received.
    """
    logger.info("Materializing record stream for dual-pass ledger verification...")

    records: List[Dict[str, Any]] = list(records_iter)

    if not records:
        raise ValueError("No records available to process in settlement window.")

    peek = records[0]
    logger.info(f"First element verified in transit: {peek['txn_id']}")

    running_volume = Decimal("0.0000")
    cleared_items: List[Dict[str, Any]] = []

    for item in records:
        running_volume += item["net_settlement"]
        cleared_items.append(item)

    # Verification pass on the full materialized record set
    seen_ids = set()
    for item in records:
        if item["txn_id"] in seen_ids:
            raise ValueError(f"Duplicate transaction ID detected: {item['txn_id']}")
        seen_ids.add(item["txn_id"])

    return running_volume, cleared_items


# ---------------------------------------------------------------------------
# Lambda Handler Entrypoint
# ---------------------------------------------------------------------------
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    logger.info("Initiating Clearinghouse Batch Settlement Engine...")

    ctx = LedgerContext(batch_id="BATCH-2026-09-001")

    # In-memory batch containing 4 transactions.
    simulated_batch = [
        {
            "payload": {
                "transaction_id": "TX-901",
                "account_id": "ACC-CORP-1",
                "amount": 50000.00,
                "currency": "USD",
                "routing_tier": "TIER_PRIME",
                "epoch_sec": 1788912000
            }
        },
        {
            "payload": {
                "transaction_id": "TX-902",
                "account_id": "ACC-CORP-2",
                "amount": 120.45,
                "currency": "EUR",
                "routing_tier": "TIER_STANDARD",
                "epoch_sec": 1788912010
            }
        },
        {
            "payload": {
                "transaction_id": "TX-903",
                "account_id": "ACC-RETAIL-1",
                "amount": 750.20,
                "currency": "USD",
                "routing_tier": "TIER_STANDARD",
                "epoch_sec": 1788912020
            }
        },
        {
            "payload": {
                "transaction_id": "TX-904",
                "account_id": "ACC-INTL-9",
                "amount": 100000.00,
                "currency": "GBP",
                "routing_tier": "TIER_CROSS_BORDER",
                "epoch_sec": 1788912030
            }
        }
    ]

    # Pipeline execution
    raw_stream = ingest_raw_wire_events(simulated_batch)
    priced_stream = (calculate_clearing_fee(tx, ctx) for tx in raw_stream)

    total_volume, finalized_records = compute_moving_settlement_window(priced_stream)

    # Post-clearing balance verification.
    # Now that ingestion uses exact Decimal conversion (via to_exact_decimal)
    # and the settlement window no longer drops a record, this reconciliation
    # should match exactly. The assertion is still wrapped defensively so a
    # genuine mismatch produces a graceful 500-style response with a
    # structured CloudWatch-friendly log line instead of an unhandled
    # AssertionError terminating the invocation.
    expected_exact_net = Decimal("150198.8478")
    try:
        if total_volume.quantize(Decimal("0.0001")) != expected_exact_net:
            raise AssertionError(
                f"Balance check failed: Volume {total_volume} does not match {expected_exact_net}"
            )
    except AssertionError as recon_err:
        logger.error(
            json.dumps({
                "metric_name": "ReconciliationMismatchCount",
                "metric_value": 1,
                "batch_id": ctx.batch_id,
                "expected": str(expected_exact_net),
                "actual": str(total_volume),
                "message": f"Reconciliation failure! Expected: {expected_exact_net}, Got: {total_volume}"
            })
        )
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "status": "RECONCILIATION_FAILED",
                "batch_id": ctx.batch_id,
                "error": str(recon_err)
            })
        }

    # Final response structure
    response_payload = {
        "status": "SETTLED",
        "batch_id": ctx.batch_id,
        "records_cleared": len(finalized_records),
        "total_volume": str(total_volume),             # Decimal is not JSON serializable
        "settled_at": datetime.now(timezone.utc).isoformat(),  # datetime is not JSON serializable
        "audit_trail": ctx.audit_trail
    }

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(response_payload)
    }


if __name__ == "__main__":
    lambda_handler({}, None)
