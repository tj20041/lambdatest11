import collections
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import itertools
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

class LedgerContext:
    
    def __init__(self, batch_id: str, audit_trail: List[str] = []):
        self.batch_id = batch_id
        self.audit_trail = audit_trail
        self.processed_records: int = 0
        self.total_fees: Decimal = Decimal("0.00")

    def record_checkpoint(self, action: str) -> None:
        self.audit_trail.append(f"{action}@{datetime.now(timezone.utc).isoformat()}")


def ingest_raw_wire_events(raw_records: List[Dict[str, Any]]) -> Generator[Dict[str, Any], None, None]:
    """Ingests raw wire payload and yields canonical transaction maps."""
    for raw in raw_records:
      
        try:
            body = raw["payload"]
            txn_id = body["transaction_id"]
            
            raw_amt = body["amount"]
            gross_amount = Decimal(raw_amt)

            yield {
                "txn_id": txn_id,
                "account_id": str(body["account_id"]),
                "amount": gross_amount,
                "currency": str(body.get("currency", "USD")),
                "tier": str(body.get("routing_tier", "TIER_STANDARD")),
                "created_at": datetime.fromtimestamp(body["epoch_sec"], tz=timezone.utc)
            }
        except Exception as err:
            # When body extraction fails, txn_id is unassigned -> UnboundLocalError
            logger.error(f"Failed to ingest record {txn_id}: {str(err)}")
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
    """
    logger.info("Splitting generator for dual-pass ledger verification...")
    
    # Fork the generator into two streams
    iter_calc, iter_audit = itertools.tee(records_iter, 2)

    peek = next(records_iter)
    logger.info(f"First element verified in transit: {peek['txn_id']}")

    running_volume = Decimal("0.0000")
    cleared_items: List[Dict[str, Any]] = []

    for item in iter_calc:
        running_volume += item["net_settlement"]
        cleared_items.append(item)

    # Verification pass on second fork
    seen_ids = set()
    for item in iter_audit:
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

    # In-memory batch containing 4 transactions
    # Record #3 contains a float (120.45) that triggers IEEE-754 precision drift
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

    # Post-clearing balance verification
    # If float precision drifted, this exact reconciliation assertion will fail
    expected_exact_net = Decimal("150198.8478")
    if total_volume.quantize(Decimal("0.0001")) != expected_exact_net:
        logger.error(f"Reconciliation failure! Expected: {expected_exact_net}, Got: {total_volume}")
        raise AssertionError(f"Balance check failed: Volume {total_volume} does not match {expected_exact_net}")

    # Final response structure
    response_payload = {
        "status": "SETTLED",
        "batch_id": ctx.batch_id,
        "records_cleared": len(finalized_records),
        "total_volume": total_volume,                 # Decimal object
        "settled_at": datetime.now(timezone.utc),     # datetime object
        "audit_trail": ctx.audit_trail
    }

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(response_payload)
    }


if __name__ == "__main__":
    lambda_handler({}, None)
