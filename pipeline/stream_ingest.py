"""
pipeline/stream_ingest.py  ─  Stage 3 Streaming Ingest (stub)
───────────────────────────────────────────────────────────────
This module is a forward stub for Stage 3.
It is NOT invoked by the Stage 1 or Stage 2 pipeline.

Stage 3 spec: stream_interface_spec.md
  - Reads micro-batch JSONL files from /data/stream every poll_interval_seconds
  - Writes to /data/output/stream_gold/current_balances  (Delta MERGE, upsert)
  - Writes to /data/output/stream_gold/recent_transactions (last 50 per account)
  - SLA: updated_at within 300 seconds of source event timestamp

Activate at Stage 3 by uncommenting the streaming: block in pipeline_config.yaml
and implementing the functions below.
"""
from __future__ import annotations

import logging
logger = logging.getLogger("stream.ingest")


def run_stream_ingestion() -> None:
    """
    Stage 3 entry point for streaming micro-batch processing.
    To be implemented at Stage 3.
    """
    logger.info("stream_ingest: Stage 3 not yet active — no-op")
