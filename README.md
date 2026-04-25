# Nedbank DE Challenge — Stage 1 Pipeline

## Architecture

```
/data/input/                    /data/output/
  accounts.csv     ──► Bronze ──► Silver ──►  Gold
  transactions.jsonl                           ├── fact_transactions (15 fields)
  customers.csv                                ├── dim_accounts      (11 fields)
                                               └── dim_customers     (9 fields)
```

## Repository Structure

```
Dockerfile                        # Extends nedbank-de-challenge/base:1.0
requirements.txt
pipeline/
  __init__.py
  run_all.py          # Orchestrator — called by scoring system
  ingest.py           # Bronze: raw ingest + _ingestion_timestamp
  transform.py        # Silver: type cast, flatten JSON, dedup, DQ flags
  provision.py        # Gold: dimensional model, BIGINT surrogate keys
  stream_ingest.py    # Stage 3 stub
  utils.py            # Spark factory, config loader, Delta helpers
config/
  pipeline_config.yaml
  dq_rules.yaml
```

## Invocation

```bash
# Build base image once
docker build -t nedbank-de-challenge/base:1.0 -f infrastructure/Dockerfile.base .

# Build candidate image
docker build -t candidate-submission:latest .

# Run
docker run -v /path/to/data:/data -m 4g --cpus="2" candidate-submission:latest
```

## Gold Layer Schema

### dim_customers (9 fields)
| # | Field | Type | Notes |
|---|-------|------|-------|
| 1 | customer_sk | BIGINT | Surrogate key (SHA-256 hash → BIGINT) |
| 2 | customer_id | STRING | Natural key |
| 3 | gender | STRING | M/F/NB/UNKNOWN |
| 4 | province | STRING | One of 9 SA provinces |
| 5 | income_band | STRING | |
| 6 | segment | STRING | |
| 7 | risk_score | INTEGER | 1–10 |
| 8 | kyc_status | STRING | |
| 9 | age_band | STRING | Derived from dob at run time |

### dim_accounts (11 fields — GAP-026 applied)
| # | Field | Type | Notes |
|---|-------|------|-------|
| 1 | account_sk | BIGINT | Surrogate key |
| 2 | account_id | STRING | Natural key |
| 3 | customer_id | STRING | Renamed from accounts.customer_ref ← required by V2 |
| 4–11 | … | | See output_schema_spec.md §3 |

### fact_transactions (15 fields)
| # | Field | Type | Notes |
|---|-------|------|-------|
| 1 | transaction_sk | BIGINT | Surrogate key |
| 2 | transaction_id | STRING | Natural key |
| 3 | account_sk | BIGINT | FK → dim_accounts |
| 4 | customer_sk | BIGINT | FK → dim_customers |
| 5 | transaction_date | DATE | |
| 6 | transaction_timestamp | TIMESTAMP | date + time combined |
| 7 | transaction_type | STRING | DEBIT/CREDIT/FEE/REVERSAL |
| 8 | merchant_category | STRING | |
| 9 | merchant_subcategory | STRING | NULL in Stage 1 |
| 10 | amount | DECIMAL(18,2) | |
| 11 | currency | STRING | Standardised to ZAR |
| 12 | channel | STRING | |
| 13 | province | STRING | From location.province |
| 14 | dq_flag | STRING | NULL in Stage 1 (clean) |
| 15 | ingestion_timestamp | TIMESTAMP | From Bronze audit |

## Design Decisions

**Surrogate keys as BIGINT:** Spec requires BIGINT. We derive them as `conv(substr(sha256(pk), 1, 15), 16, 10)` — deterministic (same input → same key), no `.collect()`, fits in BIGINT range.

**No `.collect()` on large DataFrames:** All deduplication uses Window functions. All joins use broadcast for small dimension tables.

**Nested JSON flattening in Silver:** `location.*` and `metadata.*` are flattened to top-level columns. `merchant_subcategory` absence in Stage 1 is handled by adding it as null — pipeline never fails on missing keys.

**dq_flag:** NULL in Stage 1 (clean data). Stage 2 populates with one of 6 recognised codes; logic driven by `config/dq_rules.yaml`, not hardcoded.

**age_band not dob:** `dim_customers` exposes `age_band` (derived bucket) not raw `dob`, per spec §4.

## AI Usage Disclosure
AI assistance was used for scaffolding. All architectural decisions, schema mappings, and design trade-offs are the author's own work and can be fully defended.
