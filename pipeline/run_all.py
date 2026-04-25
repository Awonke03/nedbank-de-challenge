"""
Pipeline entry point — single-JVM, three-stage medallion pipeline.


"""
import logging
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("pipeline.run_all")

from pipeline.utils import load_config, get_spark
from pipeline.ingest import run_ingestion
from pipeline.transform import run_transformation
from pipeline.provision import run_provisioning


if __name__ == "__main__":
    t0 = time.time()
    logger.info("=" * 60)
    logger.info("Nedbank DE Pipeline — starting")
    logger.info("=" * 60)

    cfg   = load_config()
    spark = get_spark(cfg)

    stages = [
        ("Bronze – Ingest",    lambda: run_ingestion(spark=spark, cfg=cfg)),
        ("Silver – Transform", lambda: run_transformation(spark=spark, cfg=cfg)),
        ("Gold   – Provision", lambda: run_provisioning(spark=spark, cfg=cfg)),
    ]

    try:
        for label, fn in stages:
            logger.info("── STAGE: %s", label)
            t1 = time.time()
            try:
                fn()
            except SystemExit as e:
                logger.error("Stage '%s' failed (exit %s). Aborting.", label, e.code)
                sys.exit(e.code if e.code else 1)
            except Exception as e:
                logger.exception("Stage '%s' raised unexpected error: %s", label, e)
                sys.exit(1)
            logger.info("── DONE: %s  (%.1fs)", label, time.time() - t1)
    finally:
        try:
            spark.stop()
        except Exception:
            pass

    logger.info("=" * 60)
    logger.info("Pipeline complete — total elapsed: %.1fs", time.time() - t0)
    logger.info("=" * 60)
    sys.exit(0)
