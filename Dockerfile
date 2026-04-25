FROM nedbank-de-challenge/base:1.0

# Install any additional Python dependencies beyond the base image.
# Base already has: pyspark==3.5.0, delta-spark==3.1.0, pandas==2.1.0,
#                   pyarrow==14.0.0, pyyaml==6.0.1, duckdb==0.10.0
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline code and configuration.
# Do NOT copy data files or output directories — injected at runtime.
COPY pipeline/ pipeline/
COPY config/   config/

# ── Fix 1: SPARK_HOME ────────────────────────────────────────────────────────
# The base image points SPARK_HOME at dist-packages/pyspark, but pip installs
# PySpark under site-packages.  Override so spark-submit is found correctly.
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark

# ── Fix 2: Driver bind address ───────────────────────────────────────────────
# With --network=none the container hostname cannot be resolved via DNS.
# Pinning Spark's local IP to loopback prevents UnknownHostException in the JVM.
ENV SPARK_LOCAL_IP=127.0.0.1

# ── Fix 3: Make /app the Python package root ─────────────────────────────────
ENV PYTHONPATH=/app

# ── Fix 4: Bake Delta Lake JARs into PySpark's default classpath ─────────────
# configure_spark_with_delta_pip downloads JARs via Ivy/Maven at runtime, which
# fails because (a) --network=none and (b) --read-only blocks Ivy cache writes.
# Instead, download both JARs during the build (network available) and place
# them directly in $SPARK_HOME/jars/.  PySpark adds everything in that directory
# to the JVM classpath automatically — no --packages, no Ivy, no network needed.
RUN SPARK_JARS_DIR=/usr/local/lib/python3.11/site-packages/pyspark/jars && \
    curl -fsSL -o "${SPARK_JARS_DIR}/delta-spark_2.12-3.1.0.jar" \
      "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar" && \
    curl -fsSL -o "${SPARK_JARS_DIR}/delta-storage-3.1.0.jar" \
      "https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar"

# Entry point — must run the complete pipeline end-to-end without interactive input.
# The scoring system uses this CMD directly.
CMD ["python", "pipeline/run_all.py"]
