"""daily_city_sales: the Spark ETL job our AI engineer is on call for.

orders (CSV, partitioned by dt) -> normalize (column aliases) -> data-quality gate
-> join stores -> aggregate GMV per city x category -> parquet.

Every stage emits structured events to OpenSearch, and every run writes one
summary document (status, schema, row counts, error class) to `nightowl-runs`.
"""
from __future__ import annotations

import hashlib
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

import yaml

from . import os_client, settings
from .eventlog import RunLogger


class DataQualityError(Exception):
    def __init__(self, message: str, details: dict):
        super().__init__(message)
        self.details = details


def load_config() -> dict:
    if not settings.CONFIG_FILE.exists():
        settings.CONFIG_FILE.write_text(settings.DEFAULT_CONFIG_FILE.read_text(encoding="utf-8"), encoding="utf-8")
    return yaml.safe_load(settings.CONFIG_FILE.read_text(encoding="utf-8"))


def config_version() -> str:
    return hashlib.sha1(settings.CONFIG_FILE.read_bytes()).hexdigest()[:8]


def _quiet_pyspark_loggers() -> None:
    """Spark 4 dumps the full JVM trace to stderr on analysis errors; we log a clean version ourselves."""
    try:
        from pyspark.logger import PySparkLogger
    except ImportError:  # older pyspark has no such logger
        return
    for name in ("DataFrameQueryContextLogger", "SQLQueryContextLogger"):
        lg = PySparkLogger.getLogger(name)  # must be created as PySparkLogger (accepts extra kwargs)
        lg.disabled = True
        lg.propagate = False


# ---------------------------------------------------------------- fast JVM start (AppCDS)
# The job is tiny (one 3MB CSV), so a run is dominated by JVM start-up: loading, verifying and
# interpreting thousands of Spark classes. A dynamic class-data-sharing archive (JDK 13+) lets the
# JVM memory-map those classes instead. Same Spark, same plan, same results; start-up is ~2x faster.
# The archive is keyed by PySpark version + JDK, so an upgrade just builds a new one. A stale or
# unusable archive is ignored by the JVM (-Xshare:auto), so the worst case is "slow", never "broken".
_CDS_DUMP_ENV = "NIGHTOWL_CDS_DUMP"


def cds_archive_path() -> Path | None:
    import pyspark

    java_home = os.environ.get("JAVA_HOME")
    java = os.path.join(java_home, "bin", "java") if java_home else shutil.which("java")  # same as spark-class
    if not java or not os.path.exists(java):
        return None
    real = os.path.realpath(java)
    key = hashlib.sha1(f"{pyspark.__version__}|{real}|{os.stat(real).st_mtime_ns}".encode()).hexdigest()[:10]
    return settings.ROOT / ".cache" / f"spark-cds-{key}.jsa"


def _java_opts() -> str:
    # SerialGC + C1-only JIT: less memory and CPU for a short single-machine job.
    opts = ["-XX:+UseSerialGC", "-XX:TieredStopAtLevel=1", "-Xlog:cds=off", "-Xlog:cds+dynamic=off"]
    dump_to = os.environ.get(_CDS_DUMP_ENV)
    if dump_to:
        opts.append(f"-XX:ArchiveClassesAtExit={dump_to}")
    else:
        jsa = cds_archive_path()
        if jsa and jsa.exists():
            opts.append(f"-XX:SharedArchiveFile={jsa}")
    return " ".join(opts)


def ensure_cds_archive(verbose: bool = True) -> Path | None:
    """Builds the class-data archive once by running the real pipeline (clean + 2 failure modes) in a
    throwaway NIGHTOWL_HOME with OpenSearch disabled, so nothing in data/, output/ or OpenSearch changes."""
    jsa = cds_archive_path()
    if jsa is None or jsa.exists():
        return jsa
    if verbose:
        print("  building Spark start-up cache (one time, ~1-3 min) ...")
    jsa.parent.mkdir(parents=True, exist_ok=True)
    tmp = jsa.with_name(f"{jsa.name}.tmp{os.getpid()}")
    with tempfile.TemporaryDirectory(prefix="nightowl-cds-") as home:
        (Path(home) / "config").mkdir()
        shutil.copy(settings.DEFAULT_CONFIG_FILE, Path(home) / "config" / settings.DEFAULT_CONFIG_FILE.name)
        env = {**os.environ, "NIGHTOWL_HOME": home, "NIGHTOWL_OS_DISABLED": "1", _CDS_DUMP_ENV: str(tmp)}
        try:
            subprocess.run([sys.executable, "-c", "from nightowl import pipeline; pipeline._cds_train()"],
                           env=env, cwd=home, capture_output=True, timeout=600, check=True)
        except Exception as exc:
            if verbose:
                print(f"  (Spark start-up cache not built, runs will just be slower: {str(exc)[:200]})")
    if tmp.exists() and tmp.stat().st_size > 0:
        for old in jsa.parent.glob("spark-cds-*.jsa"):
            old.unlink()
        os.replace(tmp, jsa)
        if verbose:
            print(f"  Spark start-up cache ready: {jsa.name}")
        return jsa
    tmp.unlink(missing_ok=True)
    return None


def _cds_train() -> None:
    """Runs inside ensure_cds_archive()'s subprocess only."""
    from pyspark import SparkContext

    from . import datagen

    assert os.environ.get(_CDS_DUMP_ENV) and settings.OS_DISABLED
    datagen.write_stores()
    dt = settings.today()
    for defect in (None, "schema_drift", "bad_data"):
        datagen.write_orders(dt, defect=defect)
        run(dt, trigger="cds-warmup")
    # The JVM writes the archive when it exits, which happens after Python would. Close the gateway
    # and wait for the JVM, so the parent never renames a half-written file into place.
    gw = SparkContext._gateway
    proc = getattr(gw, "proc", None)
    gw.shutdown()
    if proc is not None:
        proc.stdin.close()
        proc.wait(timeout=300)


def _spark():
    os.environ.setdefault("PYSPARK_PYTHON", "python3")
    _quiet_pyspark_loggers()
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder.appName("nightowl-daily-city-sales").master("local[2]")
             .config("spark.ui.enabled", "false")
             .config("spark.ui.showConsoleProgress", "false")
             .config("spark.sql.shuffle.partitions", "2")    # = local[2] cores; data is 3MB
             .config("spark.driver.memory", "512m")          # 40k rows; leaves RAM for OpenSearch on small laptops
             .config("spark.driver.extraJavaOptions", _java_opts())
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def _error_fields(exc: Exception, stage: str) -> dict:
    cond = None
    for attr in ("getCondition", "getErrorClass"):
        fn = getattr(exc, attr, None)
        if callable(fn):
            try:
                cond = fn()
            except Exception:
                cond = None
            if cond:
                break
    if isinstance(exc, DataQualityError):
        cond = "DATA_QUALITY_GATE_FAILED"
    msg = str(exc).strip()
    # keep the human-relevant head of Spark messages, drop JVM stack noise
    msg = msg.split("\nJVM stacktrace:")[0].split("\n== SQL")[0].split(";\n")[0][:1500]
    out = {"exception_type": type(exc).__name__, "error_class": cond or "UNCLASSIFIED",
           "error_message": msg, "failed_stage": stage}
    if isinstance(exc, DataQualityError):
        out["dq_details"] = exc.details
    return out


def run(dt: str | None = None, trigger: str = "scheduler") -> dict:
    from pyspark.sql import functions as F

    cfg = load_config()
    dt = dt or settings.today()
    pipeline = cfg["pipeline"]
    run_id = f"{pipeline}-{dt}-{uuid.uuid4().hex[:6]}"
    log = RunLogger(pipeline, run_id, dt)
    started = settings.now()
    t0 = time.time()
    stage = "init"
    summary: dict = {"run_id": run_id, "pipeline": pipeline, "dt": dt, "trigger": trigger,
                     "started_at": settings.iso(started), "host": socket.gethostname(),
                     "config_version": config_version(), "column_aliases": cfg.get("column_aliases") or {},
                     "quarantine_enabled": bool(cfg["data_quality"].get("quarantine_bad_rows"))}
    spark = None
    try:
        log.info(stage, "Starting Spark ETL run", trigger=trigger, config_version=summary["config_version"])
        spark = _spark()

        stage = "extract"
        src = settings.ROOT / cfg["source"]["orders_path"] / f"dt={dt}"
        log.info(stage, "Reading source partition", source_path=str(src))
        # cache(): count, DQ and transform reuse the parsed rows instead of re-reading the CSV each time
        df = spark.read.option("header", True).option("inferSchema", True).csv(str(src)).cache()
        summary["input_columns"] = df.columns
        summary["input_schema"] = [f"{f.name}:{f.dataType.simpleString()}" for f in df.schema.fields]
        rows_in = df.count()
        summary["rows_in"] = rows_in
        log.info(stage, "Source partition loaded", rows_in=rows_in, input_schema=summary["input_schema"])

        stage = "normalize"
        for upstream_name, canonical in (cfg.get("column_aliases") or {}).items():
            if upstream_name in df.columns:
                df = df.withColumnRenamed(upstream_name, canonical)
                log.info(stage, f"Applied column alias {upstream_name} -> {canonical}")
        missing = [c for c in cfg["required_columns"] if c not in df.columns]
        if missing:
            log.warn(stage, "Required columns missing from source contract", missing_columns=missing,
                     available_columns=df.columns)

        stage = "data_quality"
        dq = cfg["data_quality"]
        for column, max_rate in (dq.get("max_null_rate") or {}).items():
            if column not in df.columns:
                continue
            bad = df.filter(F.col(column).isNull() | (F.trim(F.col(column).cast("string")) == ""))
            nulls = bad.count()
            rate = round(nulls / max(rows_in, 1), 4)
            check = {"check": f"null_rate({column})", "observed": rate, "threshold": max_rate, "bad_rows": nulls}
            if rate <= max_rate:
                log.info(stage, f"DQ check passed: null_rate({column})={rate}", **check)
                continue
            breakdown = []
            if "app_version" in df.columns:
                breakdown = [r.asDict() for r in bad.groupBy("app_version").count()
                             .orderBy(F.desc("count")).limit(5).collect()]
            check["bad_rows_by_app_version"] = breakdown
            if dq.get("quarantine_bad_rows") and rate <= dq.get("max_quarantine_fraction", 0.5):
                qpath = settings.OUTPUT_DIR / "quarantine" / pipeline / f"dt={dt}"
                bad.write.mode("overwrite").parquet(str(qpath))
                df = df.subtract(bad)
                summary["rows_quarantined"] = nulls
                log.warn(stage, f"Quarantined {nulls} rows failing null_rate({column})", quarantine_path=str(qpath), **check)
                continue
            log.error(stage, f"DQ check FAILED: null_rate({column})={rate} > {max_rate}", **check)
            raise DataQualityError(
                f"Data quality gate failed: {rate:.1%} of rows have null {column} (threshold {max_rate:.0%})", check)

        stage = "transform"
        log.info(stage, "Joining stores and aggregating GMV by city x category")
        stores = spark.read.option("header", True).option("inferSchema", True).csv(
            str(settings.ROOT / cfg["source"]["stores_path"]))
        result = (df.join(stores, "store_id", "inner")
                  .groupBy("city", "category")
                  .agg(F.round(F.sum(F.col("order_amount")), 2).alias("gmv_inr"),
                       F.countDistinct("order_id").alias("orders"),
                       F.round(F.avg("delivery_minutes"), 1).alias("avg_delivery_minutes"))
                  .cache())  # so result.count() after the write doesn't recompute the join + aggregation

        stage = "load"
        out = settings.ROOT / cfg["output_path"] / f"dt={dt}"
        result.write.mode("overwrite").parquet(str(out))
        summary["rows_out"] = result.count()
        summary["status"] = "SUCCESS"
        log.info(stage, "Run completed successfully", rows_out=summary["rows_out"], output_path=str(out))
    except Exception as exc:
        summary["status"] = "FAILED"
        summary.update(_error_fields(exc, stage))
        log.error(stage, "Spark ETL run FAILED", **{k: v for k, v in summary.items()
                                                    if k in ("exception_type", "error_class", "error_message", "dq_details")})
    finally:
        summary["ended_at"] = settings.iso()
        summary["duration_s"] = round(time.time() - t0, 1)
        if os_client.available():
            try:
                os_client.index_doc(settings.IDX_RUNS, summary, doc_id=run_id, refresh=True)
            except Exception as exc:
                print(f"(could not write run summary to OpenSearch: {exc})")
        if spark is not None:
            spark.stop()
    return summary
