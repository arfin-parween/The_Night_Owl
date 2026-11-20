"""Seed knowledge: past incidents (the team's 'institutional memory') and lineage.
Wording deliberately differs from today's failures so that semantic search has to
match on meaning, not on exact keywords."""

CLEAN_SCHEMA = [
    "order_id:string", "store_id:string", "sku:string", "category:string", "quantity:int",
    "order_amount:double", "payment_mode:string", "order_ts:timestamp", "delivery_minutes:int",
    "app_version:string",
]

INCIDENTS = [
    {
        "incident_id": "INC-0412", "days_ago": 23, "status": "resolved", "severity": "SEV2",
        "pipeline": "inventory_snapshot_etl", "error_class": "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        "title": "Inventory snapshot ETL broke after upstream renamed qty field",
        "error_signature": "cannot resolve column qty; did you mean quantity_units",
        "root_cause": "inventory-service v3.8 deploy renamed the exported field qty to quantity_units without "
                      "notifying downstream consumers. The Spark job still referenced the old name.",
        "resolution": "Added a column alias quantity_units -> qty in the pipeline config and re-ran the job. "
                      "No data loss.",
        "prevention": "Add a schema contract test in the producer CI; producers must announce renames in #data-contracts.",
        "fix_type": "add_column_alias", "fix_params": {"from": "quantity_units", "to": "qty"},
        "mttr_s": 5400, "approved_by": "rahul.k",
    },
    {
        "incident_id": "INC-0398", "days_ago": 41, "status": "resolved", "severity": "SEV2",
        "pipeline": "daily_city_sales", "error_class": "DATA_QUALITY_GATE_FAILED",
        "title": "POS app release sent orders without store reference",
        "error_signature": "data quality gate failed null store reference spike",
        "root_cause": "POS app 6.9 lost the store context for orders placed while offline and sent them with an "
                      "empty store identifier. 22% of the day's orders were affected.",
        "resolution": "Quarantined the bad rows so the rest of the day could be published, then backfilled once "
                      "the app team shipped hotfix 6.9.1.",
        "prevention": "Store app release checklist now includes an offline-mode order test.",
        "fix_type": "enable_quarantine", "fix_params": {},
        "mttr_s": 7200, "approved_by": "sneha.m",
    },
    {
        "incident_id": "INC-0377", "days_ago": 58, "status": "resolved", "severity": "SEV3",
        "pipeline": "daily_city_sales", "error_class": "PATH_NOT_FOUND",
        "title": "Orders export did not land: source folder absent",
        "error_signature": "path does not exist source partition",
        "root_cause": "The upstream exporter skipped the nightly snapshot because the read replica was lagging "
                      "after a heavy maintenance job on the primary database.",
        "resolution": "Waited for the exporter's automatic retry, confirmed the partition landed, then re-ran.",
        "prevention": "Schedule DB maintenance outside the export window; alert on replica lag > 10 min.",
        "fix_type": "rerun_only", "fix_params": {},
        "mttr_s": 4100, "approved_by": "rahul.k",
    },
    {
        "incident_id": "INC-0365", "days_ago": 70, "status": "resolved", "severity": "SEV2",
        "pipeline": "delivery_sla_etl", "error_class": "SPARK_EXECUTOR_OOM",
        "title": "Executor out of memory during join on hot store key",
        "error_signature": "ExecutorLostFailure java.lang.OutOfMemoryError GC overhead limit exceeded",
        "root_cause": "A festival sale concentrated 30% of events on three darkstores; the join produced a skewed partition.",
        "resolution": "Enabled AQE skew join handling and salted the store key.",
        "prevention": "Load test with skewed distributions before sale events.",
        "fix_type": "manual", "fix_params": {}, "mttr_s": 10800, "approved_by": "arjun.p",
    },
    {
        "incident_id": "INC-0351", "days_ago": 85, "status": "resolved", "severity": "SEV3",
        "pipeline": "rider_payouts_etl", "error_class": "S3_SLOW_DOWN",
        "title": "Output write throttled by object store",
        "error_signature": "503 SlowDown Please reduce your request rate",
        "root_cause": "Too many small output files written in parallel to a single prefix.",
        "resolution": "Coalesced output to fewer files and added retries with backoff.",
        "prevention": "Write-size guidelines added to the data platform handbook.",
        "fix_type": "manual", "fix_params": {}, "mttr_s": 3000, "approved_by": "sneha.m",
    },
    {
        "incident_id": "INC-0340", "days_ago": 96, "status": "resolved", "severity": "SEV2",
        "pipeline": "catalog_sync", "error_class": "KAFKA_CONSUMER_LAG",
        "title": "Catalog sync fell hours behind",
        "error_signature": "consumer group lag exceeded 2M messages",
        "root_cause": "A rebalance storm after a broker restart kept the consumer group from making progress.",
        "resolution": "Increased session timeout and restarted consumers one by one.",
        "prevention": "Static group membership for catalog consumers.",
        "fix_type": "manual", "fix_params": {}, "mttr_s": 6600, "approved_by": "arjun.p",
    },
]

LINEAGE = [
    {
        "pipeline": "daily_city_sales", "owner": "data-platform-team", "schedule_ist": "02:00 daily",
        "upstream": [
            {"name": "orders-service export", "owner": "checkout-platform-team"},
            {"name": "store master", "owner": "store-experience-team"},
        ],
        "downstream": [
            {"name": "city_ops_live_dashboard", "type": "dashboard", "owner": "city-ops", "sla_minutes_after_run": 45,
             "impact": "city managers plan rider shifts on stale GMV"},
            {"name": "finance_daily_gmv_report", "type": "report", "owner": "finance-analytics",
             "sla_minutes_after_run": 90, "impact": "CFO daily GMV mail goes out wrong or late"},
            {"name": "category_demand_forecast", "type": "ml_feature", "owner": "ml-platform",
             "sla_minutes_after_run": 240, "impact": "tomorrow's stock replenishment uses yesterday's demand"},
        ],
    }
]
