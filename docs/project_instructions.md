# Oracle AI Data Platform (AIDP) – Project Reference
> Curated resource list for Oracle AIDP. Last updated: March 2026.  
> Organized by **Use Case** and **Resource Type** for fast lookup.

---

## 🧠 Project System Prompt (paste into Claude Project Instructions)

```
You are an Oracle AI Data Platform (AIDP) expert with deep, hands-on skills across:

- Data Engineering: Apache Spark/PySpark, SQL, Scala, pipeline design, Medallion Architecture (Bronze/Silver/Gold), Delta Lake, Iceberg table formats
- ML & AI: model development and deployment, vector search, RAG pipelines, in-database ML, GenAI-augmented data workflows
- Platform & Infrastructure: OCI architecture, compute cluster configuration, object storage, IAM policy setup, instance management
- Governance: role-based access control (admin, auditor, data engineer), data lineage, audit logs, catalog and schema management
- Integrations: Oracle Analytics Cloud (OAC), Oracle Fusion Cloud ERP/SCM, Oracle AI Database 26ai, Agent Hub, Fusion Data Intelligence (FDI)
- Notebook Development: Python, SQL, Scala, Java notebooks with Git integration, workflow orchestration, task dependencies, scheduling

When answering questions:
- Default to expert-level explanations — skip basics unless asked
- Reference specific AIDP components, APIs, or config patterns where relevant
- Suggest best practices around governance, performance, and architecture
- Use the attached resource list to cite or recommend specific documentation, videos, or guides
```

---

## 🗂 By Use Case

### 🚀 Getting Started / Instance Setup
- [Oracle AIDP Workbench – Official Documentation Hub](https://docs.oracle.com/en/cloud/paas/ai-data-platform/) — Main portal for setup, access, and administration
- [How to Create an Instance in Oracle AIDP Workbench](https://www.youtube.com/watch?v=KGsSn690ZDc) — Compartments, IAM policies, environment naming, role-based access
- [Getting Started on Your Oracle AIDP Journey (Oracle Blog)](https://blogs.oracle.com/ai-data-platform/getting-started-on-your-oracle-ai-data-platform-journey) — Full data lifecycle overview: ingestion to AI-driven insights
- [Getting Started with AIDP Sample Notebooks (A-Team Oracle)](https://www.ateam-oracle.com/oracle-ai-data-platform-getting-started-with-the-sample-notebooks) — Configure environment, run GitHub sample notebooks, cluster and workspace setup

### 🏗 Medallion Architecture & Pipelines
- [How to Build a Basic Medallion Architecture in Oracle AIDP](https://www.youtube.com/watch?v=pZ6n5jTAkmI) — Bronze/Silver/Gold layers, GenAI data augmentation, Gold dataset curation
- [Using Oracle AI Data Platform Workbench (PDF – Jan 2026)](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/using-oracle-ai-data-platform-workbench.pdf) — Medallion Architecture, Spark workflows, ML deployment, governance, pricing
- [Oracle's AI Data Platform: Building a Foundation for Enterprise AI (Terillium)](https://terillium.com/oracles-ai-data-platform-building-a-foundation-for-enterprise-ai/) — Unified data lake+warehouse+analytics, Medallion Architecture, AI lifecycle tools

### 📓 Notebooks & Workspaces
- [How to Set Up Workspaces, Clusters, and Notebooks](https://www.youtube.com/watch?v=3qq_uLz9ucU) — Spark clusters (Python/SQL), library installs via requirements.txt, cluster-to-notebook linking
- [Getting Started with AIDP Sample Notebooks (A-Team Oracle)](https://www.ateam-oracle.com/oracle-ai-data-platform-getting-started-with-the-sample-notebooks) — Object storage examples, workspace creation, sample notebook walkthroughs
- [Oracle AIDP GitHub Samples Repository](https://github.com/oracle-samples/oracle-aidp-samples) — Official sample notebooks: Spark at scale, agent development, orchestration, catalog management
- [Using Oracle AI Data Platform Workbench (HTML Guide)](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/index.html) — Full procedural guide including notebook tasks

### ⚙️ Workflows & Automation
- [How to Automate Workflows in Oracle AIDP Workbench](https://www.youtube.com/watch?v=9gAgRnCjjqo) — Multi-step workflows, task dependencies, branching, job parameters, scheduling
- [Using Oracle AI Data Platform Workbench (PDF – Jan 2026)](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/using-oracle-ai-data-platform-workbench.pdf) — Workflow automation reference including low-code/no-code pipeline patterns

### 🗄 Catalogs, Schemas & Data Management
- [How to Organize Data with Catalogs and Schemas](https://www.youtube.com/watch?v=dSuQdIOaHX8) — Standard and external catalogs, schema config, table import, volume management
- [Using Oracle AI Data Platform Workbench (HTML Guide)](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/index.html) — Catalog and schema procedures
- [Oracle AIDP GitHub Samples Repository](https://github.com/oracle-samples/oracle-aidp-samples) — Catalog management sample notebooks

### 🔐 Security, Roles & Governance
- [How to Create and Assign Roles in Oracle AIDP Workbench](https://www.youtube.com/watch?v=Bfs4AYVuleE) — Admin/auditor/data engineer roles, OCID-based user assignment, permission inheritance
- [Using Oracle AI Data Platform Workbench (HTML Guide)](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/index.html) — Security and audit log reference
- [Oracle AIDP Licensing Information](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidli/index.html) — Licensing terms and third-party acknowledgments

### 🤖 AI, ML & Advanced Capabilities
- [Strategic Analysis: Oracle AIDP (Centroid Whitepaper)](https://www.centroid.com/library/strategic-analysis-oracle-ai-data-platform-aidp/) — Vector search, in-database ML, OCI Supercluster, RAG pipelines
- [My Top 5 Key Takeaways from Oracle AI World 2025 (Apps Associates)](https://appsassociates.com/blog/my-top-5-key-takeaways-from-oracle-ai-world-2025/) — Agent Hub, enterprise catalog, Oracle AI Database 26ai integration, pricing
- [Oracle AIDP GitHub Samples Repository](https://github.com/oracle-samples/oracle-aidp-samples) — Agent development and orchestration sample notebooks

### 📅 Roadmap & Community
- [Oracle AIDP Community Hub](https://community.oracle.com/products/oracleaidp/) — Live events, webinars, sharing center (notebooks, templates, scripts)
- [Oracle AIDP Product Roadmap (December 2025)](https://community.oracle.com/products/oracleanalytics/discussion/27733/oracle-ai-data-platform-product-roadmap-for-december-2025) — Official roadmap PDF and webinar links from Oracle Product Management

---

## 📁 By Resource Type

### 📄 Official Documentation
| Resource | URL |
|----------|-----|
| AIDP Workbench Documentation Hub | https://docs.oracle.com/en/cloud/paas/ai-data-platform/ |
| Using AIDP Workbench (HTML Guide) | https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/index.html |
| Using AIDP Workbench (PDF – Jan 2026) | https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/using-oracle-ai-data-platform-workbench.pdf |
| AIDP Licensing Information | https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidli/index.html |

### ▶ Videos
| Resource | URL |
|----------|-----|
| How to Build a Basic Medallion Architecture | https://www.youtube.com/watch?v=pZ6n5jTAkmI |
| How to Create an Instance | https://www.youtube.com/watch?v=KGsSn690ZDc |
| How to Create and Assign Roles | https://www.youtube.com/watch?v=Bfs4AYVuleE |
| How to Organize Data with Catalogs and Schemas | https://www.youtube.com/watch?v=dSuQdIOaHX8 |
| How to Set Up Workspaces, Clusters, and Notebooks | https://www.youtube.com/watch?v=3qq_uLz9ucU |
| How to Automate Workflows | https://www.youtube.com/watch?v=9gAgRnCjjqo |

### ✍ Blogs & Articles
| Resource | URL |
|----------|-----|
| Getting Started on Your AIDP Journey (Oracle Blog) | https://blogs.oracle.com/ai-data-platform/getting-started-on-your-oracle-ai-data-platform-journey |
| Getting Started with Sample Notebooks (A-Team Oracle) | https://www.ateam-oracle.com/oracle-ai-data-platform-getting-started-with-the-sample-notebooks |
| Building a Foundation for Enterprise AI (Terillium) | https://terillium.com/oracles-ai-data-platform-building-a-foundation-for-enterprise-ai/ |
| Top 5 Takeaways from Oracle AI World 2025 (Apps Associates) | https://appsassociates.com/blog/my-top-5-key-takeaways-from-oracle-ai-world-2025/ |
| Strategic Analysis: Oracle AIDP (Centroid Whitepaper) | https://www.centroid.com/library/strategic-analysis-oracle-ai-data-platform-aidp/ |

### ⬡ Community & GitHub
| Resource | URL |
|----------|-----|
| Oracle AIDP Community Hub | https://community.oracle.com/products/oracleaidp/ |
| Oracle AIDP GitHub Samples Repository | https://github.com/oracle-samples/oracle-aidp-samples |
| Oracle AIDP Product Roadmap (December 2025) | https://community.oracle.com/products/oracleanalytics/discussion/27733/oracle-ai-data-platform-product-roadmap-for-december-2025 |

---

## ⚡ Quick Reference

| Topic | Detail |
|-------|--------|
| Compute default | AMD 2 OCPU / 32GB Memory |
| Pricing unit | AIDP Units (2 OCPU + 32GB = 3 units/node) |
| Min cluster cost | 230 AIDP Units/hour |
| Notebook languages | Python, SQL, Scala, Java |
| Medallion layers | Bronze (raw) → Silver (transformed) → Gold (curated) |
| Table formats | Delta Uniform, Apache Iceberg |
| Catalog naming | `catalog_name.schema.table` (3-part) |
| Access to OCI nav | cloud.oracle.com → Analytics and AI → AI Data Platform |

---

## 🗒 Notebook Scaffold Templates

> Copy-paste starting points for each Medallion layer. Assumes a Spark cluster is attached and catalogs/schemas are pre-configured.

---

### 🥉 Bronze – Raw Ingestion

```python
# ============================================================
# BRONZE LAYER – Raw Ingestion
# Purpose: Land raw source data into object storage as-is,
#          partition by ingest date, register in catalog.
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from datetime import date

spark = SparkSession.builder.appName("bronze_ingestion").getOrCreate()

# --- CONFIG -------------------------------------------------
SOURCE_PATH    = "oci://your-bucket@namespace/raw/your_file.csv"
BRONZE_CATALOG = "your_catalog"
BRONZE_SCHEMA  = "bronze"
BRONZE_TABLE   = "your_table_name"
INGEST_DATE    = str(date.today())   # e.g. "2026-03-11"
# ------------------------------------------------------------

# Read raw source (adjust format/options as needed)
df_raw = (spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(SOURCE_PATH))

# Add audit columns
df_bronze = (df_raw
    .withColumn("_ingest_timestamp", current_timestamp())
    .withColumn("_ingest_date", lit(INGEST_DATE))
    .withColumn("_source_file", lit(SOURCE_PATH)))

# Write to Bronze catalog as Delta table (append)
(df_bronze.write
    .format("delta")
    .mode("append")
    .partitionBy("_ingest_date")
    .saveAsTable(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}"))

print(f"✅ Bronze ingestion complete: {df_bronze.count()} rows written.")
spark.sql(f"SELECT * FROM {BRONZE_CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE} LIMIT 10").show()
```

---

### 🥈 Silver – Transformation & Validation

```python
# ============================================================
# SILVER LAYER – Transformation & Validation
# Purpose: Cleanse, deduplicate, validate, and optionally
#          enrich data with GenAI augmentation.
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, when, current_timestamp,
    row_number
)
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("silver_transform").getOrCreate()

# --- CONFIG -------------------------------------------------
BRONZE_CATALOG  = "your_catalog"
BRONZE_SCHEMA   = "bronze"
BRONZE_TABLE    = "your_table_name"

SILVER_CATALOG  = "your_catalog"
SILVER_SCHEMA   = "silver"
SILVER_TABLE    = "your_table_name_clean"

DEDUP_KEY       = "id"          # Primary key column for deduplication
PARTITION_COL   = "_ingest_date"
# ------------------------------------------------------------

df_bronze = spark.table(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}")

# --- 1. Cleanse ---------------------------------------------
df_clean = (df_bronze
    .withColumn("name", trim(upper(col("name"))))   # adjust to your columns
    .filter(col(DEDUP_KEY).isNotNull()))

# --- 2. Deduplicate (keep latest by ingest timestamp) -------
window = Window.partitionBy(DEDUP_KEY).orderBy(col("_ingest_timestamp").desc())
df_dedup = (df_clean
    .withColumn("_row_num", row_number().over(window))
    .filter(col("_row_num") == 1)
    .drop("_row_num"))

# --- 3. Validate (flag bad rows) ----------------------------
df_validated = df_dedup.withColumn(
    "_quality_flag",
    when(col("name").isNull(), "MISSING_NAME")    # extend with your rules
    .otherwise("OK"))

# --- 4. (Optional) GenAI Augmentation -----------------------
# Example: call OCI GenAI or a registered model to add a
# sentiment / classification column. Uncomment to activate.
#
# from pyspark.sql.functions import udf
# from pyspark.sql.types import StringType
# import oci
#
# def call_genai(text):
#     # Insert OCI GenAI API call here
#     return "POSITIVE"
#
# genai_udf = udf(call_genai, StringType())
# df_validated = df_validated.withColumn("_sentiment", genai_udf(col("description")))

# --- 5. Write Silver ----------------------------------------
df_silver = df_validated.withColumn("_silver_timestamp", current_timestamp())

(df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy(PARTITION_COL)
    .saveAsTable(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.{SILVER_TABLE}"))

good = df_silver.filter(col("_quality_flag") == "OK").count()
bad  = df_silver.filter(col("_quality_flag") != "OK").count()
print(f"✅ Silver complete. Good rows: {good} | Flagged rows: {bad}")
```

---

### 🥇 Gold – Curation & Analytics-Ready Output

```python
# ============================================================
# GOLD LAYER – Curated, Analytics-Ready Dataset
# Purpose: Aggregate and shape Silver data into trusted,
#          business-facing tables consumable by OAC/FDI.
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg, count, max as _max,
    current_timestamp
)

spark = SparkSession.builder.appName("gold_curation").getOrCreate()

# --- CONFIG -------------------------------------------------
SILVER_CATALOG  = "your_catalog"
SILVER_SCHEMA   = "silver"
SILVER_TABLE    = "your_table_name_clean"

GOLD_CATALOG    = "your_catalog"
GOLD_SCHEMA     = "gold"
GOLD_TABLE      = "your_summary_table"

GROUP_BY_COL    = "category"   # Adjust to your dimension column
METRIC_COL      = "amount"     # Adjust to your measure column
# ------------------------------------------------------------

df_silver = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.{SILVER_TABLE}")

# --- 1. Filter to quality-passed rows only ------------------
df_clean = df_silver.filter(col("_quality_flag") == "OK")

# --- 2. Aggregate -------------------------------------------
df_gold = (df_clean
    .groupBy(GROUP_BY_COL)
    .agg(
        count("*").alias("record_count"),
        _sum(METRIC_COL).alias("total_amount"),
        avg(METRIC_COL).alias("avg_amount"),
        _max(METRIC_COL).alias("max_amount")
    )
    .withColumn("_gold_timestamp", current_timestamp()))

# --- 3. Write Gold (full overwrite — trusted snapshot) ------
(df_gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.{GOLD_TABLE}"))

print(f"✅ Gold curation complete: {df_gold.count()} summary rows.")
spark.sql(f"SELECT * FROM {GOLD_CATALOG}.{GOLD_SCHEMA}.{GOLD_TABLE}").show()

# --- 4. (Optional) Register for OAC consumption -------------
# Grant read access to OAC service principal or data share:
# spark.sql(f"GRANT SELECT ON TABLE {GOLD_CATALOG}.{GOLD_SCHEMA}.{GOLD_TABLE} TO `oac-service-user`")
```

---

### 🔁 Workflow Orchestration – Multi-Step Pipeline Scaffold

```python
# ============================================================
# WORKFLOW ORCHESTRATION SCAFFOLD
# Purpose: Template for a parameterized, multi-step AIDP
#          workflow notebook. Designed to be called by the
#          AIDP Workflow engine with injected job parameters.
# ============================================================

from pyspark.sql import SparkSession
import sys, traceback
from datetime import datetime

spark = SparkSession.builder.appName("aidp_pipeline").getOrCreate()

# --- JOB PARAMETERS (injected by AIDP Workflow engine) ------
# Access via dbutils or notebook widgets depending on config.
# Replace defaults with your Workflow parameter names.
try:
    RUN_DATE   = dbutils.widgets.get("run_date")    # e.g. "2026-03-11"
    ENV        = dbutils.widgets.get("env")         # e.g. "dev" | "prod"
    TABLE_NAME = dbutils.widgets.get("table_name")
except:
    # Fallback defaults for local/interactive testing
    RUN_DATE   = str(datetime.today().date())
    ENV        = "dev"
    TABLE_NAME = "your_table"

print(f"▶ Pipeline started | run_date={RUN_DATE} | env={ENV} | table={TABLE_NAME}")

# --- STEP RUNNER UTILITY ------------------------------------
def run_step(name, fn):
    print(f"\n{'='*50}\n⚙️  STEP: {name}\n{'='*50}")
    try:
        fn()
        print(f"✅ {name} — SUCCESS")
    except Exception as e:
        print(f"❌ {name} — FAILED\n{traceback.format_exc()}")
        sys.exit(1)   # Halt workflow on failure

# --- STEP DEFINITIONS ---------------------------------------
def step_ingest():
    # Replace with Bronze ingestion logic or %run call
    # %run ./bronze_ingestion
    print(f"  Ingesting raw data for {RUN_DATE}...")

def step_transform():
    # Replace with Silver transformation logic or %run call
    # %run ./silver_transform
    print(f"  Transforming data for table: {TABLE_NAME}...")

def step_curate():
    # Replace with Gold curation logic or %run call
    # %run ./gold_curation
    print(f"  Curating Gold layer for env: {ENV}...")

def step_validate():
    # Add post-pipeline data quality assertions
    # e.g. assert row counts, null checks, schema checks
    df = spark.table(f"your_catalog.gold.{TABLE_NAME}")
    assert df.count() > 0, f"Gold table {TABLE_NAME} is empty!"
    print(f"  Validation passed. Row count: {df.count()}")

def step_notify():
    # Optional: call OCI Notifications, webhook, or log to audit table
    print(f"  Pipeline complete. Notifying downstream consumers...")

# --- PIPELINE EXECUTION -------------------------------------
run_step("1. Bronze Ingestion",       step_ingest)
run_step("2. Silver Transformation",  step_transform)
run_step("3. Gold Curation",          step_curate)
run_step("4. Post-Pipeline Validation", step_validate)
run_step("5. Notification",           step_notify)

print(f"\n🏁 Pipeline finished successfully at {datetime.now().isoformat()}")
```

---

*18 resources · 7 use case groups · 4 resource type categories · 4 notebook scaffolds*
