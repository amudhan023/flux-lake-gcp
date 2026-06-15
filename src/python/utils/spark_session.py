import os
from pyspark.sql import SparkSession

# STORAGE_BACKEND controls which connector Spark uses:
#   gcs   — GCS connector (default). Uses fake-gcs-server locally when
#            GCS_EMULATOR_HOST is set; uses real GCS on production GCE/GKE.
#   local — No connector (plain file paths). Used in tests only.
STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "gcs")


def get_spark_session(app_name: str = "pipeline", local: bool = False) -> SparkSession:
    master = "local[*]" if local else os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", os.getenv("ENABLE_AQE", "true"))
        .config("spark.sql.autoBroadcastJoinThreshold", "10m" if os.getenv("ENABLE_BROADCAST_JOINS", "true") == "true" else "-1")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", "200")
    )

    if STORAGE_BACKEND == "local":
        # Plain local filesystem — no storage connector needed. Used in tests.
        packages = ["io.delta:delta-spark_2.12:3.2.0"]
    else:
        # Google Cloud Storage via the Hadoop GCS connector.
        # When GCS_EMULATOR_HOST is set, traffic is redirected to fake-gcs-server
        # (local dev). When unset, Application Default Credentials handle auth
        # against real GCS (production GCE/GKE).
        builder = (
            builder
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        )
        emulator_host = os.getenv("GCS_EMULATOR_HOST")
        if emulator_host:
            builder = (
                builder
                .config("spark.hadoop.fs.gs.storage.root.url", f"http://{emulator_host}")
                .config("spark.hadoop.google.cloud.auth.service.account.enable", "false")
            )
        else:
            builder = builder.config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        packages = [
            "io.delta:delta-spark_2.12:3.2.0",
            "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22",
        ]

    builder = builder.config("spark.jars.packages", ",".join(packages))
    return builder.getOrCreate()
