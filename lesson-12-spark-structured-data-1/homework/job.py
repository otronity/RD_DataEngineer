"""PySpark job над GitHub Archive — ВАШ код (L12). Специфікація: SPEC.md.

Реалізуйте функції з `raise NotImplementedError`. Оркестрація (`build_spark`,
`read_raw`, `main`) вже готова — вона викликає ваші функції
і пише результати у data/output/.

Запуск:    uv run python job.py
Перевірка: uv run pytest

Запускайте з кореня homework/ (усі шляхи відносні до нього).
"""

from __future__ import annotations

import logging
import shutil

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: F401  (знадобиться у ваших функціях)
from pyspark.sql.types import BooleanType, StringType, StructField, StructType
from pyspark.sql.window import Window  # noqa: F401  (для top_repos_per_type)

import os
import sys

LANDING_GLOB = "data/landing/*.json.gz"
OUTPUT_DIR = "data/output"

TARGET_EVENT_TYPES = [
    "PushEvent",
    "PullRequestEvent",
    "IssuesEvent",
    "WatchEvent",
    "IssueCommentEvent",
]
SUMMARY_DIMENSIONS = ["event_type", "repo_owner", "actor_login", "hour"]
TOP_N = 5
BOT_SUFFIX = "[bot]"

log = logging.getLogger(__name__)


# ── Крок 1 — схема читання ────────────────────────────────────────────────────
def event_schema() -> StructType:
    """Явна схема landing-файлів (schema-on-read, без inferSchema).

    SPEC.md → «Крок 1».
    """
    return StructType([
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField(
            "actor",
            StructType([StructField("login", StringType(), True)]),
            True,
        ),
        StructField(
            "repo",
            StructType([StructField("name", StringType(), True)]),
            True,
        ),
        StructField("public", BooleanType(), True),
        StructField("created_at", StringType(), True),
    ])


def read_raw(spark: SparkSession) -> DataFrame:
    """ДАНО. Подає вашу схему у reader — жодного inferSchema."""
    return spark.read.schema(event_schema()).json(LANDING_GLOB)


# ── Крок 2 — сплющення ────────────────────────────────────────────────────────
def flatten(raw: DataFrame) -> DataFrame:
    """Розгорнути вкладені структури у пласкі колонки. SPEC.md → «Крок 2»."""
    return raw.select(
        F.col("id").alias("event_id"),
        F.col("type").alias("event_type"),
        F.col("actor.login").alias("actor_login"),
        F.col("repo.name").alias("repo_name"),
        "public",
        F.col("created_at").cast("timestamp").alias("created_at"),
    )


# ── Крок 3 — очищення ─────────────────────────────────────────────────────────
def clean(events: DataFrame) -> DataFrame:
    """Фільтри якості + дедуплікація. SPEC.md → «Крок 3»."""
    return (
        events.filter(
            F.col("event_type").isin(TARGET_EVENT_TYPES)
            & F.col("event_id").isNotNull()
            & F.col("repo_name").isNotNull()
            & F.col("public").isNotNull()
        )
        .dropDuplicates(["event_id"])
    )

# ── Крок 4 — похідні колонки ──────────────────────────────────────────────────
def with_derived(events: DataFrame) -> DataFrame:
    """Додати repo_owner, is_bot, hour. SPEC.md → «Крок 4»."""
    return (
        events
        .withColumn("repo_owner", F.split(F.col("repo_name"), "/")[0])
        .withColumn(
            "is_bot",
            F.coalesce(F.col("actor_login").endswith("[bot]"), F.lit(False))
        )
        .withColumn("hour", F.date_trunc("hour", F.col("created_at")))
    )

# ── Крок 5 — агрегати за власниками ───────────────────────────────────────────
def owner_totals(events: DataFrame) -> DataFrame:
    """Агрегат: один рядок на repo_owner. SPEC.md → «Крок 5»."""
    return (
        events.groupBy("repo_owner")
        .agg(
            F.count("*").alias("owner_events"),
            F.countDistinct("repo_name").alias("owner_repos"),
            F.sum(F.when(F.col("is_bot"), 1).otherwise(0)).alias("owner_bot_events"),
        )
    )


# ── Крок 6 — топ репозиторіїв за типом події ──────────────────────────────────
def top_repos_per_type(events: DataFrame, n: int) -> DataFrame:
    """Топ-N репозиторіїв за кількістю подій для кожного event_type. SPEC.md → «Крок 6»."""
    from pyspark.sql import Window

    # Спочатку рахуємо кількість подій для кожної пари (event_type, repo_name)
    repo_counts = (
        events.groupBy("event_type", "repo_name")
        .agg(F.count("*").alias("repo_event_count"))
    )

    # Визначаємо вікно для ранжування всередині кожного event_type
    window_spec = (
        Window.partitionBy("event_type")
        .orderBy(F.col("repo_event_count").desc(), F.col("repo_name").asc())
    )

    ranked = repo_counts.withColumn("rank", F.row_number().over(window_spec))

    return ranked.filter(F.col("rank") <= n)


# ── Крок 7 — збагачення топ репозиторіїв даними власників ─────────────────────
def enrich_top_repos(top_repos: DataFrame, owners: DataFrame) -> DataFrame:
    """LEFT JOIN топу з підсумками власників + частка. SPEC.md → «Крок 7»."""
    # Додаємо repo_owner до top_repos, якщо його там немає
    with_owner = top_repos.withColumn(
        "repo_owner", F.split(F.col("repo_name"), "/")[0]
    )

    # Виконуємо LEFT JOIN з таблицею owners
    joined = with_owner.join(owners, on="repo_owner", how="left")

    # Заповнюємо відсутні лічильники нулями, а також рахуємо частку (owner_share)
    enriched = (
        joined
        .withColumn("owner_events", F.coalesce(F.col("owner_events"), F.lit(0)))
        .withColumn("owner_repos", F.coalesce(F.col("owner_repos"), F.lit(0)))
        .withColumn("owner_bot_events", F.coalesce(F.col("owner_bot_events"), F.lit(0)))
        .withColumn(
            "owner_share",
            F.when(
                F.col("owner_events") > 0,
                F.col("repo_event_count") / F.col("owner_events")
            ).otherwise(None)
        )
    )

    return enriched


# ── Крок 8 — зріз підсумків за довільним виміром ──────────────────────────────
def summary_slice(events: DataFrame, dimension: str) -> DataFrame:
    """Агрегація подій та унікальних репозиторіїв за вказаним виміром. SPEC.md → «Крок 8»."""
    return (
        events.groupBy(dimension)
        .agg(
            F.count("*").alias("events"),
            F.countDistinct("repo_name").alias("distinct_repos"),
        )
        .withColumn("dimension", F.lit(dimension))
        .withColumn("dimension_value", F.col(dimension).cast(StringType()))
        .select("dimension", "dimension_value", "events", "distinct_repos")
    )

# ── Крок 9 — побудова зведеного звіту за списком вимірів ──────────────────────
def build_summary(events: DataFrame, dimensions: list[str]) -> DataFrame:
    """Об'єднання зрізів для всіх вказаних вимірів у єдиний DataFrame. SPEC.md → «Крок 9»."""
    from functools import reduce

    slices = [summary_slice(events, dim) for dim in dimensions]
    
    # Об'єднуємо всі зрізи в один датафрейм за допомогою reduce та unionByName
    return reduce(lambda df1, df2: df1.unionByName(df2), slices)


# ── БОНУС: Оптимізований білдер звітів за один прохід ─────────────────────────
def build_summary_optimized(events: DataFrame, dimensions: list[str]) -> DataFrame:
    """Альтернативний варіант build_summary через один прохід (explode / stacking)."""
    # Створюємо масив колонок, які представляють пари (назва_виміру, значення_виміру)
    # За допомогою array() ми формуємо список структур для кожного рядка події
    exprs = [
        F.struct(
            F.lit(dim).alias("dimension"),
            F.col(dim).cast(StringType()).alias("dimension_value")
        )
        for dim in dimensions
    ]
    
    stacked = events.withColumn("dim_struct", F.array(*exprs))
    exploded = stacked.select(
        F.explode("dim_struct").alias("d"),
        F.col("repo_name")
    )
    
    return (
        exploded
        .groupBy("d.dimension", "d.dimension_value")
        .agg(
            F.count("*").alias("events"),
            F.countDistinct("repo_name").alias("distinct_repos"),
        )
        .select("dimension", "dimension_value", "events", "distinct_repos")
    )


# ── Крок 10 — запис marts ─────────────────────────────────────────────────────
def write_outputs(outputs: dict[str, tuple[DataFrame, str | None]]) -> None:
    """Записати кожен mart у data/output/<name>/. SPEC.md → «Крок 10»."""
    for name, (df, partition_col) in outputs.items():
        out_path = os.path.join(OUTPUT_DIR, name)
        
        if partition_col:
            df = df.repartition(partition_col)
            writer = df.write.mode("overwrite").partitionBy(partition_col)
        else:
            df = df.coalesce(1)
            writer = df.write.mode("overwrite")
            
        writer.parquet(out_path)


# ── Оркестрація (ДАНО) ────────────────────────────────────────────────────────
def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        # UTC — інакше date_trunc("hour") дасть різні значення на різних машинах
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s  %(levelname)-7s %(message)s"
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)  # інакше py4j засмічує вивід
    spark = build_spark("l12-github")
    shutil.rmtree(OUTPUT_DIR, ignore_errors=True)

    # events читається кількома marts — тому cache(), а не чотири перечитування landing
    events = with_derived(clean(flatten(read_raw(spark)))).cache()

    owners = owner_totals(events)
    top_repos = enrich_top_repos(top_repos_per_type(events, TOP_N), owners)
    summary = build_summary(events, SUMMARY_DIMENSIONS)

    # Викликаємо оптимізовану версію:
    # summary = build_summary_optimized(events, SUMMARY_DIMENSIONS)

    marts: dict[str, tuple[DataFrame, str | None]] = {
        "events": (events, "event_type"),
        "owner_totals": (owners, None),
        "top_repos": (top_repos, None),
        "summary": (summary, None),
    }
    write_outputs(marts)

    for name, (df, _) in marts.items():
        log.info("%-13s %d", f"{name}:", df.count())

    events.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
