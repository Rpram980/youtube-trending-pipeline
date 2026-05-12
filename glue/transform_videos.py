import json
import re
import sys
from typing import Dict, Optional

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


ISO8601_DURATION_RE = re.compile(
    r"^P(?:(?P<days>\d+)D)?"
    r"(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?$"
)


def iso8601_duration_to_seconds(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    m = ISO8601_DURATION_RE.match(value)
    if not m:
        return None
    days = int(m.group("days") or 0)
    hours = int(m.group("hours") or 0)
    minutes = int(m.group("minutes") or 0)
    seconds = int(m.group("seconds") or 0)
    return days * 86400 + hours * 3600 + minutes * 60 + seconds


duration_to_seconds_udf = F.udf(iso8601_duration_to_seconds, T.IntegerType())


def load_category_mapping(glue_ctx: GlueContext, s3_path: Optional[str]) -> Dict[int, str]:
    """
    Expected JSON formats:
    1) { "categories": { "1": "Film & Animation", ... }, "region": "US" }
    2) { "1": "Film & Animation", ... }
    """
    if not s3_path:
        return {}

    spark = glue_ctx.spark_session
    rows = spark.read.text(s3_path).collect()
    if not rows:
        return {}
    text = "\n".join([r["value"] for r in rows if r and "value" in r]).strip()
    if not text:
        return {}
    doc = json.loads(text)

    if isinstance(doc, dict) and "categories" in doc and isinstance(doc["categories"], dict):
        doc = doc["categories"]

    mapping: Dict[int, str] = {}
    if isinstance(doc, dict):
        for k, v in doc.items():
            try:
                mapping[int(k)] = str(v)
            except Exception:
                continue
    return mapping


def with_category_name(df: DataFrame, mapping: Dict[int, str]) -> DataFrame:
    if not mapping:
        return df.withColumn("category_name", F.lit("Unknown"))

    # Build a Spark map literal: map(1, "Film & Animation", 2, "Autos & Vehicles", ...)
    items = []
    for k, v in sorted(mapping.items(), key=lambda x: x[0]):
        items.extend([F.lit(int(k)), F.lit(str(v))])

    m = F.create_map(*items)
    return df.withColumn("category_name", F.coalesce(m[F.col("category_id")], F.lit("Unknown")))


def flatten_raw_videos(raw_df: DataFrame, trending_date: str, region: str) -> DataFrame:
    """
    Input is expected to be the JSON saved by the extractor. It contains:
    - items: array of video objects
    plus optional lineage fields added by extractor: _trending_date, _region
    """
    items_df = raw_df.select(F.explode_outer(F.col("items")).alias("item"))

    df = items_df.select(
        F.col("item.id").cast("string").alias("video_id"),
        F.col("item.snippet.title").cast("string").alias("title"),
        F.col("item.snippet.channelTitle").cast("string").alias("channel_title"),
        F.col("item.snippet.categoryId").cast("int").alias("category_id"),
        F.col("item.statistics.viewCount").cast("bigint").alias("view_count"),
        F.col("item.statistics.likeCount").cast("bigint").alias("like_count"),
        F.col("item.statistics.commentCount").cast("bigint").alias("comment_count"),
        F.to_timestamp(F.col("item.snippet.publishedAt")).alias("published_at"),
        F.col("item.snippet.tags").alias("tags"),
        F.col("item.snippet.thumbnails.high.url").cast("string").alias("thumbnail_url"),
        F.col("item.contentDetails.duration").cast("string").alias("duration"),
    )

    df = df.withColumn("duration_seconds", duration_to_seconds_udf(F.col("duration")))
    df = df.drop("duration")

    df = df.withColumn("trending_date", F.to_date(F.lit(trending_date)))
    df = df.withColumn("region", F.lit(region))

    # Normalize tags: ensure array<string>
    df = df.withColumn(
        "tags",
        F.when(F.col("tags").isNull(), F.array().cast(T.ArrayType(T.StringType()))).otherwise(
            F.col("tags").cast(T.ArrayType(T.StringType()))
        ),
    )

    return df


def main() -> None:
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "raw_s3_path",
            "processed_s3_path",
            "trending_date",
            "region",
            "category_mapping_s3_path",
        ],
    )

    sc = SparkContext.getOrCreate()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    raw_s3_path = args["raw_s3_path"].rstrip("/")
    processed_s3_path = args["processed_s3_path"].rstrip("/")
    trending_date = args["trending_date"]
    region = args["region"]
    category_mapping_s3_path = (args.get("category_mapping_s3_path") or "").strip() or None

    # Read raw JSON for a specific partition (date + region) if path points to bucket root.
    # Recommended input: s3://yt-trending-raw/date=YYYY-MM-DD/region=US/
    if "date=" not in raw_s3_path or "region=" not in raw_s3_path:
        raw_s3_path = f"{raw_s3_path}/date={trending_date}/region={region}/"

    raw_df = spark.read.option("multiline", "false").json(raw_s3_path)
    mapping = load_category_mapping(glue_ctx, category_mapping_s3_path)

    out = flatten_raw_videos(raw_df, trending_date=trending_date, region=region)
    out = with_category_name(out, mapping)

    # Final ordering / selection
    out = out.select(
        "video_id",
        "title",
        "channel_title",
        "category_id",
        "category_name",
        "view_count",
        "like_count",
        "comment_count",
        "published_at",
        "trending_date",
        "duration_seconds",
        "tags",
        "thumbnail_url",
        "region",
    )

    out = out.repartition(F.col("trending_date"), F.col("category_name"), F.col("region"))

    (
        out.write.mode("append")
        .format("parquet")
        .partitionBy("trending_date", "category_name", "region")
        .save(processed_s3_path)
    )

    job.commit()


if __name__ == "__main__":
    main()

