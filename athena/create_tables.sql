-- Athena DDL for processed Parquet dataset produced by Glue ETL.
-- Update the LOCATION to match your S3 prefix.

CREATE DATABASE IF NOT EXISTS youtube_analytics;

-- Processed dataset layout (recommended):
-- s3://yt-trending-processed/trending_videos/
--   trending_date=YYYY-MM-DD/category_name=Music/region=US/part-....parquet

CREATE EXTERNAL TABLE IF NOT EXISTS youtube_analytics.trending_videos (
  video_id           string,
  title              string,
  channel_title      string,
  category_id        int,
  view_count         bigint,
  like_count         bigint,
  comment_count      bigint,
  published_at       timestamp,
  duration_seconds   int,
  tags               array<string>,
  thumbnail_url      string
)
PARTITIONED BY (
  trending_date      date,
  category_name      string,
  region             string
)
STORED AS PARQUET
LOCATION 's3://yt-trending-processed/trending_videos/'
TBLPROPERTIES (
  'parquet.compress'='SNAPPY'
);

-- Discover partitions (run after new data arrives)
MSCK REPAIR TABLE youtube_analytics.trending_videos;

-- Sample query:
-- SELECT category_name,
--        COUNT(*) AS video_count,
--        AVG(view_count) AS avg_views
-- FROM youtube_analytics.trending_videos
-- WHERE trending_date = current_date
--   AND region = 'US'
-- GROUP BY category_name
-- ORDER BY avg_views DESC;

