# YouTube Trending Data Pipeline

An automated data pipeline that collects, transforms, 
and visualizes YouTube trending videos for India.

## Architecture

YouTube API → Lambda → S3 Raw → Glue ETL → 
S3 Processed → Athena → Power BI Dashboard

## Components

- **Lambda** — Extracts daily trending videos from YouTube API
- **S3** — Stores raw JSON and processed Parquet files
- **Glue** — Transforms raw JSON to partitioned Parquet
- **Athena** — SQL queries on processed data
- **Power BI** — 4-page dashboard visualization
- **EventBridge** — Daily automation schedule

## Setup

See the setup guide for step-by-step instructions.

## Environment Variables

Set these before running locally:

YT_API_KEY      = your YouTube Data API key
RAW_BUCKET      = your S3 raw bucket name
YT_REGION_CODE  = IN
MAX_RESULTS     = 50

## Schedule

- 8:00 AM UTC  — Lambda extracts trending videos
- 8:20 AM UTC  — Glue transforms to Parquet
- 9:00 AM UTC  — Power BI dashboard refreshes
