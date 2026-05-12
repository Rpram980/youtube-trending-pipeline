import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import boto3
import requests
from botocore.exceptions import ClientError


YOUTUBE_TRENDING_URL = "https://www.googleapis.com/youtube/v3/videos"


@dataclass(frozen=True)
class Config:
    yt_api_key: str
    raw_bucket: str
    yt_region_code: str = "US"
    raw_prefix: str = ""
    max_results: int = 50
    timeout_seconds: int = 20
    max_attempts: int = 6


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    return int(v)


def load_config() -> Config:
    yt_api_key = os.getenv("YT_API_KEY", "").strip()
    raw_bucket = os.getenv("RAW_BUCKET", "").strip()

    if not yt_api_key:
        raise ValueError("Missing env var YT_API_KEY")
    if not raw_bucket:
        raise ValueError("Missing env var RAW_BUCKET")

    yt_region_code = (os.getenv("YT_REGION_CODE", "US") or "US").strip()
    raw_prefix = (os.getenv("RAW_PREFIX", "") or "").strip().strip("/")
    max_results = _env_int("MAX_RESULTS", 50)

    if max_results < 1 or max_results > 50:
        raise ValueError("MAX_RESULTS must be between 1 and 50")

    return Config(
        yt_api_key=yt_api_key,
        raw_bucket=raw_bucket,
        yt_region_code=yt_region_code,
        raw_prefix=raw_prefix,
        max_results=max_results,
        timeout_seconds=_env_int("HTTP_TIMEOUT_SECONDS", 20),
        max_attempts=_env_int("HTTP_MAX_ATTEMPTS", 6),
    )


def _compute_trending_date_utc(now: Optional[datetime] = None) -> str:
    now = now or datetime.now(timezone.utc)
    return now.date().isoformat()


def _build_s3_key(prefix: str, trending_date: str, region: str) -> str:
    base = f"date={trending_date}/region={region}/videos.json"
    if not prefix:
        return base
    return f"{prefix}/{base}"


def _request_with_retry(
    *,
    url: str,
    params: Dict[str, Any],
    timeout_seconds: int,
    max_attempts: int,
) -> Tuple[int, Dict[str, Any]]:
    last_err: Optional[Exception] = None

    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout_seconds)
            status = resp.status_code

            # Retry on throttling/transient upstream errors
            if status in (429, 500, 502, 503, 504):
                raise RuntimeError(f"HTTP {status}: {resp.text[:500]}")

            resp.raise_for_status()
            return status, resp.json()
        except Exception as e:  # noqa: BLE001 (Lambda-friendly)
            last_err = e
            if attempt >= max_attempts:
                break
            # exponential backoff with cap
            sleep_s = min(2 ** (attempt - 1), 30)
            time.sleep(sleep_s)

    raise RuntimeError(f"Failed after {max_attempts} attempts: {last_err}") from last_err


def fetch_trending_videos(cfg: Config) -> Dict[str, Any]:
    params = {
        "part": "snippet,statistics,contentDetails",
        "chart": "mostPopular",
        "regionCode": cfg.yt_region_code,
        "maxResults": cfg.max_results,
        "key": cfg.yt_api_key,
    }
    _, payload = _request_with_retry(
        url=YOUTUBE_TRENDING_URL,
        params=params,
        timeout_seconds=cfg.timeout_seconds,
        max_attempts=cfg.max_attempts,
    )
    return payload


def write_raw_to_s3(*, bucket: str, key: str, payload: Dict[str, Any]) -> None:
    s3 = boto3.client("s3")
    body = (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")

    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
    except ClientError as e:
        raise RuntimeError(f"Failed to write to s3://{bucket}/{key}: {e}") from e


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Event can optionally include:
    - trending_date: 'YYYY-MM-DD' (defaults to current UTC date)
    - region: e.g. 'US' (defaults to YT_REGION_CODE env var)
    """
    cfg = load_config()

    trending_date = (event or {}).get("trending_date") or _compute_trending_date_utc()
    region = (event or {}).get("region") or cfg.yt_region_code

    payload = fetch_trending_videos(cfg)
    # add minimal lineage fields (keeps raw response intact and self-describing)
    payload["_ingested_at_utc"] = datetime.now(timezone.utc).isoformat()
    payload["_trending_date"] = trending_date
    payload["_region"] = region

    key = _build_s3_key(cfg.raw_prefix, trending_date, region)
    write_raw_to_s3(bucket=cfg.raw_bucket, key=key, payload=payload)

    return {
        "status": "ok",
        "raw_s3_uri": f"s3://{cfg.raw_bucket}/{key}",
        "items": len(payload.get("items", []) or []),
        "trending_date": trending_date,
        "region": region,
    }


if __name__ == "__main__":
    # Simple local smoke run (requires AWS credentials in environment)
    result = lambda_handler(event={}, context=None)
    print(json.dumps(result, indent=2))

