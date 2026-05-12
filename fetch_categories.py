import argparse
import json
import sys
from typing import Any, Dict

import requests


VIDEO_CATEGORIES_URL = "https://www.googleapis.com/youtube/v3/videoCategories"


def fetch_categories(api_key: str, region: str) -> Dict[str, Any]:
    resp = requests.get(
        VIDEO_CATEGORIES_URL,
        params={"part": "snippet", "regionCode": region, "key": api_key},
        timeout=20,
    )
    resp.raise_for_status()
    payload = resp.json()

    mapping = {}
    for item in payload.get("items", []) or []:
        cat_id = item.get("id")
        title = ((item.get("snippet") or {}).get("title")) or None
        if cat_id and title:
            mapping[int(cat_id)] = title

    return {
        "region": region,
        "categories": mapping,
        "raw": payload,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api-key", required=True)
    ap.add_argument("--region", default="US")
    ap.add_argument("--out", required=True, help="Output JSON file path")
    args = ap.parse_args()

    doc = fetch_categories(args.api_key, args.region)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(f"Wrote category mapping to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

