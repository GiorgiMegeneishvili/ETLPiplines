import os
import time
from datetime import datetime, timezone

import dotenv
import requests
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from config.SparkSession import get_spark

dotenv.load_dotenv()

API_URL = "https://api.coingecko.com/api/v3/simple/price"
TARGET_TABLE = "silver.crypto_prices"
NUMERIC_FIELDS = ("price", "market_cap", "volume_24h", "change_24h")

CRYPTO_SCHEMA = StructType([
    StructField("crypto",       StringType(), False),
    StructField("price",        DoubleType(), True),
    StructField("market_cap",   DoubleType(), True),
    StructField("volume_24h",   DoubleType(), True),
    StructField("change_24h",   DoubleType(), True),
    StructField("last_updated", StringType(), True),
])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def fmt(value, pattern="{:,.2f}"):
    return pattern.format(value) if value is not None else "N/A"


def get_postgres_config():
    required = ["DB_POSTGRESQL_DATABASE", "DB_POSTGRESQL_USER", "DB_POSTGRESQL_PASSWORD"]
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        raise ValueError(f"Missing env vars: {missing}")

    host = os.getenv("DB_POSTGRESQL_HOST", "localhost")
    port = os.getenv("DB_POSTGRESQL_PORT", "5432")
    db = os.getenv("DB_POSTGRESQL_DATABASE")

    url = f"jdbc:postgresql://{host}:{port}/{db}"
    props = {
        "user": os.getenv("DB_POSTGRESQL_USER"),
        "password": os.getenv("DB_POSTGRESQL_PASSWORD"),
        "driver": os.getenv("DB_POSTGRESQL_DRIVER") or "org.postgresql.Driver",
    }
    return url, props


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------
def fetch_crypto_prices(crypto_ids=("bitcoin", "ethereum", "dogecoin"),
                        max_retries=3, verbose=True):
    params = {
        "ids": ",".join(crypto_ids),
        "vs_currencies": "usd",
        "include_market_cap": "true",
        "include_24hr_vol": "true",
        "include_24hr_change": "true",
        "include_last_updated_at": "true",
    }

    # --- API call + retry on 429 ---
    for attempt in range(1, max_retries + 1):
        response = requests.get(API_URL, params=params, timeout=30)
        if response.status_code == 429:
            wait = 60 * attempt
            print(f"Rate limit (attempt {attempt}/{max_retries}), waiting {wait}s...")
            time.sleep(wait)
            continue
        response.raise_for_status()
        break
    else:
        raise RuntimeError("CoinGecko rate limit: all retries exhausted")

    data = response.json()
    crypto_data = []

    for crypto in crypto_ids:
        info = data.get(crypto)
        if not info:
            print(f"{crypto.capitalize()} data not available.")
            continue

        ts = info.get("last_updated_at")
        last_updated = (
            datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            if ts else None
        )

        event = {
            "crypto": crypto,
            "price": info.get("usd"),
            "market_cap": info.get("usd_market_cap"),
            "volume_24h": info.get("usd_24h_vol"),
            "change_24h": info.get("usd_24h_change"),
            "last_updated": last_updated,
        }
        crypto_data.append(event)

        if verbose:
            print(f"--- {crypto.capitalize()} ---")
            print(f"Price: ${fmt(event['price'])}")
            print(f"Market Cap: ${fmt(event['market_cap'])}")
            print(f"24h Volume: ${fmt(event['volume_24h'])}")
            print(f"24h Change: {fmt(event['change_24h'], '{:.2f}')}%")
            print(f"Last Updated (UTC): {last_updated}\n")

    return crypto_data


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------
def write_to_postgres(crypto_data, table=TARGET_TABLE):
    if not crypto_data:
        print("⚠️ No data to load, skipping.")
        return

    spark = get_spark()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    # int/float mix-ის თავიდან ასაცილებლად ყველა რიცხვი float-ად
    rows = [
        {**r, **{k: float(r[k]) if r[k] is not None else None for k in NUMERIC_FIELDS}}
        for r in crypto_data
    ]

    df = (
        spark.createDataFrame(rows, schema=CRYPTO_SCHEMA)
             .withColumn("last_updated",
                         to_timestamp(col("last_updated"), "yyyy-MM-dd HH:mm:ss"))
    )

    url, props = get_postgres_config()

    # PostgreSQL Bronze Layer Load
    df.write.jdbc(
        url=url,
        table=table,
        mode="append",
        properties=props,
    )

    print(f"✅ {len(rows)} rows loaded into {table}")


# ---------------------------------------------------------------------------
# Entry point (Airflow PythonOperator -> python_callable=start)
# ---------------------------------------------------------------------------
def start():
    data = fetch_crypto_prices(verbose=False)
    write_to_postgres(data)


if __name__ == "__main__":
    start()
