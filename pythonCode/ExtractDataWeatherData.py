import requests
import os
import dotenv
from datetime import datetime
from config.SparkSession import get_spark

from pyspark.sql.functions import col, to_timestamp


dotenv.load_dotenv()
# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder.getOrCreate()

API_KEY = "52f5b9b2f30ac7ee5020df9a740aa2da"

CITIES = [
    'Tbilisi', 'Baku', 'Yerevan', 'Ankara', 'Athens',
    'Sofia', 'Bucharest', 'Budapest', 'Vienna', 'Berlin',
    'Paris', 'Madrid', 'Rome', 'Warsaw', 'Prague',
    'Bratislava', 'Zagreb', 'Ljubljana', 'Belgrade', 'Skopje',
    'Podgorica', 'Sarajevo', 'Oslo', 'Helsinki', 'Copenhagen',
    'Tallinn', 'Riga', 'Vilnius', 'Luxembourg', 'Monaco',
    'Chisinau', 'Valletta', 'Andorra la Vella', 'Vaduz', 'San Marino',
    'Bern', 'Kyiv', 'London', 'Dublin', 'Reykjavik'
]

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "weather-stream"

def weather_data():
    weather_data = []
    for city in CITIES:
        try:
            url = (
                f"https://api.openweathermap.org/data/2.5/weather"
                f"?q={city}&appid={API_KEY}&units=metric"
            )

            resp = requests.get(url, timeout=10)
            resp.raise_for_status()

            data = resp.json()

            event = {
                "city": city,
                "temperature": float(data["main"]["temp"]),
                "humidity": int(data["main"]["humidity"]),
                "weather": data["weather"][0]["description"],
                "event_time": datetime.utcnow().isoformat(),
                "pressure": int(data["main"]["pressure"]),
                "wind_speed": float(data.get("wind", {}).get("speed", 0.0)),
                "source": "openweathermap"
            }

            weather_data.append(event)

            # print(
            #     f"Extracted: {city} | "
            #     f"{event['temperature']}°C | "
            #     f"{event['weather']}"
            # )

        except Exception as e:
            print(f"Error {city}: {e}")

    return weather_data



def write_to_postgres(weather_data):
    spark = get_spark()
    weather_data = spark.createDataFrame(weather_data)
    DB_POSTGRESQL_DATABASE = os.getenv("DB_POSTGRESQL_DATABASE")
    DB_POSTGRESQL_USER = os.getenv("DB_POSTGRESQL_USER")
    DB_POSTGRESQL_PASSWORD = os.getenv("DB_POSTGRESQL_PASSWORD")
    DB_POSTGRESQL_DRIVER = os.getenv("DB_POSTGRESQL_DRIVER")
    DB_POSTGRESQL_HOST = os.getenv("DB_POSTGRESQL_HOST", "localhost")
    DB_POSTGRESQL_PORT = os.getenv("DB_POSTGRESQL_PORT", "5432")

    # Spark JDBC
    POSTGRES_URL = (
        f"jdbc:postgresql://{DB_POSTGRESQL_HOST}:"
        f"{DB_POSTGRESQL_PORT}/{DB_POSTGRESQL_DATABASE}"
    )

    POSTGRES_PROPERTIES = {
        "user": DB_POSTGRESQL_USER,
        "password": DB_POSTGRESQL_PASSWORD,
        "driver": DB_POSTGRESQL_DRIVER
    }

    # Transform event_time -> TimestampType
    df = weather_data.withColumn(
        "event_time",
        to_timestamp(col("event_time"))
    )

    # PostgreSQL Bronze Layer Load
    df.write.jdbc(
        url=POSTGRES_URL,
        table="silver.weather_data1",
        mode="append",
        properties=POSTGRES_PROPERTIES
    )

    print("✅ Weather data successfully loaded into PostgreSQL!")

# weather_data = weather_data()
# weather_data = spark.createDataFrame(weather_data)
# weather_data.show(truncate=False)
# write_to_postgres(weather_data)
def start():
    df = weather_data()
    write_to_postgres(df)

if __name__ == "__main__":
    start()
