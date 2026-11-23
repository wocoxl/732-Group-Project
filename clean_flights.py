
"""CMPT 732: Flight Data Cleaning Pipeline using PySpark."""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


TEXT = [
    "Marketing_Airline_Network", "Operating_Airline",
    "Tail_Number", "Origin", "Dest",
    "OriginCityName", "DestCityName",
    "CancellationCode", "IATA_Code_Marketing_Airline"
]

INTS = [
    "Year", "Month", "Quarter", "DayofMonth", "DayOfWeek",
    "Flight_Number_Marketing_Airline",
    "Flight_Number_Operating_Airline",
    "OriginAirportID", "DestAirportID",
    "CRSDepTime", "DepTime", "CRSArrTime", "ArrTime",
    "DivAirportLandings"
]

FLOATS = [
    "DepDelay", "DepDelayMinutes",
    "ArrDelay", "ArrDelayMinutes",
    "CarrierDelay", "WeatherDelay", "NASDelay",
    "SecurityDelay", "LateAircraftDelay",
    "CRSElapsedTime", "ActualElapsedTime",
    "Distance", "TaxiOut", "TaxiIn"
]

FLAGS = ["Cancelled", "Diverted", "DepDel15", "ArrDel15"]

NEEDED = ["FlightDate"] + TEXT + INTS + FLOATS + FLAGS


def clock_minutes(c):
    """Convert HHMM time to minutes since midnight."""
    h = F.floor(c / F.lit(100))
    m = c % F.lit(100)

    valid = (
        c.isNotNull()
        & (h >= 0) & (h <= 24)
        & (m >= 0) & (m < 60)
        & ((h < 24) | (m == 0))
    )

    return F.when(valid, h * 60 + m)


def transform(raw):
    # 1. Normalize column names
    for old in raw.columns:
        new = old.strip()
        if new and new != old and new not in raw.columns:
            raw = raw.withColumnRenamed(old, new)

    missing = sorted(set(NEEDED) - set(raw.columns))

    if missing:
        raise ValueError(
            "Missing BTS columns: " + ", ".join(missing)
        )

    df = raw.select(
        *[F.col("`" + x + "`").alias(x) for x in NEEDED]
    )

    # 2. Clean text columns
    for x in TEXT:
        v = F.trim(F.col(x))

        df = df.withColumn(
            x,
            F.when(
                (v == "") | (F.lower(v) == "null"),
                F.lit(None)
            ).otherwise(v)
        )

    # 3. Convert numeric columns
    for x in INTS:
        df = df.withColumn(
            x, F.trim(F.col(x)).cast("int")
        )

    for x in FLOATS + FLAGS:
        df = df.withColumn(
            x, F.trim(F.col(x)).cast("double")
        )

    # 4. Clean flight dates
    df = df.withColumn(
        "flight_date",
        F.to_date("FlightDate", "yyyy-MM-dd")
    )

    # Remove rows without essential identifiers
    df = df.filter(
        F.col("flight_date").isNotNull()
        & F.col("Origin").isNotNull()
        & F.col("Dest").isNotNull()
    )

    # 5. Partition fields
    df = (
        df.withColumn("year", F.year("flight_date"))
          .withColumn("month", F.month("flight_date"))
    )

    # 6. Route and flight status
    df = df.withColumn(
        "route", F.concat_ws("-", "Origin", "Dest")
    )

    df = df.withColumn(
        "is_cancelled", F.col("Cancelled") == 1.0
    )

    df = df.withColumn(
        "is_diverted", F.col("Diverted") == 1.0
    )

    df = df.withColumn(
        "is_completed",
        (F.col("Cancelled") == 0.0)
        & (F.col("Diverted") == 0.0)
    )

    # 7. Delay indicators
    df = df.withColumn(
        "is_arrival_delayed_15",
        F.when(
            F.col("is_completed")
            & F.col("ArrDelayMinutes").isNotNull(),
            F.col("ArrDelayMinutes") >= 15
        )
    )

    df = df.withColumn(
        "is_departure_delayed_15",
        F.when(
            F.col("is_completed")
            & F.col("DepDelayMinutes").isNotNull(),
            F.col("DepDelayMinutes") >= 15
        )
    )

    # 8. Scheduled and actual time features
    dep_min = clock_minutes(F.col("CRSDepTime"))

    df = df.withColumn(
        "scheduled_departure_ts",
        F.when(
            dep_min.isNotNull(),
            F.expr("cast(flight_date as timestamp)")
            + dep_min.cast("long")
            * F.expr("INTERVAL 1 MINUTE")
        )
    )

    df = df.withColumn(
        "scheduled_arrival_ts",
        F.when(
            F.col("scheduled_departure_ts").isNotNull()
            & F.col("CRSElapsedTime").isNotNull()
            & (F.col("CRSElapsedTime") >= 0),
            F.col("scheduled_departure_ts")
            + F.col("CRSElapsedTime").cast("long")
            * F.expr("INTERVAL 1 MINUTE")
        )
    )

    df = df.withColumn(
        "actual_departure_local_ts",
        F.when(
            F.col("scheduled_departure_ts").isNotNull()
            & F.col("DepDelay").isNotNull(),
            F.col("scheduled_departure_ts")
            + F.col("DepDelay").cast("long")
            * F.expr("INTERVAL 1 MINUTE")
        )
    )

    df = df.withColumn(
        "actual_arrival_local_ts",
        F.when(
            F.col("scheduled_arrival_ts").isNotNull()
            & F.col("ArrDelay").isNotNull(),
            F.col("scheduled_arrival_ts")
            + F.col("ArrDelay").cast("long")
            * F.expr("INTERVAL 1 MINUTE")
        )
    )

    # 9. Additional analysis features
    df = df.withColumn(
        "scheduled_departure_hour",
        F.floor(dep_min / 60).cast("int")
    )

    df = df.withColumn(
        "day_of_week",
        F.dayofweek("flight_date")
    )

    df = df.withColumn(
        "is_weekend",
        F.col("day_of_week").isin(1, 7)
    )

    df = df.withColumn(
        "season",
        F.when(F.col("month").isin(12, 1, 2), "winter")
         .when(F.col("month").isin(3, 4, 5), "spring")
         .when(F.col("month").isin(6, 7, 8), "summer")
         .otherwise("fall")
    )

    return df.drop("Year", "Month", "FlightDate")


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--mode",
        choices=["overwrite", "append"],
        default="overwrite"
    )

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("CMPT732-Flight-ETL")
        .getOrCreate()
    )

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set(
        "spark.sql.sources.partitionOverwriteMode",
        "dynamic"
    )

    # Extract
    raw = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .csv(args.input)
    )

    # Transform
    clean = transform(raw)

    # Load: Hive-style partitioned Parquet
    (
        clean.write
        .mode(args.mode)
        .partitionBy("year", "month")
        .parquet(args.output)
    )

    spark.stop()


if __name__ == "__main__":
    main()

