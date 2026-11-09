"""
Clickstream sessionization with PySpark window functions.

A new session starts when a user has been inactive for MORE than 30 minutes.

Run:  python sessionize.py [path/to/clickstream.csv]
"""
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

SESSION_GAP_SECONDS = 30 * 60  # 30 minutes of inactivity

SCHEMA = StructType([
    StructField("user_id", StringType()),
    StructField("ts", TimestampType()),
    StructField("url", StringType()),
])


def assign_sessions(events):
    """Step 1: tag every event with a session_id."""
    # One "lane" per user, events in time order.
    by_user = Window.partitionBy("user_id").orderBy("ts")

    return (
        events
        # Look one row back: when was this user's previous click?
        .withColumn("prev_ts", F.lag("ts").over(by_user))
        # Seconds since the previous click (null for the user's first click).
        .withColumn("gap_seconds",
                    F.col("ts").cast("long") - F.col("prev_ts").cast("long"))
        # 1 = this click starts a new session, 0 = it continues the current one.
        .withColumn("is_new_session",
                    F.when(F.col("prev_ts").isNull()
                           | (F.col("gap_seconds") > SESSION_GAP_SECONDS), 1)
                     .otherwise(0))
        # Running total of the 1s = session number for this user (1, 2, 3, ...).
        .withColumn("session_num",
                    F.sum("is_new_session").over(
                        by_user.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
        .withColumn("session_id",
                    F.concat_ws("-", "user_id", F.col("session_num").cast("string")))
    )


def summarize_sessions(sessionized):
    """Step 2: collapse events into one row per session."""
    return (
        sessionized
        .groupBy("user_id", "session_id")
        .agg(
            F.min("ts").alias("session_start"),
            F.max("ts").alias("session_end"),
            F.count("*").alias("num_events"),
        )
        .withColumn("session_seconds",
                    F.col("session_end").cast("long") - F.col("session_start").cast("long"))
        # A session belongs to the day it started on.
        .withColumn("session_date", F.to_date("session_start"))
    )


def daily_metrics(sessions):
    """Step 3: sessions per day and average session length."""
    return (
        sessions
        .groupBy("session_date")
        .agg(
            F.count("*").alias("sessions"),
            F.round(F.avg("session_seconds") / 60, 2).alias("avg_session_minutes"),
        )
        .orderBy("session_date")
    )


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "data/clickstream.csv"

    spark = (
        SparkSession.builder
        .appName("clickstream-sessionization")
        .master("local[*]")
        # Read and compute timestamps in UTC so results don't shift with the
        # machine's local time zone or daylight-saving changes.
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    events = spark.read.csv(path, header=True, schema=SCHEMA,
                            timestampFormat="yyyy-MM-dd HH:mm:ss")

    sessionized = assign_sessions(events)
    sessions = summarize_sessions(sessionized)
    daily = daily_metrics(sessions)

    print("\n== Events with session ids ==")
    sessionized.orderBy("user_id", "ts").show(truncate=False)

    print("== One row per session ==")
    sessions.orderBy("user_id", "session_start").show(truncate=False)

    print("== Sessions per day & average session length ==")
    daily.show(truncate=False)

    print("== Overall average session length (minutes) ==")
    sessions.agg(F.round(F.avg("session_seconds") / 60, 2)
                 .alias("avg_session_minutes")).show()

    spark.stop()


if __name__ == "__main__":
    main()
