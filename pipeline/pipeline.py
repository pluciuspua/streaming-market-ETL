import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from datetime import datetime
import os
import yaml

with open("config.yaml", "r") as f:
    cfg = yaml.safe_load(f)
    if cfg:
        gcp_project = cfg["gcp_project"]
        gcp_region = cfg["gcp_region"]
print(gcp_project, gcp_region)


# ------------------------------
# Parsing Pub/Sub Binance message
# ------------------------------
class ParseBinanceMessage(beam.DoFn):
    def process(self, element):
        try:
            msg = json.loads(element)
            # Check if message has 'k' key (Binance kline data)
            if 'k' in msg:
                k = msg["k"]
                ts = int(k["t"]) / 1000.0  # convert ms → seconds
                record = {
                    "symbol": k["s"],
                    "open": float(k["o"]),
                    "close": float(k["c"]),
                    "high": float(k["h"]),
                    "low": float(k["l"]),
                    "volume": float(k["v"]),
                    "event_time": datetime.utcfromtimestamp(ts).isoformat()
                }

                yield beam.window.TimestampedValue(record, ts)
        except Exception as e:
            print(f"Error parsing message: {e}")
            print(f"Message content: {element}")


# ------------------------------
# Transform records to KV pairs
# ------------------------------
class AddKeyToRecord(beam.DoFn):
    def process(self, record):
        # Extract the symbol as the key
        key = record["symbol"]
        # Emit (key, record) pair
        yield (key, record)


# ------------------------------
# Aggregate windowed statistics
# ------------------------------
class ToBQRow(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        symbol, records = element
        closes = [r["close"] for r in records]
        highs = [r["high"] for r in records]
        lows = [r["low"] for r in records]
        vols = [r["volume"] for r in records]

        row = {
            "symbol": symbol,
            "window_start": window.start.to_utc_datetime(),
            "window_end": window.end.to_utc_datetime(),
            "avg_close": sum(closes) / len(closes),
            "max_high": max(highs),
            "min_low": min(lows),
            "total_volume": sum(vols),
        }
        print(f"Writing to BQ: {row}")  # Debug output
        yield row


# ------------------------------
# Main pipeline
# ------------------------------
def run():
    # Set the project ID explicitly
    os.environ["GOOGLE_CLOUD_PROJECT"] = "live-data-pipeline-471309"

    # ------------------------------
    # Pipeline options for Dataflow
    # ------------------------------
    options = PipelineOptions(
        runner = "DataflowRunner",
        streaming=True,
        save_main_session=True,
        project="live-data-pipeline-471309",
        region="asia-southeast1",
        job_name="crypto-streaming-job-02",
        temp_location="gs://live-data-pipeline-bkt/crypto_data_temp",
    )

    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
                p
                | "ReadPubSub" >> beam.io.ReadFromPubSub(
            subscription="projects/live-data-pipeline-471309/subscriptions/live_data_crypto-sub"
        ).with_output_types(bytes)
                | "Decode" >> beam.Map(lambda x: x.decode("utf-8"))
                | "Parse" >> beam.ParDo(ParseBinanceMessage())
                | "Window" >> beam.WindowInto(beam.window.FixedWindows(300))  # 5-min windows
                | "AddKey" >> beam.ParDo(AddKeyToRecord())  # Add this step to create (key, value) pairs
                | "GroupBySymbol" >> beam.GroupByKey()
                | "Aggregate" >> beam.ParDo(ToBQRow())
                | "WriteBQ" >> beam.io.WriteToBigQuery(
            table="live-data-pipeline-471309.crypto_market_data.crypto_aggregates",
            schema={
                "fields": [
                    {"name": "symbol", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "window_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
                    {"name": "window_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
                    {"name": "avg_close", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "max_high", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "min_low", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "total_volume", "type": "FLOAT", "mode": "NULLABLE"},
                ]
            },
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
        )


if __name__ == "__main__":
    run()