import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from datetime import datetime, timedelta
import logging
import requests
import yaml
import uuid

with open("config.yaml", "r") as f:
    cfg = yaml.safe_load(f)
    if cfg:
        gcp_project_id = cfg["gcp_project_id"]
        gcp_region = cfg["gcp_region"]
        gcp_temp_bucket = cfg["temp_location"]
        bq_table_crypto = cfg["bq_table_crypto"]
        bq_table_news = cfg["bq_table_news"]
        crypto_sub = cfg["subscribers"]["crypto"]
        news_sub = cfg["subscribers"]["news"]
        FINBERT_URL = cfg["finbert_url"]
    else:
        raise Exception("config.yaml is missing")


# Parsing Pub/Sub Binance message

class ParseBinanceMessage(beam.DoFn):
    '''Custom Function to parse the required fields for Binance message'''
    def process(self, element):
        try:
            msg = json.loads(element)
            if 'k' in msg:
                k = msg["k"]
                ts = int(k["t"]) / 1000.0
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


# Transform records to KV pairs
class AddKeyToRecord(beam.DoFn):
    '''Custom Function to add key to record for GroupByKey'''
    def process(self, record):
        key = record["symbol"]
        yield (key, record)


# Aggregate windowed statistics
class ToBQRow(beam.DoFn):
    '''Custom Function to transform aggreated records to BQ row format'''
    def process(self, element, window=beam.DoFn.WindowParam):
        symbol, records = element
        # Sort records by event_time to get first and last
        sorted_records = sorted(records, key=lambda r: r["event_time"])
        opens = [r["open"] for r in records]
        closes = [r["close"] for r in records]
        highs = [r["high"] for r in records]
        lows = [r["low"] for r in records]
        vols = [r["volume"] for r in records]

        row = {
            "symbol": symbol,
            "window_start": window.start.to_utc_datetime() + timedelta(hours=8), #--> convert to SGT
            "window_end": window.end.to_utc_datetime() + timedelta(hours=8), #--> convert to SGT
            "avg_close": sum(closes) / len(closes) if closes else None,
            "open_price": sorted_records[0]["open"],
            "close_price": sorted_records[-1]["close"],
            "max_high": max(highs),
            "min_low": min(lows),
            "total_volume": sum(vols),
        }
        yield row

class ParseDateTime(beam.DoFn):
    '''Custom Function to adjust the tz time format to UTC time format for BQ'''
    def process(self, element):
        from datetime import datetime
        record = element
        dt_object = datetime.strptime(record["time_published"], '%Y%m%dT%H%M%S') + timedelta(hours=8) #--> convert to SGT
        reformatted_dt = dt_object.strftime('%Y-%m-%d %H:%M:%S')
        record['time_published'] = reformatted_dt
        yield record

class FinBertBatchPredictDoFn(beam.DoFn):
    def process(self, batch):
        key, elements = batch
        texts = [el.get("description", "") for el in elements]

        # for confirming if the API call is working and batching is correct
        logging.info(f"[FinBERT] Processing batch key={key}, size={len(elements)}")
        logging.info(f"[FinBERT] Sample texts (first 2): {texts[:2]}")

        try:
            resp = requests.post(
                FINBERT_URL + "/batch_predict",
                json={"texts": texts},
                timeout=300
            )
            resp.raise_for_status()
            preds = resp.json()
            logging.info(f"[FinBERT] API returned {len(preds)} predictions")
        except Exception as e:
            logging.error(f"[FinBERT] API call failed: {e}")
            preds = [{"label": None, "score": None} for _ in elements]

        # check alignment
        if len(preds) != len(elements):
            logging.error(
                f"[FinBERT] Prediction length mismatch: "
                f"{len(preds)} preds vs {len(elements)} elements"
            )

        for el, pred in zip(elements, preds):
            el["finbert_sentiment"] = pred.get("label")
            el["finbert_score"] = pred.get("score")
            yield el
# ------------------------------
# Main pipeline
def run():
    now = datetime.now().strftime("%Y%m%d-%H%M")
    job_name = "crypto-streaming-job-{now}".format(now=now)
    # runner options
    options = PipelineOptions(
        runner = "DataflowRunner",
        streaming=True,
        save_main_session=True,
        project=gcp_project_id,
        region=gcp_region,
        job_name=job_name,
        temp_location=gcp_temp_bucket,
    )

    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        crypto = (
                p
                | "ReadPubSub" >> beam.io.ReadFromPubSub(
                        subscription= "projects/{gcp_project_id}/subscriptions/{crypto_sub}".format(gcp_project_id=gcp_project_id, crypto_sub=crypto_sub)
                    ).with_output_types(bytes)
                | "Decode" >> beam.Map(lambda x: x.decode("utf-8"))
                | "Parse" >> beam.ParDo(ParseBinanceMessage())
                | "Window" >> beam.WindowInto(beam.window.FixedWindows(300))  # gather data in 5 min windows
                | "AddKey" >> beam.ParDo(AddKeyToRecord())
                | "GroupBySymbol" >> beam.GroupByKey()
                | "Aggregate" >> beam.ParDo(ToBQRow())
                | "WriteBQ" >> beam.io.WriteToBigQuery(
            table=bq_table_crypto,
            schema={
                "fields": [
                    {"name": "symbol", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "window_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
                    {"name": "window_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
                    {"name": "open_price", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "close_price", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "avg_close", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "max_high", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "min_low", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "total_volume", "type": "FLOAT", "mode": "NULLABLE"},
                ]
            },
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            method=beam.io.gcp.bigquery.WriteToBigQuery.Method.STREAMING_INSERTS
        )
        )
        news = (
                p
                | "ReadPubSubNews" >> beam.io.ReadFromPubSub(
                        subscription= "projects/{gcp_project_id}/subscriptions/{news_sub}".format(gcp_project_id=gcp_project_id, news_sub=news_sub)
                    ).with_output_types(bytes)
                | "DecodeNews" >> beam.Map(lambda x: x.decode("utf-8"))
                | "ParseAndFlattenNews" >> beam.FlatMap(lambda x: json.loads(x)) # --> use flatmap bc the input is list of dicts
                | "ParseDateTimeNews" >> beam.ParDo(ParseDateTime())
                | "ToBQRowNews" >> beam.Map(lambda record: {
                    "news_id": str(uuid.uuid4()),
                    "title": record.get("title", ""),
                    "description": record.get("summary", ""),
                    "url": record.get("url", ""),
                    "source": record.get("source", ""),
                    "published_at": record.get("time_published", ""),
                    "symbol": record.get("symbol", ""),
                    "alphavantage_sentiment": record.get("sentiment", ""),
                    "alphavantage_sentiment_score": float(record.get("sentiment_score", 0)),
                    "finbert_sentiment": None,
                    "finbert_score": None
                })
                | "WindowNews" >> beam.WindowInto(beam.window.FixedWindows(60))  # gaterh data in 1 min windows
                | "AddDummyKey" >> beam.Map(lambda x: ("key", x))
                | "Batch" >> beam.transforms.util.GroupIntoBatches(10)  # mini batch in payload size of max 10,
                | "FinBERTBatch" >> beam.ParDo(FinBertBatchPredictDoFn())
                | "WriteBQNews" >> beam.io.WriteToBigQuery(
            table=bq_table_news,
            schema={
                "fields": [
                    {"name": "news_id", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "symbol", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "title", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "description", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "url", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "source", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "published_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
                    {"name": "alphavantage_sentiment", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "alphavantage_sentiment_score", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "finbert_sentiment", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "finbert_score", "type": "FLOAT", "mode":"NULLABLE"}
                ]
            },
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            method=beam.io.gcp.bigquery.WriteToBigQuery.Method.STREAMING_INSERTS
        )
        )

if __name__ == "__main__":
    run()

#python pipeline.py --project=live-data-pipeline-471309 --region=asia-southeast1 --temp_location=gs://live-data-pipeline-bkt/crypto_data_temp --runner=DataflowRunner