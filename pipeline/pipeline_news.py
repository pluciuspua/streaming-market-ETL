import apache_beam as beam
import json
from nltk.sentiment.vader import SentimentIntensityAnalyzer

analyzer = SentimentIntensityAnalyzer()

class AnalyzeSentiment(beam.DoFn):
    def process(self, element):
        record = json.loads(element.decode("utf-8"))
        text = record["text"]

        score = analyzer.polarity_scores(text)
        record["sentiment_score"] = score["compound"]
        record["sentiment_label"] = (
            "POSITIVE" if score["compound"] > 0.05
            else "NEGATIVE" if score["compound"] < -0.05
            else "NEUTRAL"
        )
        yield record
