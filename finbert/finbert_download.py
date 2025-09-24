from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch

from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch

model_name = "ProsusAI/finbert"

tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSequenceClassification.from_pretrained(model_name)

# Save to local directory
model.save_pretrained("./finbert_model")
tokenizer.save_pretrained("./finbert_model")
