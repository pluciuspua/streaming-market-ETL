from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import logging

app = FastAPI()
################################
###FOR LOCAL TESTING PURPOSES###
################################
# To run the API locally, use the command below:
#uvicorn finbert.finbert_api:app --reload --host 0.0.0.0 --port 8080

# Load FinBERT
MODEL_NAME = "ProsusAI/finbert"
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model.to(device)
model.eval()

labels = ["positive", "negative", "neutral"]

class BatchRequest(BaseModel):
    texts: List[str]

class Request(BaseModel):
    text: str

@app.get("/healthz")
def health_check():
    return {"status": "ok"}


@app.post("/predict")
def predict_single(request: Request):
    inputs = tokenizer(request.text, return_tensors="pt", truncation=True, padding=True)
    inputs = {k: v.to(device) for k, v in inputs.items()}
    with torch.no_grad():
        outputs = model(**inputs)
        # Diagnostic: raw logits
        probs = torch.nn.functional.softmax(outputs.logits, dim=1)
        # Diagnostic: probabilities
        scores, indices = torch.max(probs, dim=1)
        # Diagnostic: predicted index and mapped label
        predicted_index = indices[0].item()
        mapped_label = labels[predicted_index]
    response = {"label": mapped_label, "score": scores[0].item()}
    return response

@app.post("/batch_predict")
def predict_batch(request: BatchRequest):
    if not request.texts:
        return []
    inputs = tokenizer(request.texts, return_tensors="pt", truncation=True, padding=True)
    inputs = {k: v.to(device) for k, v in inputs.items()}
    with torch.no_grad():
        outputs = model(**inputs)
        probs = torch.nn.functional.softmax(outputs.logits, dim=1)
        scores, indices = torch.max(probs, dim=1)

    results = []
    for i, text in enumerate(request.texts):
        idx = indices[i].item()
        score = scores[i].item()
        mapped_label = labels[idx]
        # Diagnostic for each item
        results.append({"text": text, "label": mapped_label, "score": score})
    return results
