import pandas as pd
from transformers import pipeline
import os
from src.utils.config import HF_TOKEN, EMOTION_MODEL

def get_emotion_analyzer():
    """
    Returns an emotion detection pipeline.
    This should be called inside a Spark partition to ensure lazy initialization on workers.
    """
    if HF_TOKEN:
        os.environ["HF_TOKEN"] = HF_TOKEN
    
    # Set a writable cache directory for Hugging Face
    os.environ["HF_HOME"] = "/tmp/huggingface"
        
    # Using a global-ish cache within the worker process to avoid reloading 
    # if multiple batches are processed by the same worker instance
    if not hasattr(get_emotion_analyzer, "_model"):
        print(f"Loading emotion model: {EMOTION_MODEL}")
        get_emotion_analyzer._model = pipeline(
            "text-classification",
            model=EMOTION_MODEL,
            device=-1,  # Default to CPU for Spark tasks
            truncation=True,
            max_length=512
        )
    return get_emotion_analyzer._model

def analyze_emotions_partition(iterator):
    """
    Spark mapInPandas iterator.
    Processes partitions of DataFrames.
    Loads the model once per partition.
    """
    # The model will be loaded the first time nlp() is called in this partition
    nlp = get_emotion_analyzer()
    
    for pdf in iterator:
        # Ensure we have the required column
        if "text_cleaned" not in pdf.columns:
            yield pd.DataFrame(columns=["comment_id", "emotion_label", "emotion_score"])
            continue

        texts = pdf["text_cleaned"].fillna("").astype(str).tolist()
        
        if texts:
            # Run inference in batches
            results = nlp(texts, batch_size=16)
            
            pdf["emotion_label"] = [r["label"] for r in results]
            pdf["emotion_score"] = [float(r["score"]) for r in results]
        else:
            pdf["emotion_label"] = []
            pdf["emotion_score"] = []
            
        # We only need to return the join key and the new features
        yield pdf[["comment_id", "emotion_label", "emotion_score"]]
