import boto3
import io
import json
import pandas as pd

from typing import Dict, List

def filter_for_relevant_comments(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Filter a comments DataFrame for keywords before running sentiment inference"""
    keywords = [
        "coventry",
        "pusb",
        "ccfc",
        "sky blues",
        "sky blue",
        "cbs",
        "arena",
        "lampard",
        "wright",
        "sheaf",
        "sakamoto",
        "bidwell",
        "eccles"
    ]
    df = df[
        df[column].str.lower().apply(
            lambda text: any(kw in text for kw in keywords)
        )
    ]
    return df

def process_llm_responses(responses: List[Dict]) -> List[Dict]:
    sentiments = []
    for response in responses:
        try:
            if not response["error"]:
                raw_content: str = response["response"]["choices"][0]["message"]["content"]
                cleaned = raw_content.strip()
                parsed = json.loads(cleaned)
                sentiments.append({
                    "comment_id": response["comment_id"],
                    "sentiment": parsed["sentiment"],
                    "confidence": parsed["confidence"]
                })
        except json.decoder.JSONDecodeError as e:
            print(f"JSONDecodeError for comment: {response["comment_id"]}, string: {raw_content}")
    return sentiments

def write_sentiment_df_to_parquet(df: pd.DataFrame, partition_on: List[str], s3_key_prefix: str) -> None:
    df.to_parquet(
        f"s3://bmooreawsbucket/{s3_key_prefix}/",
        engine="pyarrow",
        index=False,
        partition_cols=partition_on
    )
