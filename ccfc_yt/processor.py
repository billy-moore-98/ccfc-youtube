import json
import pandas as pd
import re
import unicodedata

from html import unescape
from typing import Dict, List, Union

class Processor:
    """Methods to flatten and clean the YouTube comments json responses"""
    EXTRACT_COLS = [
        "snippet.videoId",
        "snippet.channelId",
        "snippet.topLevelComment.id",
        "snippet.topLevelComment.snippet.authorChannelId.value",
        "snippet.topLevelComment.snippet.textDisplay",
        "snippet.topLevelComment.snippet.textOriginal",
        "snippet.topLevelComment.snippet.likeCount",
        "snippet.topLevelComment.snippet.publishedAt",
        "snippet.canReply",
        "snippet.totalReplyCount"
    ]
    RENAME_MAPPING = {
        "snippet.videoId": "video_id",
        "snippet.channelId": "video_channel_id",
        "snippet.topLevelComment.id": "comment_id",
        "snippet.topLevelComment.snippet.authorChannelId.value": "comment_author_id",
        "snippet.topLevelComment.snippet.textDisplay": "text_display",
        "snippet.topLevelComment.snippet.textOriginal": "text_original",
        "snippet.topLevelComment.snippet.likeCount": "like_count",
        "snippet.topLevelComment.snippet.publishedAt": "published_at",
        "snippet.canReply": "can_reply",
        "snippet.totalReplyCount": "total_reply_count",
    }
    
    def clean_html(self, text: str) -> str:
        """Remove html tags and convert character references from an input string"""
        text = re.sub(r"(?i)<br\s*/?>", "\n", text)
        text = re.sub(r"<[^>]+>", "", text)
        return unescape(text)
    
    def normalize_unicode(self, text: str) -> str:
        """Normalize unicode variants in an input string into single consistent form"""
        return unicodedata.normalize("NFKC", text)
    
    def clean_string_input(self, text: str) -> str:
        """Run through comment string cleaning steps"""
        text = self.clean_html(text)
        text = self.normalize_unicode(text)
        text = re.sub(r"\s+", " ", text)
        return text.strip()
    
    def parse_jsonl_string(self, jsonl_string: str) -> List[Dict]:
        """Parse jsonl input string to List of Dicts"""
        parsed = [json.loads(line) for line in jsonl_string.strip().split("\n") if line.strip()]
        return parsed
    
    def flatten_json_to_df(self, input: Union[List, List[Dict]], **kwargs) -> pd.DataFrame:
        """Flatten a json input to a DataFrame"""
        return pd.json_normalize(data=input, **kwargs)
    
    def clean_column(self, column: pd.Series) -> pd.Series:
        """Apply the string cleaning on a column"""
        return column.apply(lambda x: self.clean_string_input(x))
    
    def run_comments_processing(
        self,
        jsonl_string: str,
        comment_column: str = "text_display",
        cleaned_comment_column: str = "text_display_cleaned",
        rename_mapping: Dict = RENAME_MAPPING,
        filter_on: List = EXTRACT_COLS,
    ) -> pd.DataFrame:
        """
        Process raw YouTube comments JSONL string: flatten, filter, rename, clean text column.

        :param jsonl_string: Raw JSONL string input.
        :param comment_column: Original column name containing raw comment text.
        :param cleaned_comment_column: Name for new column with cleaned comment text.
        :param rename_mapping: Dict mapping raw columns to desired flat column names.
        :param filter_on: List of columns to retain (default: EXTRACT_COLS).
        :return: Cleaned and flattened pandas DataFrame.
        """
        comments = self.parse_jsonl_string(jsonl_string)
        df = self.flatten_json_to_df(comments)
        df = df[filter_on]
        df.rename(rename_mapping, axis=1, inplace=True)
        df = df[
            (df[comment_column].notna()) &
            (df[comment_column].str.strip() != "")
        ]
        df[cleaned_comment_column] = self.clean_column(df[comment_column])
        return df
