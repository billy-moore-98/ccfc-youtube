import pandera.pandas as pa

from pandera.dtypes import DateTime

schema = pa.DataFrameSchema({
    "video_id": pa.Column(str),
    "video_channel_id": pa.Column(str),
    "comment_id": pa.Column(str),
    "comment_author_id": pa.Column(str),
    "text_display": pa.Column(str),
    "text_original": pa.Column(str),
    "like_count": pa.Column(int),
    "published_at": pa.Column(DateTime),
    "can_reply": pa.Column(bool),
    "total_reply_count": pa.Column(int),
    "text_display_cleaned": pa.Column(str),
    "sentiment": pa.Column(str),
    "confidence": pa.Column(float)
}, strict=True, coerce=True)