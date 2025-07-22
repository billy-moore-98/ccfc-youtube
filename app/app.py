import altair as alt
import pandas as pd
import streamlit as st

from pyathena import connect

st.set_page_config(layout="wide")

st.title("CCFC YouTube Comments Sentiment Analysis")

# load data from Athena and cache
@st.cache_data
def load_total_comments():
    conn = connect(
        s3_staging_dir="s3://ccfcyoutube/gold/athena/",
        region_name="eu-west-2",
        schema_name="ccfcyoutube"
    )
    query = """
        SELECT
            year,
            month,
            100 * year + month year_month,
            sentiment,
            COUNT(*) total_comments,
            SUM(like_count) total_like_count,
            SUM(total_reply_count) total_reply_count
        FROM comment_sentiments
        WHERE confidence > 0.7
        GROUP BY
            year,
            month,
            sentiment
    """
    df = pd.read_sql_query(query, conn)
    return df

@st.cache_data
def load_avg_likes():
    conn = connect(
        s3_staging_dir="s3://ccfcyoutube/gold/athena/",
        region_name="eu-west-2",
        schema_name="ccfcyoutube"
    )
    query = """
        SELECT
            year,
            month,
            100 * year + month year_month,
            sentiment,
            ROUND(AVG(like_count)) avg_like_count
        FROM comment_sentiments
        WHERE confidence > 0.7
        GROUP BY
            year,
            month,
            sentiment
    """
    df = pd.read_sql_query(query, conn)
    return df

# show some data loading progess
data_load_state = st.text("Loading data...")
total_comments_df = load_total_comments()
avg_likes_df = load_avg_likes()
data_load_state.text("Loading data... done!")

# set a date range select slider
date_range = st.sidebar.select_slider(
    "Select date range",
    options=sorted(total_comments_df["year_month"].unique()),
    value=(total_comments_df["year_month"].min(), total_comments_df["year_month"].max())
)

# filter the dataframe based on the selected date range
filtered_comments_df = total_comments_df[
    (total_comments_df["year_month"] >= date_range[0]) &
    (total_comments_df["year_month"] <= date_range[1])
]
filtered_avg_likes_df = avg_likes_df[
    (avg_likes_df["year_month"] >= date_range[0]) &
    (avg_likes_df["year_month"] <= date_range[1]) &
    (avg_likes_df["sentiment"] != "neutral")
]

# set sentiment color mappings for visuals
sentiment_colors = {
    "positive": "#2ca02c",  # green
    "negative": "#d62728",  # red
    "neutral": "#7f7f7f"    # gray
}

# create charts for positive/negative sentiment analysis
# filter out neutral sentiments
plot_df = filtered_comments_df[filtered_comments_df["sentiment"] != "neutral"]

# create bar chart for total comments by sentiment
total_counts_chart = alt.Chart(plot_df).mark_bar().encode(
    x=alt.X("year_month:O", title=None, axis=alt.Axis(labelAngle=-45)),
    y=alt.Y("total_comments:Q", title=None),
    color=alt.Color("sentiment:N", scale=alt.Scale(domain=list(sentiment_colors.keys()),
                                                   range=list(sentiment_colors.values()))),
    tooltip=["month", "sentiment", "total_comments"]
).properties(
    width=700,
    height=400,
    title="Total Comments by Sentiment over Time"
)

# create bar chart for comments sentiment proportion
total_per_month = plot_df.groupby("year_month")["total_comments"].transform("sum")
plot_df["percentage"] = (plot_df["total_comments"] / total_per_month) * 100
plot_df["percentage"] = plot_df["percentage"].round(2)
proportion_chart = alt.Chart(plot_df).mark_bar().encode(
    x=alt.X("year_month:O", title=None, axis=alt.Axis(labelAngle=-45)),
    y=alt.Y("percentage:Q", title=None),
    color=alt.Color(
        "sentiment:N",
        scale=alt.Scale(
            domain=list(sentiment_colors.keys()),
            range=list(sentiment_colors.values())
        ),
        legend=alt.Legend(title="Sentiment")
    ),
    tooltip=["month", "sentiment", alt.Tooltip("percentage:Q", format=".2f")]
).properties(
    width=700,
    height=400,
    title="Sentiment Proportion over Time"
)

# create pie chart for sentiment distribution
counts = filtered_comments_df.groupby("sentiment")["total_comments"].sum().reset_index()
pie_chart = alt.Chart(counts).mark_arc().encode(
    theta=alt.Theta(field="total_comments", type="quantitative"),
    color=alt.Color(field="sentiment", type="nominal",
                    scale=alt.Scale(domain=list(sentiment_colors.keys()),
                                    range=list(sentiment_colors.values()))),
    tooltip=["sentiment", "total_comments"]
).properties(
    width=400,
    height=400,
    title="Total Sentiment Proportion"
)

# create line chart for avg likes by sentiment
line_chart = alt.Chart(filtered_avg_likes_df).mark_line(point=True).encode(
    x=alt.X("year_month:O", title="Year-Month", axis=alt.Axis(labelAngle=45)),
    y=alt.Y("avg_like_count:Q", title="Average Likes"),
    color=alt.Color(
        "sentiment:N",
        title="Sentiment",
        scale=alt.Scale(
            domain=list(sentiment_colors.keys()),
            range=list(sentiment_colors.values())
        )
    ),
    tooltip=["year_month", "sentiment", alt.Tooltip("like_count:Q", format=".2f")]
).properties(
    title="Average Likes per Sentiment over Time",
    width=700,
    height=400
)

col1, col2 = st.columns(2)
with col1:
    st.altair_chart(total_counts_chart, use_container_width=True)
    st.altair_chart(pie_chart, use_container_width=True)
with col2:
    st.altair_chart(proportion_chart, use_container_width=True)
    st.altair_chart(line_chart, use_container_width=True)
