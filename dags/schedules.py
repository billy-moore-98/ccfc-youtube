import dagster as dg

from dags.jobs import comments_sentiment_analysis

monthly_schedule = dg.ScheduleDefinition(
    job=comments_sentiment_analysis,
    cron_schedule="0 6 7 * *"
)