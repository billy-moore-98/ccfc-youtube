# ccfc-youtube

This project performs sentiment analysis on **YouTube** comments related to **Coventry City Football Club** videos using **GPT-4.1 Nano**, a lightweight large language model variant. The analysis is presented through an interactive Streamlit dashboard.

The repository contains source code for data ingestion, orchestration, and deployment to run the pipeline locally. It harvests data into **AWS S3** and performs asynchronous sentiment inference via **API** calls, enabling efficient processing and **Docker-based deployment** on local environments.

## Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Technologies](#technologies)
- [Setup & Installation](#setup--installation)
- [Source Code](docs/source_code.md)
- [Data Pipeline Details](docs/data_pipeline_details.md)
- [Testing](#testing)
- [Deployment](#deployment)

## Overview

Sentiment analysis is a natural language processing technique that determines the emotional tone of text as positive, neutral, or negative. Common use cases include customer feedback analysis, social media monitoring, and market research.

This project applies sentiment analysis to YouTube comments on Coventry City FC videos to track fan sentiment throughout the football season.

The pipeline integrates the Google YouTube API — which provides access to video metadata, comments, playlists, and analytics — with the OpenRouter LLMs API, a unified interface for large language model prompting.

Data is stored in AWS S3 and queried using AWS Athena, allowing performant, serverless analytical SQL queries that power the Streamlit dashboard’s interactive visualizations. This combination supports automated data extraction, sentiment inference, and trend tracking.

Dagster orchestrates the pipeline execution to ensure reliability and idempotency, while Docker enables containerized deployment for easy setup in both local and cloud environments.

## Architecture

This project implements a **medallion data lakehouse architecture** on AWS, organizing data into layers that progressively improve data quality, structure, and usability.

### Data Layers

#### Bronze Layer
- Stores raw, ingested data fetched from the YouTube API in its original, unmodified format, partitioned by year and month.
- Preserves all source data for traceability, reprocessing, and auditing.
- Contains:
  - Video metadata from a "Coventry City FC" YouTube search
  - Associated comment metadata for these videos

#### Silver Layer
- Cleans and standardizes comments data.

#### Gold Layer
- Enriches data via asynchronous sentiment inference API calls.
- Applies validation checks before storing as Parquet files optimized for analytics.
- Serves as the primary source for scalable analytics and reporting in the Streamlit dashboard.

### Storage and Querying
- **AWS S3** hosts all data layers, providing cost-effective, durable, and highly available storage.
- **AWS Athena** is used for ad-hoc and analytical queries over the Gold layer, enabling serverless, scalable, and performant querying for dashboard visualizations.

### Orchestration and Deployment
- **Dagster** orchestrates the end-to-end workflow, managing task dependencies, scheduling, and automation. See [Data Pipeline Details](docs/data_pipeline_details.md) for more information.
- Custom Python code handles data extraction and asynchronous sentiment inference. Details are in [Source Code](docs/source_code.md).
- **Docker Compose** containerizes the environment, including:
  - Dagster webserver
  - Dagster daemon
  - Log/compute storage service
- Supports deployment locally or on cloud environments for flexible execution.

## Technologies

This project leverages the following technologies:

- **Python** — Core programming language used for data ingestion, transformation, orchestration, and API interaction.  
- **SQL** — Used within AWS Athena for querying and analyzing data stored in the data lakehouse.  
- **YouTube API** — Source of video metadata and comment data related to Coventry City FC.  
- **OpenRouter API** — Provides access to large language models for asynchronous sentiment inference on comments.  
- **AWS S3** — Object storage service used as the data lake for storing raw, cleaned, and enriched datasets.  
- **AWS Athena** — Serverless query engine for running analytical SQL queries over data stored in S3.  
- **Streamlit** — Framework for building the interactive dashboard to visualize sentiment trends and insights.  
- **Dagster** — Orchestration platform managing pipeline scheduling, dependencies, and automation.  
- **Docker** — Containerization tool used to package and deploy the entire pipeline and orchestration stack consistently across environments.

## Setup & Installation

Follow these steps to set up and run the project locally.

### Prerequisites
- Python 3.10+
- Docker and Docker Compose
- AWS credentials with appropriate permissions for S3 (**ccfcyoutube** bucket created) and Athena
- [YouTube](https://developers.google.com/youtube/v3) and [OpenRouter](https://openrouter.ai/) API keys

### Clone the repository

```bash
git clone https://github.com/billy-moore-98/ccfc-youtube.git
cd ccfc-youtube
```

### Configure Environment Variables

Set the following environment variables in a ```.env``` file at the project root.

- ```AWS_ACCESS_KEY_ID```
- ```AWS_SECRET_ACCESS_KEY```
- ```OPENROUTER_API_KEY```
- ```YOUTUBE_API_KEY```

### Build and start Docker containers

```bash
docker compose up --build
```

### Access Dagster UI and Dashboard

Access the UI to trigger and monitor pipelines.

- Dagster UI: [http://localhost:3000](http://localhost:3000)

Once successfully completed for however many partitions you want, run Streamlit:

```bash
streamlit run ./app/app.py
```

## Source Code

## Data Pipeline Details

## Deployment Details

## Testing