# Python Source Code

## Overview

Source code for data extraction, cleaning and inference can be found at ```/ccfc_yt```. It is split into several modules as below.

```
ccfc_yt/
├── __init__.py
├── client.py       - YouTube API client
├── exceptions.py   - Custom exceptions to raise
├── infer.py        - OpenRouter API asynchronous client
└── processor.py    - Data cleaner and processor
```

## YouTube API Client

The `YoutubeClient` class provides a custom, lightweight interface to interact with the YouTube Data API using the `requests` library, offering fine-grained control and enhanced testability compared to standard Google developed SDKs.

**Key Features:**

- **API Key Management:** Validates API key upon initialization.  
- **Robust HTTP Session:** Implements retries with exponential backoff for handling rate limits and server errors.  
- **Error Handling:** Detects and raises custom exceptions for quota limits and disabled comments.  
- **Pagination Support:**  
  - Retrieves all paginated results as a combined list.  
  - Streams paginated results using generators for efficient processing of large datasets.  
- **Core Methods:**  
  - `get_videos_search`: Searches for YouTube videos based on query parameters.  
  - `get_comments_thread`: Fetches comment threads for a given video ID.  
- **Flexible Parameters:** Both core methods support optional parameters, pagination, and streaming modes.  
- **Integration:** Uses Dagster’s logger for monitoring and integrates with custom exceptions for clear error signaling.

This module is designed for scalable, reliable data ingestion from YouTube, forming a foundational component of the data pipeline.

## Processor

The `Processor` class provides methods to clean, flatten, and transform raw YouTube comments JSON data into a structured pandas DataFrame ready for analysis.

**Key functionalities include:**

- **Flattening nested JSON:** Converts complex YouTube API comment responses into flat tables using pandas' `json_normalize`.  
- **Selective column extraction:** Filters and renames relevant fields such as video ID, comment ID, author ID, and comment text for clarity and consistency.  
- **Text cleaning:**  
  - Removes HTML tags and converts HTML entities to plain text.  
  - Normalizes Unicode characters to a consistent form.  
  - Strips extra whitespace and replaces HTML line breaks with newlines.  
- **JSONL parsing:** Handles multiline JSON (JSONL) string inputs to produce lists of dictionaries.  
- **Pipeline method (`run_comments_processing`):**  
  - Parses raw JSONL comments string, flattens them, filters and renames columns, and applies text cleaning to the comment text column.  
  - Returns a cleaned and ready-to-use DataFrame for downstream processing or sentiment analysis.

This module forms a crucial step in the pipeline by ensuring input comment data is clean, uniform, and structured for efficient analytics.

### OpenRouter Async Client

The `OpenRouterAsyncClient` class enables asynchronous interaction with the OpenRouter API to perform large language model (LLM) inference, such as sentiment analysis, on batches of comments.

**Key features include:**

- **Async HTTP requests:** Uses `aiohttp` to efficiently send multiple concurrent POST requests to the OpenRouter API, maximizing throughput and reducing latency.  
- **Concurrency control:** Manages the maximum number of simultaneous requests via an asyncio semaphore.  
- **Robust retry logic:** Implements exponential backoff with the `backoff` library for handling transient network errors and rate limiting (HTTP 429), with detailed logging during retries.  
- **Batch inference:** Accepts batches of comment messages with associated IDs, sending inference requests in parallel.  
- **Error handling:** Captures and logs exceptions per comment, returning structured results including successes and failures for downstream processing or retries.  
- **Integration:** Uses Dagster’s logger for consistent pipeline logging and observability.

This module is critical for scalable, reliable asynchronous calls to LLM inference endpoints within the data pipeline.