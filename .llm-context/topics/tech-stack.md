# Tech Stack

## Languages & Runtime
- **Python 3.11** - Core application and backend language
- **Jinja2** - Template engine for web frontend

## Backend Frameworks & Web
- **Litestar 2.0+** - Async web framework for API and server endpoints
- **Uvicorn 0.27+** - ASGI server for running Litestar application

## Data & Database
- **PostgreSQL** - Primary relational database (ai_opportunity_index)
- **SQLAlchemy 2.0+** - ORM for database operations and model definitions
- **psycopg2-binary 2.9+** - PostgreSQL database adapter
- **Alembic 1.13+** - Database migration management
- **pandas 2.0+** - Data manipulation and analysis
- **NumPy 1.24+** - Numerical computing
- **PyArrow 14.0+** - Apache Arrow data format (columnar storage, Parquet support)

## APIs & Data Sources
- **yfinance 0.2.31+** - Yahoo Finance stock market data
- **sec-edgar-downloader 5.0+** - SEC Edgar financial filings
- **Beautiful Soup 4 4.12+** - HTML/XML parsing
- **Feedparser 6.0+** - RSS/Atom feed parsing
- **lxml 4.9+** - XML/HTML processing
- **requests 2.31+** - HTTP client library
- **httpx 0.27+** - Modern async HTTP client
- **googlesearch-python 1.2+** - Google search integration
- *(firecrawl-py removed — web enrichment now uses free scrapers: httpx + BeautifulSoup)*

## AI/ML & LLM Integration
- **Anthropic 0.40+** - Claude API client for LLM capabilities
- **Pydantic-AI 0.1+** - Pydantic integration for AI workloads
- **Tiktoken 0.5+** - Token counting and encoding for LLMs
- **LLM Provider**: Claude (claude-sonnet-4-20250514 primary, Gemini 3.0-flash for extraction/estimation)

## Payment & Monetization
- **Stripe 8.0+** - Payment processing and subscription management
- **Resend 2.0+** - Email service for transactional messages

## Build & Deployment
- **Docker** - Containerization (Python 3.11-slim base image)
- **Google Cloud Build** - CI/CD pipeline orchestration
- **Google Cloud Run** - Serverless container deployment (us-central1)
- **Google Artifact Registry** - Container registry for images

## External Services
SEC EDGAR, Yahoo Finance, BLS, GitHub API, USPTO PatentsView, Stripe, Resend

## Key Infrastructure
- Container Port: 8080
- Cloud Run: 2Gi memory, 2 CPU, 0-3 instances
- DB Pooling: SQLAlchemy (5 connections, 10 overflow)
- Rate Limiting: Built-in for SEC, Yahoo Finance, GitHub
