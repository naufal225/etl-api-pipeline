# API ETL Pipeline (Python + PostgreSQL)

Mini Data Engineering project that demonstrates a basic ETL pipeline:

API → Raw Data → Transform → PostgreSQL → Analytics

This project simulates a simple data ingestion pipeline using the public JSONPlaceholder API.

The goal is to practice fundamental data engineering concepts:

- API data ingestion
- Data transformation with pandas
- Loading data into PostgreSQL
- Data validation
- Logging and error handling


---

# Architecture

Pipeline flow:

API
 ↓
Raw JSON files
 ↓
Data transformation (pandas)
 ↓
Load to PostgreSQL
 ↓
Analytics queries


---

# Data Source

Public API:

https://jsonplaceholder.typicode.com

Endpoints used:

- /users
- /posts
- /comments


---

# Project Structure
etl-api-pipeline/
│
├─ README.md
├─ requirements.txt
├─ .env.example
│
├─ data/
│   ├─ raw/
│   └─ processed/
│
├─ logs/
│
├─ sql/
│
└─ src/

---

# Setup

### 1 Install dependencies

pip install -r requirements.txt


### 2 Configure environment variables

Create `.env` file:


PGHOST=localhost
PGPORT=5432
PGDATABASE=etl_practice
PGUSER=postgres
PGPASSWORD=yourpassword


### 3 Run pipeline


python src/pipeline.py


---

# Example Output

Pipeline will generate:


---

# Example Output

Pipeline will generate:


data/raw/users.json
data/raw/posts.json
data/raw/comments.json

data/processed/posts_analytics.csv

logs/etl.log


Data will also be loaded into PostgreSQL table:


analytics_posts


---

# Learning Objectives

This project focuses on learning:

- ETL pipeline fundamentals
- Data ingestion from API
- Data transformation with pandas
- Loading data to relational databases
- Logging and validation