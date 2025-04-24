FROM apache/airflow:2.8.1

# Switch to root to install system dependencies
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3-pip \
    python3-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Switch to airflow user before installing packages
USER airflow

# Install Python packages
RUN pip install --no-cache-dir \
    yfinance \
    apache-airflow-providers-snowflake \
    snowflake-connector-python \
    sentence-transformers \
    pinecone-client \
    dbt-core==1.7.1 \
    dbt-snowflake==1.7.1 \
    dbt-postgres==1.7.1 \
    sqlalchemy \
    pandas \
    psycopg2-binary

# Add .local/bin to PATH
ENV PATH="/home/airflow/.local/bin:${PATH}"
