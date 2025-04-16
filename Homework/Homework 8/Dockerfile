FROM apache/airflow:2.10.1

# Switch to the airflow user
USER airflow

# Install required Python packages
RUN pip install --no-cache-dir \
    yfinance \
    apache-airflow-providers-snowflake \
    snowflake-connector-python \
    sentence-transformers \
    pinecone-client
