FROM nedbank-de-challenge/base:1.0

WORKDIR /app

# Fix Python imports
ENV PYTHONPATH=/app

# Fix Spark environment (CRITICAL)
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH=$SPARK_HOME/bin:$PATH

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pipeline/ pipeline/
COPY config/ config/

CMD ["python", "pipeline/run_all.py"]
