FROM apache/spark:3.5.0

USER root

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Upgrade pip first for better dependency resolution
RUN pip install --no-cache-dir --upgrade pip

# Install Torch separately as it is the largest dependency
# This helps with caching and identifying failures
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu

# Install the rest of the dependencies
RUN pip install --no-cache-dir \
    python-dotenv \
    transformers \
    pandas \
    pyarrow \
    sqlalchemy \
    psycopg2-binary

USER spark
