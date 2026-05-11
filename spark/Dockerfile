FROM apache/spark:3.5.0

USER root

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Ensure spark home is writable and exists
RUN mkdir -p /home/spark && chown -R spark:spark /home/spark

# Set cache directories to writable locations
ENV TRANSFORMERS_CACHE=/tmp/huggingface
ENV HF_HOME=/tmp/huggingface
ENV MPLCONFIGDIR=/tmp/matplotlib

# Upgrade pip
RUN pip install --no-cache-dir --upgrade pip

# Install Torch CPU
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu

# Install other dependencies
RUN pip install --no-cache-dir \
    python-dotenv \
    transformers \
    pandas \
    pyarrow \
    sqlalchemy \
    psycopg2-binary

# Create the cache directory and ensure it's writable
RUN mkdir -p /tmp/huggingface && chown -R spark:spark /tmp/huggingface

USER spark
