FROM eclipse-temurin:11-jdk-jammy

# Install dependencies
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    postgresql-client \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Copy pre-downloaded Pentaho PDI
COPY downloads/pdi-ce-9.4.0.0-343.zip /tmp/pdi.zip

# Extract Pentaho PDI
RUN unzip -q /tmp/pdi.zip -d /opt/ \
    && rm /tmp/pdi.zip \
    && mv /opt/data-integration /opt/pentaho-pdi

# Set environment
ENV PENTAHO_HOME=/opt/pentaho-pdi
ENV PATH=$PATH:$PENTAHO_HOME

# Download PostgreSQL JDBC driver
RUN wget -q "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar" -O $PENTAHO_HOME/lib/postgresql-42.7.3.jar

# Create jobs directory
RUN mkdir -p /pentaho-jobs

WORKDIR /opt/pentaho-pdi

CMD ["tail", "-f", "/dev/null"]