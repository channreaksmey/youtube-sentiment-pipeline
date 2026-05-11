FROM eclipse-temurin:11-jdk-alpine

RUN apk add --no-cache bash postgresql-client

# Create minimal Pentaho structure
RUN mkdir -p /opt/pentaho-pdi/lib /pentaho-jobs /pipeline-src

ENV PENTAHO_HOME=/opt/pentaho-pdi
ENV PATH=$PATH:$PENTAHO_HOME

WORKDIR /opt/pentaho-pdi

CMD ["sh", "-c", "echo 'Pentaho PDI container running'; tail -f /dev/null"]
