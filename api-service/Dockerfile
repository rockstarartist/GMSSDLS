FROM node:12.6.0-alpine

# Setup variables for default, build, and runtime
ARG kafka_brokers="kafka:9092"
ENV KAFKA_BROKERS=$kafka_brokers

ARG kafka_topic="t.messagebus"
ENV KAFKA_TOPIC=$kafka_topic

ARG kafka_group_id="api_service_consumer_group"
ENV KAFKA_GROUP_ID=$kafka_group_id

RUN pip install kafka-python
