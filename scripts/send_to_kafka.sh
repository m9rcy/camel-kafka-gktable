#!/bin/bash

TOPIC="topic"
BROKER="${BROKER:-localhost:9092}"
KAFKA_HOME="${KAFKA_HOME:-/opt/kafka-tools}"

INPUT_PATH="$1"  # File or directory

# Function to send transformed JSON to Kafka
send_to_kafka() {
  local file="$1"
  echo "Processing $file ..."

  jq -c '.[]
    | "\( {\"devicePositionName\": .devicePosition.name, \"outageBlockName\": .outageBlock.name}):\( {\"devicePosition\": .devicePosition, \"outageBlock\": .outageBlock})"' "$file" \
  | "$KAFKA_HOME/bin/kafka-console-producer.sh" \
      --broker-list "$BROKER" \
      --topic "$TOPIC" \
      --property "parse.key=true" \
      --property "key.separator=:"
}

# Main logic
if [ -f "$INPUT_PATH" ]; then
  send_to_kafka "$INPUT_PATH"
elif [ -d "$INPUT_PATH" ]; then
  for file in "$INPUT_PATH"/*.json; do
    send_to_kafka "$file"
  done
else
  echo "Invalid input path: $INPUT_PATH"
  exit 1
fi
