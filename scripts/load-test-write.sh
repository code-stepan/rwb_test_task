#!/bin/bash
# Produce events directly to Kafka for write load test
# Usage: ./scripts/load-test-write.sh

for i in $(seq 1 100000); do
  echo '{"event_id":"'$i'","timestamp":"'$(date -u +%Y-%m-%dT%H:%M:%SZ)'","query_normalized":"iphone","user_id":"u'$i'","session_id":"s'$i'","ip_hash":"h'$i'","device_type":"mobile","platform":"ios","source":"main"}'
done | rpk topic produce search_log --brokers localhost:9092
