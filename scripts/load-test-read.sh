#!/bin/bash
# Vegeta load test for read endpoint
# Usage: ./scripts/load-test-read.sh

echo "GET http://localhost:8080/api/v1/trends?limit=10" > /tmp/trends-target.txt
vegeta attack -targets=/tmp/trends-target.txt -rate=100000 -duration=60s | vegeta report
