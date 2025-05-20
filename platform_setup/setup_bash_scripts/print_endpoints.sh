#!/bin/bash

echo -e "\n Setup Complete! Access your apps below:\n"

declare -A ports=(
  ["Airflow"]=8080
  ["MinIO Console"]=9001
  ["Python App"]=1212
  ["Spark UI"]=1234
  ["Superset"]=8088
  ["Trino"]=8082
)

for app in "${!ports[@]}"; do
  port="${ports[$app]}"
  if docker ps --format '{{.Ports}}' | grep -q "$port->"; then
    printf "🔗 %-15s -> http://localhost:%s\n" "$app" "$port"
  fi
done

echo ""
