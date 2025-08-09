#!/bin/bash

echo -e "\n Setup Complete! Access your apps below:\n"

declare -A ports=(
  ["Airflow"]=8080
  ["MinIO Console"]=9001
  ["Orchestration UI"]=1212
  ["Superset"]=8088
)

for app in "${!ports[@]}"; do
  port="${ports[$app]}"
  if docker ps --format '{{.Ports}}' | grep -q "$port->"; then
    printf "🔗 %-20s -> http://localhost:%s\n" "$app" "$port"
  fi
done

echo ""
