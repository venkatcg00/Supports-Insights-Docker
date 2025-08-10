#!/usr/bin/env bash
#
# print_service_urls.sh — Show local URLs for running platform services.
#
# Behavior:
# - Lists fixed services & ports
# - Only prints ones with a running container bound to that port
#
# Usage: invoked by platform_setup.sh

set -Eeuo pipefail
IFS=$'\n\t'

# =============================== Config =============================== #
declare -A PORTS=(
  ["Airflow"]=8080
  ["MinIO Console"]=9001
  ["Orchestration UI"]=1212
  ["Superset"]=8088
)

# =============================== Colors =============================== #
if [[ -t 1 ]]; then
  GREEN='\033[0;32m'; BLUE='\033[1;34m'; NC='\033[0m'
else
  GREEN=''; BLUE=''; NC=''
fi

# =============================== Logging ============================== #
log() { echo -e "${BLUE}[SETUP]${NC} $*"; }
ok()  { echo -e "  ${GREEN}🔗${NC} $*"; }

# =============================== Main ================================= #
log ""
log "Setup Complete! Access your apps below:"
log ""

for app in "${!PORTS[@]}"; do
  port="${PORTS[$app]}"
  if docker ps --format '{{.Ports}}' | grep -q "${port}->"; then
    ok "$(printf "%-20s -> http://localhost:%s" "$app" "$port")"
  fi
done

log ""
