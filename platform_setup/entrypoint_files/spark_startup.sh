#!/bin/bash

# Start Spark Master
/opt/spark/sbin/start-master.sh

# Wait to ensure master has started
sleep 5

# Fetch actual container hostname
HOSTNAME=$(hostname)

/opt/spark/sbin/start-worker.sh spark://$HOSTNAME:7077

# Keep container alive
tail -f /dev/null
