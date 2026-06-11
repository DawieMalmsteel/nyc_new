#!/bin/bash

# Auto-restart port-forward on connection loss.
# Usage: ./scripts/k8s_ui.sh [start|stop]

LIVELINESS=3  # check interval (seconds)
case "$1" in
  start)
    echo "Starting K8s port-forwards..."
    pkill -f "kubectl port-forward.*nyc-taxi" 2>/dev/null; true
    for mapping in "svc/svc-superset:39080:8088" "svc/svc-minio:39081:9000" "svc/svc-kafka-ui:39082:8080" \
                   "svc/svc-spark-master:39083:8081" "svc/svc-trino:39084:8080" \
                   "svc/svc-airflow-webserver:39085:8080" "svc/svc-minio:39086:9001"; do
        svc=$(echo "$mapping" | cut -d: -f1)
        lport=$(echo "$mapping" | cut -d: -f2)
        rport=$(echo "$mapping" | cut -d: -f3)
        setsid -f sh -c "
          while true; do
            kubectl port-forward --address 0.0.0.0 -n nyc-taxi $svc $lport:$rport > /dev/null 2>&1
            sleep $LIVELINESS
          done
        "
    done
    echo "UIs ready at localhost:39080-39086"
    ;;
  stop)
    echo "Stopping port-forwards..."
    pkill -9 -f "port-forward.*nyc-taxi"
    echo "Port-forwards stopped"
    ;;
esac
