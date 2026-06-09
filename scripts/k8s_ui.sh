#!/bin/bash
case "$1" in
  start)
    echo "Starting K8s port-forwards..."
    setsid -f sh -c 'kubectl port-forward --address 0.0.0.0 -n nyc-taxi svc/svc-superset 39080:8088 > /dev/null 2>&1'
    setsid -f sh -c 'kubectl port-forward --address 0.0.0.0 -n nyc-taxi svc/svc-minio 39081:9000 > /dev/null 2>&1'
    setsid -f sh -c 'kubectl port-forward --address 0.0.0.0 -n nyc-taxi svc/svc-kafka-ui 39082:8080 > /dev/null 2>&1'
    setsid -f sh -c 'kubectl port-forward --address 0.0.0.0 -n nyc-taxi svc/svc-spark-master 39083:8081 > /dev/null 2>&1'
    setsid -f sh -c 'kubectl port-forward --address 0.0.0.0 -n nyc-taxi svc/svc-trino 39084:8080 > /dev/null 2>&1'
    setsid -f sh -c 'kubectl port-forward --address 0.0.0.0 -n nyc-taxi svc/svc-airflow-webserver 39085:8080 > /dev/null 2>&1'
    setsid -f sh -c 'kubectl port-forward --address 0.0.0.0 -n nyc-taxi svc/svc-minio 39086:9001 > /dev/null 2>&1'
    echo "UIs ready at localhost:39080-39086"
    ;;
  stop)
    echo "Stopping port-forwards..."
    pkill -9 -f "port-forward.*nyc-taxi"
    echo "Port-forwards stopped"
    ;;
esac
