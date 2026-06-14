# 14. Biểu Đồ Kiến Trúc Skaffold, Helm và Kubernetes

## 14.1 Skaffold Dev Workflow

```mermaid
flowchart TD
    subgraph USER["👤 Developer"]
        CMD["skaffold dev --namespace nyc-taxi"]
    end

    subgraph SKAFFOLD["Skaffold Engine"]
        direction TB
        B["1. Build Artifacts<br/>• nyc-pipeline-tools (Dockerfile)<br/>• nyc-dbt (Dockerfile)<br/>• nyc-airflow (Dockerfile)"]
        PRE["2. Pre-Deploy Hooks<br/>• Xoá immutable jobs<br/>• Sync files → kind-worker PVC"]
        H["3. Helm Deploy<br/>• helm install nyc-taxi chart<br/>• Tạo namespace + resources"]
        POST["4. Post-Deploy Hooks<br/>• Sleep 5s<br/>• In UIs URLs<br/>• Mở browser tabs"]
        PF["5. Port-Forward<br/>• 8 services → localhost:39080-39087<br/>• Skaffold giữ kết nối alive"]
        W["6. Watch Mode<br/>• File changes → sync to PVC<br/>• Dockerfile changes → rebuild + redeploy"]
        
        CMD --> B --> PRE --> H --> POST --> PF --> W
        W -. "loop" .-> B
    end

    subgraph FILESYNC["📁 File Sync Rules"]
        direction LR
        SRC1["airflow/dags/**/*.py"] --> DEST1["/opt/project/airflow/dags/"]
        SRC2["jobs/**/*"] --> DEST2["/opt/project/jobs/"]
        SRC3["scripts/**/*"] --> DEST3["/opt/project/scripts/"]
        SRC4["dbt/**/*"] --> DEST4["/opt/project/dbt/"]
        SRC5["charts/**/*"] --> DEST5["/opt/project/charts/"]
    end

    W --> FILESYNC

    style CMD fill:#4CAF50,color:#fff
    style SKAFFOLD fill:#1565C0,color:#fff
    style FILESYNC fill:#FF8F00,color:#fff
```

---

## 14.2 kind Cluster Topology

```mermaid
graph TB
    subgraph HOST["🖥️ Host Machine (docker)"]
        NP["kind NodePorts<br/>38080-38088, 39000"]
        PF["Skaffold Port-Forwards<br/>39080-39087"]
    end

    subgraph CP["Control-Plane Node"]
        direction TB
        CP1["kube-apiserver"]
        CP2["etcd"]
        CP3["kube-scheduler / controller-manager"]
    end

    subgraph W1["Worker Node 1 (kind-worker)"]
        direction TB
        W1_LBL["Labels: node-type=worker<br/>kubernetes.io/hostname=kind-worker"]
        W1_PV1["PVC: project-files-pv<br/>HostPath: /mnt/nyc-project"]
        W1_PV2["PVC: raw-data-pv<br/>HostPath: /mnt/nyc-data"]
        W1_PODS["Pods:<br/>• file-sync<br/>• minio-setup (Job)<br/>• postgres-init (Job)<br/>• topic-init (Job)<br/>• tất cả services<br/>(nodeSelector: kind-worker)"]
    end

    subgraph W2["Worker Node 2 (kind-worker)"]
        direction TB
        W2_LBL["Labels: node-type=worker"]
        W2_PV1["HostPath mounts available:<br/>/mnt/nyc-project<br/>/mnt/nyc-data"]
        W2_PODS["Pods (nếu cần thêm)"]
    end

    HOST -->|"extraPortMappings"| CP
    HOST -->|"NodePort 38080-38088"| W1
    HOST --> W2

    CP -->|"schedules pods"| W1
    CP -->|"schedules pods"| W2

    W1_PV1 -.->|"hostPath"| HOST
    W1_PV2 -.->|"hostPath"| HOST

    style HOST fill:#E8EAF6,color:#000
    style CP fill:#FFF3E0,color:#000
    style W1 fill:#E3F2FD,color:#000
    style W2 fill:#F3E5F5,color:#000
```

### NodePort Mappings

```mermaid
flowchart LR
    subgraph HOST_PORTS["Host Ports"]
        HP38080["38080"]
        HP38081["38081"]
        HP38082["38082"]
        HP38088["38088"]
        HP39000["39000"]
    end

    subgraph KIND_NODEPORT["kind NodePort (Control-Plane)"]
        NP30080["30080 → Superset"]
        NP30081["30081 → MinIO"]
        NP30082["30082 → Spark"]
        NP30088["30088 → Airflow"]
        NP39000["39000 → Trino"]
    end

    subgraph SKAFFOLD_PF["Skaffold Port-Forward (39080+)"]
        PF80["39080 → svc-superset:8088"]
        PF81["39081 → svc-minio:9000 (API)"]
        PF82["39082 → svc-kafka-ui:8080"]
        PF83["39083 → svc-spark-master:8081"]
        PF84["39084 → svc-trino:8080"]
        PF85["39085 → svc-airflow-webserver:8080"]
        PF86["39086 → svc-minio:9001 (Console)"]
        PF87["39087 → svc-postgres-cdc:5432"]
    end

    HP38080 --> NP30080
    HP38081 --> NP30081
    HP38082 --> NP30082
    HP38088 --> NP30088
    HP39000 --> NP39000

    HP38080 -.->|"alternative"| PF80
    HP38081 -.->|"alternative"| PF81
    HP38082 -.->|"alternative"| PF82
    HP38088 -.->|"alternative"| PF85
```

---

## 14.3 Helm Chart Resource Tree

```mermaid
graph TB
    subgraph CHART["📦 Helm Chart nyc-taxi (0.1.0)"]
        direction TB
        CHART_META["Chart.yaml<br/>apiVersion: v2<br/>type: application<br/>appVersion: 1.0.0"]
        
        subgraph NS["Namespace"]
            N1["namespace.yaml<br/>→ nyc-taxi namespace<br/>Helm managed labels"]
        end

        subgraph STORAGE["Storage Layer"]
            S1["project-files-pv.yaml<br/>PV: 5Gi hostPath<br/>PVC: project-files-pvc<br/>NodeAffinity: kind-worker"]
            S2["raw-data-pv.yaml<br/>PV: hostPath<br/>PVC: raw-data-pvc"]
        end

        subgraph MESSAGING["Messaging Layer"]
            M1["zookeeper/statefulset.yaml"]
            M2["zookeeper/service.yaml<br/>→ svc-zookeeper:2181"]
            M3["kafka/statefulset.yaml<br/>initContainer: wait-zookeeper"]
            M4["kafka/service.yaml<br/>→ svc-kafka:9092"]
            M5["kafka-ui/deployment.yaml"]
            M6["kafka-ui/service.yaml<br/>→ svc-kafka-ui:8080"]
        end

        subgraph STORAGE_SVC["Storage Services"]
            ST1["minio/deployment.yaml<br/>args: server /data --console-address :9001"]
            ST2["minio/service.yaml<br/>→ svc-minio:9000 (API)<br/>→ svc-minio:9001 (Console)"]
            ST3["minio/pvc.yaml<br/>→ minio-data"]
        end

        subgraph PROCESSING["Processing Layer"]
            P1["spark/master-deployment.yaml"]
            P2["spark/master-service.yaml<br/>→ svc-spark-master:7077,8081"]
            P3["spark/worker-deployment.yaml"]
            P4["spark/worker-service.yaml<br/>→ svc-spark-worker:8082"]
        end

        subgraph CDC["CDC Layer"]
            C1["postgres-cdc/statefulset.yaml<br/>wal_level=logical"]
            C2["postgres-cdc/service.yaml<br/>→ svc-postgres-cdc:5432"]
            C3["postgres-cdc/pvc.yaml"]
            C4["debezium/deployment.yaml<br/>Kafka Connect 2.5"]
            C5["debezium/service.yaml<br/>→ svc-debezium:8083"]
        end

        subgraph SQL["SQL & Analytics Layer"]
            Q1["trino/configmap.yaml<br/>hive.properties + config.properties"]
            Q2["trino/deployment.yaml<br/>Trino 435"]
            Q3["trino/service.yaml<br/>→ svc-trino:8080"]
            Q4["superset/configmap.yaml<br/>superset_config.py"]
            Q5["superset/deployment.yaml<br/>Superset 4.0.0"]
            Q6["superset/service.yaml<br/>→ svc-superset:8088"]
        end

        subgraph AIRFLOW["Orchestration Layer"]
            A1["airflow/postgres/statefulset.yaml<br/>Airflow metadata DB"]
            A2["airflow/postgres/service.yaml"]
            A3["airflow/postgres/pvc.yaml"]
            A4["airflow/rbac.yaml<br/>ServiceAccount: airflow-sa<br/>Role + RoleBinding"]
            A5["airflow/init-job.yaml<br/>db migrate + create admin"]
            A6["airflow/file-sync.yaml<br/>sleep infinity pod for skaffold sync"]
            A7["airflow/scheduler/deployment.yaml"]
            A8["airflow/webserver/deployment.yaml<br/>→ svc-airflow-webserver:8080"]
            A9["airflow/webserver/service.yaml"]
        end

        subgraph JOBS["One-Shot Jobs"]
            J1["jobs/minio-setup.yaml<br/>→ mc alias + buckets + upload"]
            J2["jobs/postgres-init.yaml<br/>→ psycopg2 create trips table"]
            J3["jobs/topic-init.yaml<br/>→ wait-kafka + create topics"]
        end

        subgraph DBT["(dbt runs via Airflow KPO)"]
            D1["dbt/ (empty templates)"]
        end
    end

    style CHART fill:#1A237E,color:#fff
    style NS fill:#E8EAF6,color:#000
    style STORAGE fill:#E3F2FD,color:#000
    style MESSAGING fill:#F3E5F5,color:#000
    style STORAGE_SVC fill:#E0F2F1,color:#000
    style PROCESSING fill:#FFF3E0,color:#000
    style CDC fill:#FCE4EC,color:#000
    style SQL fill:#E8F5E9,color:#000
    style AIRFLOW fill:#FFF8E1,color:#000
    style JOBS fill:#F3E5F5,color:#000
    style DBT fill:#ECEFF1,color:#000
```

---

## 14.4 PVC File-Sync Data Flow

```mermaid
flowchart TD
    subgraph DEV_MACHINE["💻 Developer Machine"]
        direction TB
        LOCAL_FILES["Local Project Files<br/>• airflow/dags/<br/>• jobs/<br/>• scripts/<br/>• dbt/<br/>• charts/"]
        SKAFFOLD_WATCH["Skaffold Watch<br/>detects file changes"]
        TAR_CMD["tar cf - | docker exec -i kind-worker ..."]
    end

    subgraph PRE_HOOK["Skaffold Pre-Deploy Hook"]
        direction TB
        HOOK_SYNC["bash -c:<br/>docker exec kind-worker mkdir -p /mnt/nyc-project<br/>tar cf - ... | docker exec -i kind-worker tar xf - -C /mnt/nyc-project"]
        HOOK_DELETE["kubectl delete job -n nyc-taxi --all<br/>Xoá immutable jobs trước Helm deploy"]
    end

    subgraph KIND_WORKER["kind-worker Node"]
        direction TB
        HOSTPATH["/mnt/nyc-project/ (HostPath)"]
        PVC["project-files-pvc<br/>(PersistentVolumeClaim)"]
        SYNC_POD["file-sync Pod<br/>image: nyc-pipeline-tools:k8s<br/>command: sleep infinity<br/>mount: /opt/project"]
        OTHER_PODS["Tất cả Pods dùng PVC:<br/>• Airflow (scheduler, webserver)<br/>• Spark (master, worker)<br/>• Trino<br/>• Superset<br/>• dbt<br/>• CDC jobs<br/>mount: /opt/project"]
    end

    subgraph SKAFFOLD_SYNC["🔄 Skaffold Sync (Hot-Reload)"]
        direction LR
        SYNC_RULES["Sync Rules:<br/>airflow/dags/**/*.py → /opt/project/airflow/dags/<br/>jobs/**/* → /opt/project/jobs/<br/>scripts/**/* → /opt/project/scripts/<br/>dbt/**/* → /opt/project/dbt/<br/>charts/**/* → /opt/project/charts/"]
        SKAFFOLD_PUSH["Skaffold pushes changed files<br/>directly to file-sync pod"]
    end

    LOCAL_FILES -->|"Lần đầu (pre-deploy hook)"| TAR_CMD
    TAR_CMD -->|"docker exec"| HOOK_SYNC
    HOOK_SYNC -->|"ghi vào"| HOSTPATH
    HOSTPATH -->|"hostPath mount"| PVC
    PVC -->|"mounted at /opt/project"| SYNC_POD
    PVC -->|"mounted at /opt/project"| OTHER_PODS

    LOCAL_FILES -->|"Thay đổi file"| SKAFFOLD_WATCH
    SKAFFOLD_WATCH -->|"kích hoạt"| SKAFFOLD_SYNC
    SKAFFOLD_SYNC -->|"push file đến"| SYNC_POD
    SYNC_POD -->|"ghi vào"| PVC
    PVC -->|"tất cả pods thấy<br/>thay đổi ngay lập tức"| OTHER_PODS

    style DEV_MACHINE fill:#E8EAF6,color:#000
    style PRE_HOOK fill:#FFF3E0,color:#000
    style KIND_WORKER fill:#E3F2FD,color:#000
    style SKAFFOLD_SYNC fill:#F3E5F5,color:#000
    style SYNC_POD fill:#4CAF50,color:#fff
    style PVC fill:#FF8F00,color:#fff
```

---

## 14.5 Skaffold Deploy Hook Flow (Chi tiết)

```mermaid
sequenceDiagram
    participant DEV as Developer
    participant SK as Skaffold
    participant DOCK as Docker
    participant K8S as Kubernetes (kind)
    participant PVC as kind-worker PVC
    
    DEV->>SK: skaffold dev --namespace nyc-taxi
    
    Note over SK: === BUILD PHASE ===
    SK->>DOCK: Build nyc-pipeline-tools:k8s
    SK->>DOCK: Build nyc-dbt:k8s
    SK->>DOCK: Build nyc-airflow:k8s
    DOCK-->>SK: Images built
    
    Note over SK: === PRE-DEPLOY HOOK ===
    SK->>K8S: kubectl delete job -n nyc-taxi --all
    K8S-->>SK: Jobs deleted
    
    SK->>K8S: docker exec kind-worker mkdir -p /mnt/nyc-project
    SK->>K8S: tar cf - | docker exec -i kind-worker tar xf - -C /mnt/nyc-project
    Note right of K8S: Excludes: dbt/logs, dbt/target,<br/>.git, __pycache__, *.pyc
    K8S-->>SK: Files synced to PVC
    
    Note over SK: === HELM DEPLOY ===
    SK->>K8S: helm install nyc-taxi charts/nyc-taxi/ --namespace nyc-taxi --create-namespace
    K8S-->>SK: All 30+ resources created
    
    Note over SK: === POST-DEPLOY HOOK ===
    SK->>SK: sleep 5
    SK->>DEV: 🚀 PIPELINE UIs
    SK->>DEV: Superset http://localhost:39080
    SK->>DEV: MinIO API http://localhost:39081
    SK->>DEV: Kafka UI http://localhost:39082
    SK->>DEV: Spark http://localhost:39083
    SK->>DEV: Trino http://localhost:39084
    SK->>DEV: Airflow http://localhost:39085
    SK->>DEV: MinIO Admin http://localhost:39086
    SK->>DEV: Postgres CDC localhost:39087
    
    SK->>DEV: xdg-open http://localhost:39085 (Airflow)
    SK->>DEV: xdg-open http://localhost:39080 (Superset)
    SK->>DEV: xdg-open http://localhost:39082 (Kafka UI)
    
    Note over SK: === PORT-FORWARD ===
    SK->>K8S: port-forward svc-superset 39080:8088
    SK->>K8S: port-forward svc-minio 39081:9000 & 39086:9001
    SK->>K8S: port-forward svc-kafka-ui 39082:8080
    SK->>K8S: port-forward svc-spark-master 39083:8081
    SK->>K8S: port-forward svc-trino 39084:8080
    SK->>K8S: port-forward svc-airflow-webserver 39085:8080
    SK->>K8S: port-forward svc-postgres-cdc 39087:5432
    
    Note over SK,DEV: === WATCH MODE ===
    loop File changes detected
        DEV->>DEV: Edit file in airflow/dags/
        SK->>PVC: Sync changed file to file-sync pod
        PVC-->>K8S: All pods see new file instantly
    end
    
    loop Dockerfile changes
        DEV->>DEV: Edit docker/tools.Dockerfile
        SK->>DOCK: Rebuild nyc-pipeline-tools
        SK->>K8S: Re-deploy chart
    end
```

---

## 14.6 Service Topology & Dependencies

```mermaid
graph TB
    subgraph EXTERNAL["🌐 External Access (Port-Forward 39080-39087)"]
        SUP_UI["Superset UI<br/>:39080"]
        MINIO_UI["MinIO Console<br/>:39086"]
        KAFKA_UI["Kafka UI<br/>:39082"]
        SPARK_UI["Spark Master<br/>:39083"]
        TRINO_UI["Trino<br/>:39084"]
        AIR_UI["Airflow UI<br/>:39085"]
    end

    subgraph K8S_SERVICES["Kubernetes Services (ClusterIP)"]
        direction TB
        SVC_ZK["svc-zookeeper:2181"]
        SVC_KAFKA["svc-kafka:9092"]
        SVC_KAFKA_UI["svc-kafka-ui:8080"]
        SVC_MINIO["svc-minio:9000 / 9001"]
        SVC_SPARK_M["svc-spark-master:7077 / 8081"]
        SVC_SPARK_W["svc-spark-worker:8082"]
        SVC_TRINO["svc-trino:8080"]
        SVC_SUP["svc-superset:8088"]
        SVC_AIR_WEB["svc-airflow-webserver:8080"]
        SVC_PG_CDC["svc-postgres-cdc:5432"]
        SVC_DEBEZIUM["svc-debezium:8083"]
    end

    subgraph PODS["Pods (Containers)"]
        ZK["Zookeeper<br/>confluentinc/cp-zookeeper:7.6.1"]
        KAFKA["Kafka Broker<br/>confluentinc/cp-kafka:7.6.1<br/>initContainer: wait-zookeeper"]
        KAFKA_UI_POD["Kafka UI<br/>provectuslabs/kafka-ui:latest"]
        MINIO["MinIO S3<br/>minio/minio:latest<br/>server /data --console-address :9001"]
        SPARK_M["Spark Master<br/>apache/spark:3.5.1<br/>spark-class Master"]
        SPARK_W["Spark Worker<br/>apache/spark:3.5.1<br/>spark-class Worker"]
        TRINO["Trino Coordinator<br/>trinodb/trino:435"]
        SUP["Superset<br/>apache/superset:4.0.0"]
        AIR_PG["Airflow Postgres<br/>postgres:16-alpine"]
        AIR_SCH["Airflow Scheduler<br/>nyc-airflow:k8s"]
        AIR_WEB["Airflow Webserver<br/>nyc-airflow:k8s"]
        PG_CDC["Postgres CDC<br/>postgres:16-alpine<br/>wal_level=logical"]
        DEBEZIUM["Debezium Connect<br/>debezium/connect:2.5"]
        FILE_SYNC["file-sync<br/>nyc-pipeline-tools:k8s<br/>sleep infinity"]
    end

    subgraph JOBS_PODS["Jobs (One-Shot)"]
        MINIO_SETUP["minio-setup<br/>minio/mc:latest<br/>Create buckets + upload"]
        PG_INIT["postgres-init<br/>nyc-pipeline-tools:k8s<br/>psycopg2 create table"]
        TOPIC_INIT["topic-init<br/>nyc-pipeline-tools:k8s<br/>wait-kafka + create topics"]
    end

    %% Dependencies
    KAFKA -->|"depends on"| SVC_ZK
    KAFKA_UI_POD -->|"connects to"| SVC_KAFKA
    SPARK_W -->|"registers with"| SVC_SPARK_M
    
    TRINO -->|"reads from"| SVC_MINIO
    SUP -->|"queries"| SVC_TRINO
    AIR_SCH -->|"schedules pods on"| SVC_KAFKA
    AIR_SCH --> SVC_TRINO
    AIR_SCH --> SVC_MINIO
    
    DEBEZIUM -->|"connects to"| SVC_KAFKA
    DEBEZIUM -->|"captures from"| SVC_PG_CDC
    
    MINIO_SETUP --> SVC_MINIO
    PG_INIT --> SVC_PG_CDC
    TOPIC_INIT --> SVC_KAFKA

    %% UI connections
    SUP_UI --> SVC_SUP
    MINIO_UI --> SVC_MINIO
    KAFKA_UI --> SVC_KAFKA_UI
    SPARK_UI --> SVC_SPARK_M
    TRINO_UI --> SVC_TRINO
    AIR_UI --> SVC_AIR_WEB

    style EXTERNAL fill:#E8EAF6,color:#000
    style K8S_SERVICES fill:#FFF3E0,color:#000
    style PODS fill:#E3F2FD,color:#000
    style JOBS_PODS fill:#F3E5F5,color:#000
```

---

## 14.7 PVC Mounts và Node Affinity

```mermaid
graph TB
    subgraph KIND_CLUSTER["kind Cluster"]
        subgraph CP["Control-Plane"]
            CP_NODE["kind-control-plane"]
        end

        subgraph W1["Worker 1: kind-worker"]
            W1_AFFINITY["Node Labels:<br/>kubernetes.io/hostname=kind-worker<br/>node-type=worker"]
            W1_MOUNT1["HostPath Mount:<br/>Host: /home/.../nyc_new<br/>→ Container: /mnt/nyc-project"]
            W1_MOUNT2["HostPath Mount:<br/>Host: /home/.../nyc_new/data<br/>→ Container: /mnt/nyc-data"]

            subgraph W1_PV["PersistentVolumes (NodeAffinity: kind-worker)"]
                PV1["project-files-pv<br/>5Gi, hostPath /mnt/nyc-project<br/>ReadWriteOnce"]
                PV2["raw-data-pv<br/>hostPath /mnt/nyc-data<br/>ReadWriteOnce"]
            end

            subgraph W1_PVC["PersistentVolumeClaims"]
                PVC1["project-files-pvc<br/>Requests: 5Gi<br/>StorageClass: ''"]
                PVC2["raw-data-pvc"]
                PVC3["minio-data"]
                PVC4["postgres-cdc-data"]
                PVC5["airflow-postgres-data"]
            end

            subgraph W1_PODS["Pods on kind-worker"]
                FS["file-sync<br/>mount: /opt/project → PVC1"]
                MS["minio-setup (Job)<br/>mount: /data → PVC2"]
                PI["postgres-init (Job)<br/>mount: /opt/project → PVC1"]
                TI["topic-init (Job)<br/>mount: /opt/project → PVC1"]
                MINIO_POD["minio<br/>mount: /data → PVC3"]
                TRINO_POD["trino<br/>mount: /opt/project → PVC1"]
                SUP_POD["superset<br/>mount: /opt/project → PVC1"]
                SPARK_M_POD["spark-master<br/>mount: /opt/project → PVC1"]
                SPARK_W_POD["spark-worker<br/>mount: /opt/project → PVC1"]
                AIR_PODS["airflow-*<br/>mount: /opt/project → PVC1"]
                PG_POD["postgres-cdc<br/>mount: /var/lib/... → PVC4"]
                AIR_PG_POD["airflow-postgres<br/>mount: /var/lib/... → PVC5"]
            end
        end

        subgraph W2["Worker 2: kind-worker2"]
            W2_MOUNT1["HostPath Mount:<br/>Host: /home/.../nyc_new<br/>→ Container: /mnt/nyc-project"]
            W2_MOUNT2["HostPath Mount:<br/>Host: /home/.../nyc_new/data<br/>→ Container: /mnt/nyc-data"]
            W2_AFFINITY["Node Labels:<br/>node-type=worker"]
            W2_SPARE["(Spare capacity -<br/>scheduling only)"]
        end
    end

    PV1 -->|"bound to"| PVC1
    PV2 -->|"bound to"| PVC2
    
    PVC1 -->|"mounted at /opt/project"| FS
    PVC1 --> PI
    PVC1 --> TI
    PVC1 --> TRINO_POD
    PVC1 --> SUP_POD
    PVC1 --> SPARK_M_POD
    PVC1 --> SPARK_W_POD
    PVC1 --> AIR_PODS
    
    PVC2 -->|"mounted at /data"| MS
    
    PVC3 -->|"mounted at /data"| MINIO_POD
    PVC4 -->|"mounted at /var/lib/postgresql/data"| PG_POD
    PVC5 -->|"mounted at /var/lib/postgresql/data"| AIR_PG_POD

    style KIND_CLUSTER fill:#1A237E,color:#fff
    style CP fill:#E8EAF6,color:#000
    style W1 fill:#E3F2FD,color:#000
    style W2 fill:#F3E5F5,color:#000
    style W1_PV fill:#FF8F00,color:#fff
    style W1_PVC fill:#4CAF50,color:#fff
    style W1_PODS fill:#E0F2F1,color:#000
```

---

## 14.8 Port-Forward Mapping

```mermaid
flowchart LR
    subgraph BROWSER["🌐 Browser"]
        SUP["http://localhost:39080<br/>→ Superset (admin/admin)"]
        MINIO_C["http://localhost:39086<br/>→ MinIO Console (minio/minio123)"]
        KAFKA_U["http://localhost:39082<br/>→ Kafka UI"]
        SPARK_U["http://localhost:39083<br/>→ Spark Master"]
        TRINO_U["http://localhost:39084<br/>→ Trino"]
        AIR_U["http://localhost:39085<br/>→ Airflow (admin/admin)"]
    end

    subgraph SKAFFOLD_PF["Skaffold Port-Forward Manager"]
        PF_SUP["svc-superset<br/>8088 → 39080"]
        PF_MINIO_A["svc-minio<br/>9000 → 39081"]
        PF_MINIO_C["svc-minio<br/>9001 → 39086"]
        PF_KAFKA["svc-kafka-ui<br/>8080 → 39082"]
        PF_SPARK["svc-spark-master<br/>8081 → 39083"]
        PF_TRINO["svc-trino<br/>8080 → 39084"]
        PF_AIR["svc-airflow-webserver<br/>8080 → 39085"]
        PF_PG["svc-postgres-cdc<br/>5432 → 39087"]
    end

    subgraph K8S_SERVICES["Kubernetes Services (ClusterIP : namespace nyc-taxi)"]
        SVC_SUP["svc-superset<br/>ClusterIP :8088"]
        SVC_MINIO["svc-minio<br/>ClusterIP :9000, :9001"]
        SVC_KAFKA_UI["svc-kafka-ui<br/>ClusterIP :8080"]
        SVC_SPARK["svc-spark-master<br/>ClusterIP :8081"]
        SVC_TRINO["svc-trino<br/>ClusterIP :8080"]
        SVC_AIR["svc-airflow-webserver<br/>ClusterIP :8080"]
        SVC_PG["svc-postgres-cdc<br/>ClusterIP :5432"]
    end

    SUP --> PF_SUP
    MINIO_C --> PF_MINIO_C
    MINIO_A["MinIO S3 API<br/>http://localhost:39081"] --> PF_MINIO_A
    KAFKA_U --> PF_KAFKA
    SPARK_U --> PF_SPARK
    TRINO_U --> PF_TRINO
    AIR_U --> PF_AIR
    PG_CLI["psql -h localhost -p 39087"] --> PF_PG

    PF_SUP --> SVC_SUP
    PF_MINIO_A --> SVC_MINIO
    PF_MINIO_C --> SVC_MINIO
    PF_KAFKA --> SVC_KAFKA_UI
    PF_SPARK --> SVC_SPARK
    PF_TRINO --> SVC_TRINO
    PF_AIR --> SVC_AIR
    PF_PG --> SVC_PG

    style BROWSER fill:#E8EAF6,color:#000
    style SKAFFOLD_PF fill:#1565C0,color:#fff
    style K8S_SERVICES fill:#FFF3E0,color:#000
```

---

## 14.9 Docker Compose vs Skaffold Deployment Comparison

```mermaid
graph TB
    subgraph COMPOSE["🐳 Docker Compose Mode"]
        direction TB
        COMPOSE_CMD["make infra-up"]
        COMPOSE_BUILD["docker compose build<br/>(implicit)"]
        COMPOSE_START["docker compose up -d<br/>16 services, 6 profiles"]
        COMPOSE_VOL["Docker Volumes<br/>• minio_data<br/>• airflow_postgres_data<br/>• postgres_cdc_data"]
        COMPOSE_BIND["Bind Mount<br/>./ → /opt/project<br/>(hot-reload native)"]
        COMPOSE_NET["Docker Network<br/>nyc_new_default<br/>Service DNS: kafka, minio..."]
        
        COMPOSE_CMD --> COMPOSE_BUILD --> COMPOSE_START
        COMPOSE_START --> COMPOSE_VOL
        COMPOSE_START --> COMPOSE_BIND
        COMPOSE_START --> COMPOSE_NET
    end

    subgraph SKAFFOLD["🚀 Skaffold (Kubernetes) Mode"]
        direction TB
        SKAFFOLD_CMD["skaffold dev --namespace nyc-taxi"]
        
        subgraph BUILD["Build (local push: false)"]
            B1["Build nyc-pipeline-tools<br/>docker/tools.Dockerfile"]
            B2["Build nyc-dbt<br/>docker/dbt.Dockerfile"]
            B3["Build nyc-airflow<br/>docker/airflow.Dockerfile"]
        end
        
        subgraph PRE_HOOK["Pre-Deploy Hooks (host)"]
            H1["kubectl delete job -n nyc-taxi --all"]
            H2["tar + docker exec → kind-worker PVC<br/>Sync: dags/ jobs/ scripts/ dbt/ charts/"]
        end
        
        subgraph HELM["Helm Deploy"]
            DPL["helm install nyc-taxi charts/nyc-taxi/<br/>namespace: nyc-taxi<br/>createNamespace: true"]
            DPL_TMPL["30+ templates rendered:<br/>• Deployments, StatefulSets<br/>• Services, ConfigMaps<br/>• PVCs, PVs, Jobs<br/>• RBAC"]
        end
        
        subgraph POST_HOOK["Post-Deploy Hooks"]
            PO1["Print URLs + credentials"]
            PO2["xdg-open browsers"]
        end
        
        subgraph PF["Port-Forward (auto)"]
            PF1["8 port-forwards<br/>39080-39087"]
        end
        
        subgraph SYNC["Sync Rules (Watch)"]
            S1["Airflow DAGs<br/>Jobs, Scripts<br/>dbt, Charts"]
            S2["→ file-sync pod<br/>→ PVC<br/>→ tất cả pods"]
        end
        
        SKAFFOLD_CMD --> BUILD --> PRE_HOOK --> HELM --> POST_HOOK --> PF --> SYNC
        SYNC -.->|"file changes<br/>loop"| BUILD
    end

    subgraph COMPARE["Comparison"]
        C1["🐳 Compose: Simple, fast, less features"]
        C2["🚀 Skaffold: Production-like, auto-sync, auto-rebuild"]
    end

    style COMPOSE fill:#E8F5E9,color:#000
    style SKAFFOLD fill:#E3F2FD,color:#000
    style COMPARE fill:#FFF8E1,color:#000
```

---

## 14.10 kind Cluster Creation Flow

```mermaid
flowchart TD
    START("make k8s-up / kind create cluster") --> CHECK{Cluster exists?}
    CHECK -->|"No"| CREATE["kind create cluster --config kind.yaml<br/>• 3 nodes: 1 CP + 2 workers<br/>• extraPortMappings: 38080-38088, 39000<br/>• extraMounts on workers"]
    CHECK -->|"Yes"| BUILD_IMAGES
    
    CREATE --> BUILD_IMAGES["make k8s-images<br/>• docker build 3 images<br/>• kind load docker-image"]
    BUILD_IMAGES --> DEPLOY["make k8s-deploy<br/>• kubectl apply ordered manifests<br/>• namespace → storage → services → jobs"]
    DEPLOY --> WAIT["kubectl wait --for=condition=ready pod --all -n nyc-taxi<br/>Timeout: 300s"]
    WAIT --> PORT["make k8s-ui / skaffold port-forward<br/>• 39080-39087<br/>• setsid -f auto-restart"]
    PORT --> DONE["✅ All services running"]

    CREATE --> DETAILS["kind Config Details"]
    
    subgraph DETAILS["kind.yaml Config"]
        CP["Control-Plane Node<br/>• NodePort: 38080→30080 (Superset)<br/>• NodePort: 38081→30081 (MinIO)<br/>• NodePort: 38082→30082 (Spark)<br/>• NodePort: 38088→30088 (Airflow)<br/>• NodePort: 39000→39000 (Trino)"]
        
        W1["Worker 1 (kind-worker)<br/>• hostPath: /mnt/nyc-project<br/>• hostPath: /mnt/nyc-data<br/>• Label: node-type=worker"]
        
        W2["Worker 2 (kind-worker2)<br/>• hostPath: /mnt/nyc-project<br/>• hostPath: /mnt/nyc-data<br/>• Label: node-type=worker"]
    end

    style START fill:#4CAF50,color:#fff
    style CREATE fill:#FF8F00,color:#fff
    style BUILD_IMAGES fill:#1565C0,color:#fff
    style DEPLOY fill:#7B1FA2,color:#fff
    style WAIT fill:#C62828,color:#fff
    style PORT fill:#00838F,color:#fff
    style DONE fill:#2E7D32,color:#fff
    style DETAILS fill:#E8EAF6,color:#000
```
