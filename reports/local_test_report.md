# Local E2E Test Report

Status: **PASS**

- Valid records: **956**
- Invalid records: **44**
- Total records: **1000**
- Invalid percentage: **4.40%**

## Checks

- Kafka running: PASS
- Topic creation: PASS
- Generator publish: PASS
- Processor (Spark(docker)) + write silver: PASS
- Processor (Spark(docker)) + write quarantine: PASS
