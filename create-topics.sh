cub kafka-ready -b kafka-broker:9094 1 120

kafka-topics --create \
  --if-not-exists \
  --topic topic1 \
  --bootstrap-serverkafka-broker:9094 \
  --partitions 3 \
  --replication-factor 1 \
  --config min.insync.replicas=1
