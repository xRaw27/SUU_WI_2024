ls /brokers/ids
ls /brokers/topics
get /controller
get /brokers/ids/0


kafka-topics --create --bootstrap-server kafka-4:9092 --replication-factor 1 --partitions 1 --topic case-one
kafka-topics --create --bootstrap-server kafka-4:9092 --replication-factor 4 --partitions 7 --topic case-two

kafka-topics --bootstrap-server kafka-4:9092 --describe --topic case-one
kafka-topics --bootstrap-server kafka-4:9092 --describe --topic case-two

kafka-producer-perf-test --num-records 100000 --record-size 10000 --throughput 1000 --topic case-one --producer-props bootstrap.servers=kafka-4:9092
kafka-producer-perf-test --num-records 100000 --record-size 10000 --throughput 1000 --topic case-two --producer-props bootstrap.servers=kafka-4:9092