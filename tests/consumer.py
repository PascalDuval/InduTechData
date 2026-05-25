from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "events",
    bootstrap_servers="localhost:19092",
    group_id="python-group",
    auto_offset_reset="earliest"
)

for msg in consumer:
    print("reçu:", msg.value.decode())
