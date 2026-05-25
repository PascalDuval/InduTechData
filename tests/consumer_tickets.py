import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "client_tickets",
    bootstrap_servers="localhost:19092",
    group_id="ticket-group",
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Lecture des tickets...")

for msg in consumer:
    print(msg.value)
