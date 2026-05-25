import json
from kafka import KafkaConsumer

# consumer_mysql.py est conservé mais le write MySQL est désactivé pour conteneurisation.
# Il imprime uniquement les messages Kafka pour vérification.

consumer = KafkaConsumer(
    "client_tickets",
    bootstrap_servers="localhost:19093",
    group_id="ticket-mysql",
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Consommation (no MySQL) ...")

for msg in consumer:
    t = msg.value
    print("ticket reçu:", t)
