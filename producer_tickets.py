import json
import os
import random
import time
from datetime import datetime
from kafka import KafkaProducer

bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19093")

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

demandes = [
    "Problème connexion",
    "Erreur paiement",
    "Bug application",
    "Demande remboursement"
]

types = ["technique", "facturation", "compte"]
priorites = ["basse", "moyenne", "haute"]

def generate_ticket(ticket_id):
    return {
        "ticket_id": ticket_id,
        "client_id": random.randint(1000, 9999),
        "created_at": datetime.now().isoformat(),
        "demande": random.choice(demandes),
        "type_demande": random.choice(types),
        "priorite": random.choice(priorites)
    }

ticket_id = 1

while True:
    try:
        ticket = generate_ticket(ticket_id)
        producer.send("client_tickets", ticket)
        print(ticket)
        ticket_id += 1
    except Exception as e:
        print(f"Error sending ticket: {e}")
    time.sleep(1)
