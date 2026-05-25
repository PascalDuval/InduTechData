import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# connexion Redpanda
producer = KafkaProducer(
    bootstrap_servers="localhost:19092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

demandes = [
    "Problème de connexion",
    "Erreur de paiement",
    "Demande de remboursement",
    "Bug application",
    "Changement mot de passe",
    "Question facturation"
]

types = ["technique", "facturation", "compte"]
priorites = ["basse", "moyenne", "haute", "critique"]

def generate_ticket(ticket_id):
    return {
        "ticket_id": ticket_id,
        "client_id": random.randint(1000, 9999),
        "created_at": datetime.now().isoformat(),
        "demande": random.choice(demandes),
        "type_demande": random.choice(types),
        "priorite": random.choice(priorites)
    }

print("Envoi de tickets...")

ticket_id = 1

while True:
    ticket = generate_ticket(ticket_id)
    producer.send("client_tickets", ticket)
    print("envoyé:", ticket)

    ticket_id += 1
    time.sleep(1)
