from kafka import KafkaProducer

try:
    producer = KafkaProducer(bootstrap_servers="localhost:19093")
    print("connexion Redpanda OK")
except Exception as e:
    print("erreur:", e)
