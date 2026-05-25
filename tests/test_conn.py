from kafka import KafkaProducer

try:
    producer = KafkaProducer(bootstrap_servers="localhost:19092")
    print("connexion OK")
except Exception as e:
    print("erreur:", e)
