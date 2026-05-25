from kafka import KafkaProducer
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:19092"
)

i = 0
while True:
    msg = f"event {i}"
    producer.send("events", msg.encode())
    print("envoyé:", msg)
    i += 1
    time.sleep(1)

