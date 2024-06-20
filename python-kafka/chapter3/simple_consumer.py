from confluent_kafka import Consumer

c = Consumer(
    {
        "bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094",
        "group.id": "kafka-python-consumer",
        "auto.offset.reset": "earliest",
    }
)

c.subscribe(["kafka.learning.orders"])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print("Received message: {}".format(msg.value().decode("utf-8")))

c.close()
