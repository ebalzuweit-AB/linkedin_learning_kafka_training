from confluent_kafka import Consumer
from confluent_kafka.serialization import StringDeserializer

deserialize = StringDeserializer()
c = Consumer(
    {
        "bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094",
        "group.id": "kafka-options-consumer",
        "auto.offset.reset": "earliest",
        "fetch.min.bytes": 20,
        "max.partition.fetch.bytes": 2097152,
        "enable.auto.commit": False,
    }
)

c.subscribe(["kafka.usecase.students"])

while True:
    msgs = c.consume(num_messages=10, timeout=0.2)  # fetch.max.wait.ms
    for msg in msgs:
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        print("Received message: {}".format(msg.value().decode("utf-8")))

    if len(msgs) > 0:
        c.commit()
