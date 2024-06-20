from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer

serialize = StringSerializer()
p = Producer(
    {
        "bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094",
        "acks": "all",
        "batch.size": 32000,
    }
)


def on_completion(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


data = 1234
p.produce(
    "kafka.usecase.students",
    key=serialize(data.__str__()),
    value=serialize(f"Student data #{data}"),
    callback=on_completion,
)
p.flush()
