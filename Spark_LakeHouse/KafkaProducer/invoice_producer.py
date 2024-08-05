from confluent_kafka import Producer
import json
import time


class InvoiceProducer:
    def __init__(self):
        self.topic = "invoices"
        self.conf = {
            'bootstrap.servers': 'localhost:9092',
            'client.id': 'local-producer'
        }

    def delivery_callback(self, err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            key = msg.key().decode('utf-8')
            invoice_id = json.loads(msg.value().decode('utf-8'))["InvoiceNumber"]
            print(f"Produced event to : key = {key} value = {invoice_id}")

    def produce_invoices(self, producer):
        with open("data/invoices.json") as lines:
            for line in lines:
                invoice = json.loads(line)
                store_id = str(invoice["StoreID"]).encode('utf-8')
                producer.produce(self.topic, key=store_id, value=json.dumps(invoice).encode('utf-8'), callback=self.delivery_callback)
                time.sleep(0.5)
                producer.poll(1)

    def start(self):
        kafka_producer = Producer(self.conf)
        self.produce_invoices(kafka_producer)
        kafka_producer.flush()


if __name__ == "__main__":
    invoice_producer = InvoiceProducer()
    invoice_producer.start()
