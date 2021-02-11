import sys
import os

from confluent_Kafka import Consumer, KafkaException, KafkaError

if __name__ == '__main__':
    topics = os.environ['Kafka_TOPIC'].split(",")

 
    conf = {
        'bootstrap.servers': os.environ['Kafka_BROKERS'],
        'group.id': "%s-consumer" % os.environ['Kafka_USERNAME'],
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        'security.protocol': 'SASL_SSL',
	'sasl.mechanisms': 'SCRAM-SHA-256',
        'sasl.username': os.environ['Kafka_USERNAME'],
        'sasl.password': os.environ['Kafka_PASSWORD']
    }

    c = Consumer(**conf)
    c.subscribe(topics)
    try:
        while True:
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                   
                    raise KafkaException(msg.error())
            else:
                
                sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                                 (msg.topic(), msg.partition(), msg.offset(),
                                  str(msg.key())))
                print(msg.value())

    except KeyboardInterrupt:
        sys.stderr.write('%% Abort\n')

   
    c.close()
