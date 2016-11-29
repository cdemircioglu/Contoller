#!/usr/bin/env python
import pika
import sys

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='worknumber',
                         type='fanout')

message = sys.argv[1]
channel.basic_publish(exchange='worknumber',
                      routing_key='',
                      body=message)
print(" [x] Sent %r" % message)
connection.close()
