'''
Create bible generator
'''

from time import sleep
from datetime import datetime
from random import random
from kafka import KafkaProducer
import json
import re
import os

producer = KafkaProducer(bootstrap_servers=['course-kafka:9092'],
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))

dir_name = 'my_stream_directory'

# Read from csv file

def verse_generator(limit=5):
    '''
    This generator iterates the (arbitrary) lines of the file Bible.txt
    and yields its verses, as determined by lines beginning with a regex of the
    form [chapter: verse]
    '''
    if limit is None:
        limit = 10**6
    with open(r'SparkTeacher/SparkStructureStreaming/Bible.txt') as f:
        verse=''
        for i, line in enumerate(f):            # Skip empty lines
            if line=='\n':
                continue
            if limit is not None:               # Keep output limit
                if i>limit:
                    break
            if re.findall('^\d+:\d+', line):    # If beginning of a verse
                sleep(random()/10)
                yield verse                     # Yield previous verse
                verse = line[:-1]               # Start a new verse (ignore '\n')
            else:
                if verse:                       # If continuation of a verse
                    verse += line[:-1]
                else:                           # Header / title / comments
                    continue

for verse in verse_generator(200):
    print(verse)
    if verse:
        producer.send('bible', {'chapter': verse.split(":")[0], 'verse': verse.split(":")[1].split(" ")[0], 'text': " ".join(verse.split(" ")[1:])})
    sleep(random())



