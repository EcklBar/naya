from random import randint, random
from os import getcwd, remove
from time import time, sleep
from datetime import datetime

# This generator yields 3-tuples of the form (counter, hostname, ts).

def event_generator():
    counter = 1
    while True:
        hostname = f'host{randint(1, 5)}'
        ts = datetime.now().strftime('%a %b %d %H:%M:%S %Y')
        print(counter, hostname, ts)
        yield counter, hostname, ts
        counter += 1



log_path = '/home/naya/kafka/srcFiles/srcFile.log'
with open(log_path, 'w') as f:
    for counter, hostname, ts in event_generator():
        f.write(f'Event #{counter}|{hostname}|{ts}\n')
        sleep(5*random())
        f.flush()


