import os
from queue import Queue

QUEUE_MAX_SIZE = int(os.getenv("QUEUEGUARD_QUEUE_MAX_SIZE", "100"))
if QUEUE_MAX_SIZE < 1:
    raise ValueError("QUEUEGUARD_QUEUE_MAX_SIZE must be a positive integer")

job_queue = Queue(maxsize=QUEUE_MAX_SIZE)
