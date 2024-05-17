import multiprocessing

class Queue:
    def __init__(self):
        self._items = []
        self._semaphore = multiprocessing.Semaphore(1)  # Semaphore for access control
        self._semaphore_enq = multiprocessing.Semaphore(1)  # Semaphore for enqueue access control
        self._semaphore_deq = multiprocessing.Semaphore(1)  # Semaphore for dequeue access control

    def enqueue(self, item):
        self._semaphore_enq.acquire()
        self._items.append(item)
        self._semaphore_enq.release()

    def dequeue(self):
        self._semaphore_deq.acquire()
        if self._items:
            item = self._items.pop(0)
            self._semaphore_deq.release()
            return item
        else:
            self._semaphore_deq.release()
            # Return None if the queue is empty

    def size(self):
        return len(self._items)