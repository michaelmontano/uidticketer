import sys
sys.path = ['..'] + sys.path

from src import idworker
import time

def test_new_worker():
    worker = idworker.IdWorker(1, 2, 3, 'localhost', 'user', 'passwd', 'uidticketer')
    assert worker

def test_generate_id():
    worker = idworker.IdWorker(1, 2, 3, 'localhost', 'user', 'passwd', 'uidticketer')
    tid = worker.next_id()
    assert tid > 0

def test_get_worker_id():
    worker = idworker.IdWorker(1, 2, 3, 'localhost', 'user', 'passwd', 'uidticketer')
    assert worker.get_worker_id() == 1

def test_get_worker_increment():
    worker = idworker.IdWorker(1, 2, 3, 'localhost', 'user', 'passwd', 'uidticketer')
    assert worker.get_worker_increment() == 2

def test_get_worker_offset():
    worker = idworker.IdWorker(1, 2, 3, 'localhost', 'user', 'passwd', 'uidticketer')
    assert worker.get_worker_offset() == 3

def test_generate_increasing_ids():
    worker = idworker.IdWorker(1, 2, 3, 'localhost', 'user', 'passwd', 'uidticketer')
    last_id = 0
    for i in range(100):
        tid = worker.next_id()
        assert tid > last_id
        last_id = tid

def test_generate_one_million_ids_quickly():
    worker = idworker.IdWorker(1, 2, 3, 'localhost', 'user', 'passwd', 'uidticketer')
    t1 = int(time.time()*1000)
    for i in range(10000):
        tid = worker.next_id()
        assert tid
    t2 = int(time.time()*1000)
    rate = 10000.0/(t2 - t1)
    print 'generated 10000 ids in %d ms, or %s ids/ms' %(t2 - t1, rate)
    assert rate > 1

def test_generate_increment_ids():
    worker = idworker.IdWorker(1, 2, 3, 'localhost', 'user', 'passwd', 'uidticketer')
    tid1 = worker.next_id()
    tid2 = worker.next_id()
    assert tid2 == tid1 + 2