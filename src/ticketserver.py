import sys
sys.path = ['..'] + sys.path

import zope
from twisted.internet import reactor

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.transport import TTwisted
from thrift.protocol import TBinaryProtocol

from lib.genpy.uidticketer import UIDTicketer
from lib.genpy.uidticketer.ttypes import *

import idworker

class TicketServer(object):
    zope.interface.implements(UIDTicketer.Iface)
    
    def __init__(self, worker_id, worker_increment, worker_offset, mysql_host, mysql_user, mysql_passwd, mysql_db):
        self.worker = idworker.IdWorker(worker_id, worker_increment, worker_offset, mysql_host, mysql_user, mysql_passwd, mysql_db)
    
    def get_worker_id(self):
        return self.worker.get_worker_id()
    
    def get_worker_increment(self):
        return self.worker.get_worker_increment()
    
    def get_worker_offset(self):
        return self.worker.get_worker_offset()
    
    def get_id(self):
        return self.worker.get_id()

def print_usage():
    print 'python ticketserver.py <port> <worker_id> <worker_increment> <worker_offset> <mysql_host> <mysql_user> <mysql_passwd> <mysql_db>'
    print 'e.g.: python ticketserver.py 2222 1 2 3 localhost user passwd uidticketer'

def main():
    if len(sys.argv) != 9:
        return print_usage()
    
    port = int(sys.argv[1])
    worker_id = int(sys.argv[2])
    worker_increment = int(sys.argv[3])
    worker_offset = int(sys.argv[4])
    mysql_host = sys.argv[5]
    mysql_user = sys.argv[6]
    mysql_passwd = sys.argv[7]
    mysql_db = sys.argv[8]
    
    reactor.listenTCP(port, TTwisted.ThriftServerFactory(
                                processor=UIDTicketer.Processor(TicketServer(worker_id, worker_increment, worker_offset, mysql_host, mysql_user, mysql_passwd, mysql_db)),
                                iprot_factory=TBinaryProtocol.TBinaryProtocolFactory()
    ))
    reactor.run()

if __name__ == '__main__':
    sys.exit(main())