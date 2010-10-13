import sys
sys.path = ['..'] + sys.path

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from lib.genpyblocking.uidticketer import UIDTicketer
from lib.genpyblocking.uidticketer.ttypes import *

def print_usage():
    print 'python test_client.py <port> <get_worker_id|get_worker_increment|get_worker_offset|get_id>'
    print 'e.g. python test_client.py 2222 get_id'

def main():
    if len(sys.argv) != 3:
        return print_usage()
    
    port = int(sys.argv[1])
    service = sys.argv[2]
    
    transport = TSocket.TSocket('localhost', port)
    transport = TTransport.TFramedTransport(transport)
    prot = TBinaryProtocol.TBinaryProtocol(transport)
    client = UIDTicketer.Client(prot)
    transport.open()
    
    if service == 'get_worker_id':
        print client.get_worker_id()
    elif service == 'get_worker_increment':
        print client.get_worker_increment()
    elif service == 'get_worker_offset':
        print client.get_worker_offset()
    elif service == 'get_id':
        print client.get_id()
    else:
        print_usage()

if __name__ == '__main__':
    sys.exit(main())