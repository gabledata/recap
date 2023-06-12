from recap.readers.hive.thrift_gen import ThriftHiveMetastore as hms
from recap.readers.hive.thrift_gen.ttypes import *
from thrift.transport import TTransport, TSocket
from thrift.protocol.TBinaryProtocol import TBinaryProtocol

class HMS:
    def __init__(self, host='localhost', port=9090):
        self.host = host
        self.port = port
        self.socket = TSocket.TSocket(self.host, self.port)
        self.transport = TTransport.TBufferedTransport(self.socket)
        self.protocol = TBinaryProtocol(self.transport)
        self.client = hms.Client(self.protocol)

    def connect(self):
        self.transport.open()

    def disconnect(self):
        self.transport.close()

    def get_databases(self):
        return self.client.get_all_databases()
        
    def get_tables(self, database):
        return self.client.get_all_tables(database)
        
    def get_table(self, database, table):
        return self.client.get_table(database, table)
        
    def get_catalogs(self):
        return self.client.get_catalogs()
        
    def get_schema(self, database, table):
        return self.client.get_schema(database, table)
        
    def get_fields(self, database, table):
        return self.client.get_fields(database, table)