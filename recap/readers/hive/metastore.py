from thrift_gen import ThriftHiveMetastore as hms
from thrift_gen.ttypes import *
from thrift.transport import TTransport, TSocket
from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from enum import Enum
from typing import Dict, List

    
class HPrincipalType(Enum):
    ROLE = "ROLE"
    USER = "USER"

class PrimitiveCategory(Enum):
    VOID = "VOID"
    BOOLEAN = "BOOLEAN"
    BYTE = "BYTE"
    SHORT = "SHORT"
    INT = "INT"
    LONG = "LONG"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    STRING = "STRING"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMPLOCALTZ = "TIMESTAMPLOCALTZ"
    BINARY = "BINARY"
    DECIMAL = "DECIMAL"
    VARCHAR = "VARCHAR"
    CHAR = "CHAR"
    INTERVAL_YEAR_MONTH = "INTERVAL_YEAR_MONTH"
    INTERVAL_DAY_TIME = "INTERVAL_DAY_TIME"
    UNKNOWN = "UNKNOWN"

class HTypeCategory(Enum):
    PRIMITIVE = PrimitiveCategory
    STRUCT = "STRUCT"
    MAP = "MAP"
    LIST = "LIST"
    UNION = "UNION"

class HType:
    def __init__(self, name: str, category: HTypeCategory):
        self.name = name
        self.category = category
    
class HMapType(HType):
    def __init__(self, keyType: HType, valueType: HType):
        super().__init__("MAP", HTypeCategory.MAP)
        self.keyType = keyType
        self.valueType = valueType

class HListType(HType):
    def __init__(self, elementType: HType):
        super().__init__("LIST", HTypeCategory.LIST)
        self.elementType = elementType

class HUnionType(HType):
    def __init__(self, types: List[HType]):
        super().__init__("UNION", HTypeCategory.UNION)
        self.types = types

class HVarcharType(HType):
    def __init__(self, length: int):
        MAX_VARCHAR_LENGTH = 65535
        if length > MAX_VARCHAR_LENGTH:
            raise ValueError("Varchar length cannot exceed 65535")
        super().__init__("VARCHAR", HTypeCategory.PRIMITIVE)
        self.length = length

class HCharType(HType):
    def __init__(self, length: int):
        MAX_CHAR_LENGTH = 255
        if length > MAX_CHAR_LENGTH:
            raise ValueError("Char length cannot exceed 255")
        super().__init__("CHAR", HTypeCategory.PRIMITIVE)
        self.length = length

class HDecimalType(HType):
    def __init__(self, precision: int, scale: int):
        MAX_PRECISION = 38
        MAX_SCALE = 38
        if precision > MAX_PRECISION:
            raise ValueError("Decimal precision cannot exceed 38")
        if scale > MAX_SCALE:
            raise ValueError("Decimal scale cannot exceed 38")
        super().__init__("DECIMAL", HTypeCategory.PRIMITIVE)
        self.precision = precision
        self.scale = scale

class HStructType(HType):
    def __init__(self, names: List[str], types:List[HType]):
        if len(names) != len(types):
            raise ValueError("mismatched size of names and types.")
        if None in names:
            raise ValueError("names cannot contain None")
        if None in types:
            raise ValueError("types cannot contain None")
        
        super().__init__("STRUCT", HTypeCategory.STRUCT)
        self.names = names
        self.types = types

class HPrimitiveType(HType):
    def __init__(self, primitiveType: PrimitiveCategory):
        super().__init__(primitiveType.value, HTypeCategory.PRIMITIVE)
        self.primitiveType = primitiveType

class HDatabase:
    def __init__(self, name: str, 
                 location: str =None, 
                 ownerName:str =None, 
                 ownerType: HPrincipalType =None, 
                 comment=None, 
                 parameters: Dict[str, str]=None):
        
        self.name = name
        self.location = location
        self.ownerName = ownerName
        self.ownerType = ownerType
        self.comment = comment
        self.parameters = parameters
    
class HColumn:
    def __init__(self, name: str, type: HType, comment: str =None):
        self.name = name
        self.type = type
        self.comment = comment

class StorageFormat:
    def __init__(self, serde: str, inputFormat: str, outputFormat: str) -> None:
        self.serde = serde
        self.inputFormat = inputFormat
        self.outputFormat = outputFormat

#TODO: Implement
class HiveBucketProperty:
    def __init__(self):
        # TODO!!
        pass

class HStorage:
    def __init__(self, storageFormat: StorageFormat,
                 skewed: bool = False, 
                 location: str =None, 
                 bucketProperty: HiveBucketProperty =None, 
                 serdeParameters: Dict[str, str] =None):
        
        if storageFormat is None:
            raise ValueError("storageFormat cannot be None")
        self.storageFormat = storageFormat
        self.skewed = skewed
        self.location = location
        self.bucketProperty = bucketProperty
        self.serdeParameters = serdeParameters

class HTable:
    def __init__(self, databaseName: str, name: str, 
                 tableType: str, columns: List[HColumn], 
                 partitionColumns: List[HColumn],
                 storage: HStorage, 
                 parameters: Dict[str, str], 
                 viewOriginalText: str = None, 
                 viewExpandedText: str = None, 
                 writeId: int = None, 
                 owner: str = None):
        
        self.databaseName = databaseName
        self.name = name
        self.storage = storage
        self.tableType = tableType
        self.columns = columns
        self.partitionColumns = partitionColumns
        self.parameters = parameters
        self.viewOriginalText = viewOriginalText
        self.viewExpandedText = viewExpandedText
        self.writeId = writeId
        self.owner = owner


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

    def list_databases(self) -> List[str]:
        databases = self.client.get_all_databases()
        db_names = []
        for database in databases:
            db_names.append(database.name)
        return db_names
            
    def get_database(self, name: str) -> HDatabase:
        db: Database = self.client.get_database(name)
        
        if db.ownerType is PrincipalType.USER:
            ownerType = HPrincipalType.USER
        elif db.ownerType is PrincipalType.ROLE:
            ownerType = HPrincipalType.ROLE
        else:
            ownerType = None

        return HDatabase(db.name, db.locationUri, db.ownerName, ownerType, db.description, db.parameters)
        

        