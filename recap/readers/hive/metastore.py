from thrift_gen import ThriftHiveMetastore as hms
from thrift_gen.ttypes import *
from thrift.transport import TTransport, TSocket
from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from enum import Enum
from typing import Dict, List
from collections import namedtuple
from typing import Any
    
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

# We need to maintain a list of the expected serialized type names. Thrift will return the type in string format and we will have to parse it to get the type.
class SerdeTypeNameConstants(Enum):
    # Primitive types
    VOID = "void"
    BOOLEAN = "boolean"
    TINYINT = "tinyint"
    SMALLINT = "smallint"
    INT = "int"
    BIGINT = "bigint"
    FLOAT = "float"
    DOUBLE = "double"
    STRING = "string"
    DATE = "date"
    CHAR = "char"
    VARCHAR = "varchar"
    TIMESTAMP = "timestamp"
    TIMESTAMPLOCALTZ = "timestamp with local time zone"
    DECIMAL = "decimal"
    BINARY = "binary"
    INTERVAL_YEAR_MONTH = "interval_year_month"
    INTERVAL_DAY_TIME = "interval_day_time"
    # Complex types
    LIST = "array"
    MAP = "map"
    STRUCT = "struct"
    UNION = "uniontype"
    UNKNOWN = "unknown"

# To be used for tokening the Thrift type string. We need to tokenize the string to get the type name and the type parameters.
class Token(namedtuple('Token', ['position', 'text', 'type'])):
    def __new__(cls, position: int, text: str, type_: bool) -> Any:
        if text is None:
            raise ValueError("text is null")
        return super().__new__(cls, position, text, type_)

    def __str__(self):
        return f"{self.position}:{self.text}"

class PrimitiveParts(namedtuple('PrimitiveParts', ['type_name', 'type_params'])):
    def __new__(cls, type_name: str, type_params: List[str]) -> Any:
        if type_name is None:
            raise ValueError("type_name is null")
        if type_params is None:
            raise ValueError("type_params is null")
        return super().__new__(cls, type_name, type_params)

    def __str__(self):
        return f"{self.type_name}:{self.type_params}"

# Util functions to parse the Thrift column types (as strings) and return the expected hive type
class TypeParser:

    def __init__(self, type_info_string: str):
        self.type_string = type_info_string
        self.type_tokens = self.tokenize(type_info_string)
        self.index = 0
        
    # we want the base name of the type, in general the format of a parameterized type is <base_name>(<type1>, <type2>, ...), so the base name is the string before the first '('
    @staticmethod
    def get_base_name(type_name: str) -> str:
        index = type_name.find('(')
        if index == -1:
            return type_name
        return type_name[:index]
    
    # Is the type named using the allowed characters?
    @staticmethod
    def is_valid_type_char(c: str) -> bool:
        return c.isalnum() or c in ('_', '.', ' ', '$')
    
    # The concept of the tokenizer is to split the type string into tokens. The tokens are either type names or type parameters. The type parameters are enclosed in parentheses.
    def tokenize(self, type_info_string: str) -> List[Token]:
        tokens = []
        begin = 0
        end = 1
        while end <= len(type_info_string):
            if begin > 0 and type_info_string[begin - 1] == '(' and type_info_string[begin] == "'":
                begin += 1
                end += 1
                while type_info_string[end] != "'":
                    end += 1

            elif type_info_string[begin] == "'" and type_info_string[begin + 1] == ')':
                begin += 1
                end += 1

            if (
                end == len(type_info_string)
                or not self.is_valid_type_char(type_info_string[end - 1])
                or not self.is_valid_type_char(type_info_string[end])
            ):
                token = Token(begin, type_info_string[begin:end], self.is_valid_type_char(type_info_string[begin]))
                tokens.append(token)
                begin = end
            end += 1
        
        return tokens
    
    def peek(self) -> Token:
        if self.index >= len(self.type_tokens):
            raise ValueError("Error: Unexpected end of 'typeInfoString'")
        return self.type_tokens[self.index]

    def expect(self, item: str, alternative: str = None) -> Token:
        if self.index >= len(self.type_tokens):
            raise ValueError(f"Error: {item} expected at the end of 'typeInfoString'")

        token = self.type_tokens[self.index]

        if item == "type":
            if token.text() not in [SerdeTypeNameConstants.LIST.value,
                                SerdeTypeNameConstants.MAP.value,
                                SerdeTypeNameConstants.STRUCT.value,
                                SerdeTypeNameConstants.UNION.value] and (self.get_base_name(token.text()) is None) and (token.text() != alternative):
                raise ValueError(f"Error: {item} expected at the position {token.position()} of 'typeInfoString' but '{token.text()}' is found.")
        elif item == "name":
            if not token.type() and token.text() != alternative:
                raise ValueError(f"Error: {item} expected at the position {token.position()} of 'typeInfoString' but '{token.text()}' is found.")
        elif item != token.text() and token.text() != alternative:
            raise ValueError(f"Error: {item} expected at the position {token.position()} of 'typeInfoString' but '{token.text()}' is found.")

        self.index += 1 # increment the index
        return token
    
    def parse_params(self) -> List[str]:
        params = []

        token = self.peek(self.type_tokens, index)
        if token is not None and token.text() == "(":
            token, index = self.expect("(", None, self.type_tokens, index)
            token = self.peek(self.type_tokens, index)
            while token is None or token.text() != ")":
                token, index = self.expect("name", None, self.type_tokens, index)
                params.append(token.text())
                token, index = self.expect(",", ")", self.type_tokens, index)

            if not params:
                raise ValueError("type parameters expected for type string 'typeInfoString'")

        return params
    
    #TODO: write custom parsing logic for each complex type.
    def parse_type(self) -> HType:
        token = self.peek()
        if token.text() == SerdeTypeNameConstants.LIST.value:
            return self.parse_list_type()
        elif token.text() == SerdeTypeNameConstants.MAP.value:
            return self.parse_map_type()
        elif token.text() == SerdeTypeNameConstants.STRUCT.value:
            return self.parse_struct_type()
        elif token.text() == SerdeTypeNameConstants.UNION.value:
            return self.parse_union_type()
        else:
            return self.parse_primitive_type()

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
    
    def list_tables(self, databaseName: str) -> List[str]:
        tables = self.client.get_all_tables(databaseName)
        table_names = []
        for table in tables:
            table_names.append(table.tableName)
        return table_names
    
    def list_columns(self, databaseName: str, tableName: str) -> List[str]:
        columns = self.client.get_table(databaseName, tableName).sd.cols
        self.client.get_schema(databaseName, tableName)
        column_names = []
        for column in columns:
            column_names.append(column.name)
        return column_names
        

hms = HMS()
hms.connect()
print(hms.list_databases())
print(hms.get_database("default"))
print(hms.list_tables("default"))
print(hms.list_columns("default", "test"))  
hms.disconnect()