import os

import pytest
from pymetastore.hive_metastore import ttypes
from pymetastore.hive_metastore.ThriftHiveMetastore import Client
from pymetastore.metastore import HMS
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport

from recap.clients import create_client
from recap.clients.hive_metastore import HiveMetastoreClient
from recap.types import (
    BoolType,
    BytesType,
    FloatType,
    IntType,
    ListType,
    MapType,
    NullType,
    StringType,
    StructType,
    UnionType,
)


@pytest.fixture(scope="module")
def hive_client():
    host = os.environ.get("HMS_HOST", "localhost")
    port = int(os.environ.get("HMS_PORT", 9083))
    transport = TSocket.TSocket(host, port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = Client(protocol)
    transport.open()

    yield client

    transport.close()


@pytest.fixture(scope="module", autouse=True)
def setup_data(hive_client):
    db = ttypes.Database(
        name="test_db",
        description="This is a test database",
        locationUri="/tmp/test_db.db",
        parameters={},
        ownerName="owner",
    )

    if "test_db" in hive_client.get_all_databases():
        hive_client.drop_database("test_db", True, True)
    hive_client.create_database(db)

    partitionKeys = [ttypes.FieldSchema(name="partition", type="string", comment="")]

    serde_info = ttypes.SerDeInfo(
        name="test_serde",
        serializationLib="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        parameters={"field.delim": ","},
    )

    # testing primitive Types
    cols2 = [
        ttypes.FieldSchema(name="col4", type="boolean", comment="c4"),
        ttypes.FieldSchema(name="col5", type="tinyint", comment="c5"),
        ttypes.FieldSchema(name="col6", type="smallint", comment="c6"),
        ttypes.FieldSchema(name="col7", type="bigint", comment="c7"),
        ttypes.FieldSchema(name="col8", type="float", comment="c8"),
        ttypes.FieldSchema(name="col9", type="double", comment="c9"),
        ttypes.FieldSchema(name="col10", type="date", comment="c10"),
        ttypes.FieldSchema(name="col11", type="timestamp", comment="c11"),
        ttypes.FieldSchema(
            name="col12",
            type="timestamp with local time zone",
            comment="c12",
        ),
        ttypes.FieldSchema(name="col13", type="interval_year_month", comment="c13"),
        ttypes.FieldSchema(name="col14", type="interval_day_time", comment="c14"),
        ttypes.FieldSchema(name="col15", type="binary", comment="c15"),
        ttypes.FieldSchema(name="col16", type="void", comment="c16"),
    ]

    storageDesc = ttypes.StorageDescriptor(
        location="/tmp/test_db/test_table2",
        cols=cols2,
        inputFormat="org.apache.hadoop.mapred.TextInputFormat",
        outputFormat="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        compressed=False,
        numBuckets=-1,
        serdeInfo=serde_info,
        bucketCols=[],
    )

    table = ttypes.Table(
        tableName="test_table2",
        dbName="test_db",
        owner="owner",
        createTime=0,
        lastAccessTime=0,
        retention=0,
        sd=storageDesc,
        partitionKeys=partitionKeys,
        parameters={},
        tableType="EXTERNAL_TABLE",
    )

    if "test_table2" in hive_client.get_all_tables("test_db"):
        hive_client.drop_table("test_db", "test_table2", True)
    hive_client.create_table(table)

    # testing Parameterized Types
    cols3 = [
        ttypes.FieldSchema(name="col16", type="decimal(10,2)", comment="c16"),
        ttypes.FieldSchema(name="col17", type="varchar(10)", comment="c17"),
        ttypes.FieldSchema(name="col18", type="char(10)", comment="c18"),
        ttypes.FieldSchema(name="col19", type="array<int>", comment="c19"),
        ttypes.FieldSchema(name="col20", type="map<int,string>", comment="c20"),
        ttypes.FieldSchema(name="col21", type="struct<a:int,b:string>", comment="c21"),
        ttypes.FieldSchema(name="col22", type="uniontype<int,string>", comment="c22"),
    ]

    storageDesc = ttypes.StorageDescriptor(
        location="/tmp/test_db/test_table3",
        cols=cols3,
        inputFormat="org.apache.hadoop.mapred.TextInputFormat",
        outputFormat="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        compressed=False,
        numBuckets=-1,
        serdeInfo=serde_info,
        bucketCols=[],
    )

    table = ttypes.Table(
        tableName="test_table3",
        dbName="test_db",
        owner="owner",
        createTime=0,
        lastAccessTime=0,
        retention=0,
        sd=storageDesc,
        partitionKeys=partitionKeys,
        parameters={},
        tableType="EXTERNAL_TABLE",
    )

    if "test_table3" in hive_client.get_all_tables("test_db"):
        hive_client.drop_table("test_db", "test_table3", True)
    hive_client.create_table(table)


def test_primitive_types(hive_client):
    hms = HMS(hive_client)
    client = HiveMetastoreClient(hms)
    table = client.schema("test_db", "test_table2")
    fields = table.fields

    assert len(fields) == 13

    assert fields[0].extra_attrs["name"] == "col4"
    assert isinstance(fields[0], UnionType)
    assert isinstance(fields[0].types[0], NullType)
    assert isinstance(fields[0].types[1], BoolType)
    assert fields[0].doc == "c4"

    assert fields[1].extra_attrs["name"] == "col5"
    assert isinstance(fields[1], UnionType)
    assert isinstance(fields[1].types[0], NullType)
    assert isinstance(fields[1].types[1], IntType)
    assert fields[1].types[1].bits == 8
    assert fields[1].types[1].signed is True
    assert fields[1].doc == "c5"

    assert fields[2].extra_attrs["name"] == "col6"
    assert isinstance(fields[2], UnionType)
    assert isinstance(fields[2].types[0], NullType)
    assert isinstance(fields[2].types[1], IntType)
    assert fields[2].types[1].bits == 16
    assert fields[2].types[1].signed is True
    assert fields[2].doc == "c6"

    assert fields[3].extra_attrs["name"] == "col7"
    assert isinstance(fields[3], UnionType)
    assert isinstance(fields[3].types[0], NullType)
    assert isinstance(fields[3].types[1], IntType)
    assert fields[3].types[1].bits == 64
    assert fields[3].types[1].signed is True
    assert fields[3].doc == "c7"

    assert fields[4].extra_attrs["name"] == "col8"
    assert isinstance(fields[4], UnionType)
    assert isinstance(fields[4].types[0], NullType)
    assert isinstance(fields[4].types[1], FloatType)
    assert fields[4].types[1].bits == 32
    assert fields[4].doc == "c8"

    assert fields[5].extra_attrs["name"] == "col9"
    assert isinstance(fields[5], UnionType)
    assert isinstance(fields[5].types[0], NullType)
    assert isinstance(fields[5].types[1], FloatType)
    assert fields[5].types[1].bits == 64
    assert fields[5].doc == "c9"

    assert fields[6].extra_attrs["name"] == "col10"
    assert isinstance(fields[6], UnionType)
    assert isinstance(fields[6].types[0], NullType)
    assert isinstance(fields[6].types[1], IntType)
    assert fields[6].types[1].logical == "build.recap.Date"
    assert fields[6].types[1].bits == 32
    assert fields[6].types[1].signed is True
    assert fields[6].types[1].extra_attrs["unit"] == "day"
    assert fields[6].doc == "c10"

    assert fields[7].extra_attrs["name"] == "col11"
    assert isinstance(fields[7], UnionType)
    assert isinstance(fields[7].types[0], NullType)
    assert isinstance(fields[7].types[1], IntType)
    assert fields[7].types[1].logical == "build.recap.Timestamp"
    assert fields[7].types[1].bits == 64
    assert fields[7].types[1].signed is True
    assert fields[7].types[1].extra_attrs["unit"] == "nanosecond"
    assert fields[7].types[1].extra_attrs["timezone"] == "UTC"
    assert fields[7].doc == "c11"

    assert fields[8].extra_attrs["name"] == "col12"
    assert isinstance(fields[8], UnionType)
    assert isinstance(fields[8].types[0], NullType)
    assert isinstance(fields[8].types[1], IntType)
    assert fields[8].types[1].logical == "build.recap.Timestamp"
    assert fields[8].types[1].bits == 64
    assert fields[8].types[1].signed is True
    assert fields[8].types[1].extra_attrs["unit"] == "nanosecond"
    assert fields[8].types[1].extra_attrs["timezone"] is None
    assert fields[8].doc == "c12"

    assert fields[9].extra_attrs["name"] == "col13"
    assert isinstance(fields[9], UnionType)
    assert isinstance(fields[9].types[0], NullType)
    assert isinstance(fields[9].types[1], BytesType)
    assert fields[9].doc == "c13"

    assert fields[10].extra_attrs["name"] == "col14"
    assert isinstance(fields[10], UnionType)
    assert isinstance(fields[10].types[0], NullType)
    assert isinstance(fields[10].types[1], BytesType)
    assert fields[10].doc == "c14"

    assert fields[11].extra_attrs["name"] == "col15"
    assert isinstance(fields[11], UnionType)
    assert isinstance(fields[11].types[0], NullType)
    assert isinstance(fields[11].types[1], BytesType)
    assert fields[11].types[1].bytes_ == 2_147_483_647
    assert fields[11].doc == "c15"

    assert fields[12].extra_attrs["name"] == "col16"
    assert isinstance(fields[12], NullType)
    assert fields[12].doc == "c16"


def test_parameterized_types(hive_client):
    hms = HMS(hive_client)
    client = HiveMetastoreClient(hms)
    table = client.schema("test_db", "test_table3")
    fields = table.fields

    assert len(fields) == 7

    assert fields[0].extra_attrs["name"] == "col16"
    assert isinstance(fields[0], UnionType)
    assert isinstance(fields[0].types[0], NullType)
    assert isinstance(fields[0].types[1], BytesType)
    assert fields[0].types[1].logical == "build.recap.Decimal"
    assert fields[0].types[1].bytes_ == 16
    assert fields[0].types[1].extra_attrs["precision"] == 10
    assert fields[0].types[1].extra_attrs["scale"] == 2
    assert fields[0].doc == "c16"

    assert fields[1].extra_attrs["name"] == "col17"
    assert isinstance(fields[1], UnionType)
    assert isinstance(fields[1].types[0], NullType)
    assert isinstance(fields[1].types[1], StringType)
    assert fields[1].types[1].bytes_ == 10
    assert fields[1].doc == "c17"

    assert fields[2].extra_attrs["name"] == "col18"
    assert isinstance(fields[2], UnionType)
    assert isinstance(fields[2].types[0], NullType)
    assert isinstance(fields[2].types[1], StringType)
    assert fields[2].types[1].bytes_ == 10
    assert fields[2].doc == "c18"

    assert fields[3].extra_attrs["name"] == "col19"
    assert isinstance(fields[3], UnionType)
    assert isinstance(fields[3].types[0], NullType)
    assert isinstance(fields[3].types[1], ListType)
    assert isinstance(fields[3].types[1].values, UnionType)
    assert isinstance(fields[3].types[1].values.types[0], NullType)
    assert isinstance(fields[3].types[1].values.types[1], IntType)
    assert fields[3].types[1].values.types[1].bits == 32
    assert fields[3].types[1].values.types[1].signed
    assert fields[3].doc == "c19"

    assert fields[4].extra_attrs["name"] == "col20"
    assert isinstance(fields[4], UnionType)
    assert isinstance(fields[4].types[0], NullType)
    assert isinstance(fields[4].types[1], MapType)
    assert isinstance(fields[4].types[1].keys, UnionType)
    assert isinstance(fields[4].types[1].keys.types[0], NullType)
    assert isinstance(fields[4].types[1].keys.types[1], IntType)
    assert fields[4].types[1].keys.types[1].bits == 32
    assert fields[4].types[1].keys.types[1].signed
    assert isinstance(fields[4].types[1].values, UnionType)
    assert isinstance(fields[4].types[1].values.types[0], NullType)
    assert isinstance(fields[4].types[1].values.types[1], StringType)
    assert fields[4].types[1].values.types[1].bytes_ is None
    assert fields[4].doc == "c20"

    assert fields[5].extra_attrs["name"] == "col21"
    assert isinstance(fields[5], UnionType)
    assert isinstance(fields[5].types[0], NullType)
    assert isinstance(fields[5].types[1], StructType)
    assert len(fields[5].types[1].fields) == 2
    assert fields[5].types[1].fields[0].extra_attrs["name"] == "a"
    assert isinstance(fields[5].types[1].fields[0], UnionType)
    assert isinstance(fields[5].types[1].fields[0].types[0], NullType)
    assert isinstance(fields[5].types[1].fields[0].types[1], IntType)
    assert fields[5].types[1].fields[0].types[1].bits == 32
    assert fields[5].types[1].fields[0].types[1].signed
    assert fields[5].types[1].fields[1].extra_attrs["name"] == "b"
    assert isinstance(fields[5].types[1].fields[1], UnionType)
    assert isinstance(fields[5].types[1].fields[1].types[0], NullType)
    assert isinstance(fields[5].types[1].fields[1].types[1], StringType)
    assert fields[5].types[1].fields[1].types[1].bytes_ is None
    assert fields[5].doc == "c21"

    assert fields[6].extra_attrs["name"] == "col22"
    assert isinstance(fields[6], UnionType)
    assert len(fields[6].types) == 3
    assert isinstance(fields[6].types[0], NullType)
    assert isinstance(fields[6].types[1], IntType)
    assert fields[6].types[1].bits == 32
    assert fields[6].types[1].signed
    assert isinstance(fields[6].types[2], StringType)
    assert fields[6].types[2].bytes_ is None
    assert fields[6].doc == "c22"


def test_create_client():
    hms_url = "thrift+hms://hive:password@localhost:9083/test_db"

    with create_client(hms_url) as client:
        assert client.ls() == ["default", "test_db"]
        assert client.ls("test_db") == ["test_table2", "test_table3"]
