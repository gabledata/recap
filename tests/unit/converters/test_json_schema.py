from recap.converters.json_schema import JSONSchemaConverter
from recap.types import (
    BoolType,
    FloatType,
    IntType,
    ListType,
    NullType,
    StringType,
    StructType,
    UnionType,
)


def test_all_basic_types():
    json_schema = """
    {
        "type": "object",
        "properties": {
            "a_string": {"type": "string"},
            "a_number": {"type": "number"},
            "an_integer": {"type": "integer"},
            "a_boolean": {"type": "boolean"},
            "a_null": {"type": "null"}
        }
    }
    """
    struct_type = JSONSchemaConverter().convert(json_schema)
    assert isinstance(struct_type, StructType)
    assert struct_type.fields == [
        UnionType(
            [NullType(), StringType(bytes_=9_223_372_036_854_775_807)],
            name="a_string",
            default=None,
        ),
        UnionType(
            [NullType(), FloatType(bits=64)],
            name="a_number",
            default=None,
        ),
        UnionType(
            [NullType(), IntType(bits=32, signed=True)],
            name="an_integer",
            default=None,
        ),
        UnionType(
            [NullType(), BoolType()],
            name="a_boolean",
            default=None,
        ),
        UnionType(
            [NullType(), NullType()],
            name="a_null",
            default=None,
        ),
    ]


def test_nested_objects():
    json_schema = """
    {
        "type": "object",
        "properties": {
            "an_object": {
                "type": "object",
                "properties": {
                    "a_string": {"type": "string"}
                }
            }
        }
    }
    """
    struct_type = JSONSchemaConverter().convert(json_schema)
    assert isinstance(struct_type, StructType)
    assert struct_type.fields == [
        UnionType(
            [
                NullType(),
                StructType(
                    [
                        UnionType(
                            [NullType(), StringType(bytes_=9_223_372_036_854_775_807)],
                            name="a_string",
                            default=None,
                        ),
                    ],
                ),
            ],
            name="an_object",
            default=None,
        ),
    ]


def test_object_with_array_of_objects():
    json_schema = """
    {
        "type": "object",
        "properties": {
            "an_array": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "a_string": {"type": "string"}
                    }
                }
            }
        }
    }
    """
    struct_type = JSONSchemaConverter().convert(json_schema)
    assert isinstance(struct_type, StructType)
    assert struct_type.fields == [
        UnionType(
            [
                NullType(),
                ListType(
                    StructType(
                        [
                            UnionType(
                                [
                                    NullType(),
                                    StringType(bytes_=9_223_372_036_854_775_807),
                                ],
                                name="a_string",
                                default=None,
                            )
                        ]
                    ),
                ),
            ],
            name="an_array",
            default=None,
        ),
    ]


def test_required_properties():
    json_schema = """
    {
        "type": "object",
        "properties": {
            "required_property": {"type": "string"},
            "optional_property": {"type": "number"}
        },
        "required": ["required_property"]
    }
    """
    struct_type = JSONSchemaConverter().convert(json_schema)
    assert isinstance(struct_type, StructType)
    assert struct_type.fields == [
        StringType(bytes_=9_223_372_036_854_775_807, name="required_property"),
        UnionType(
            [NullType(), FloatType(bits=64)],
            name="optional_property",
            default=None,
        ),
    ]


def test_doc_attribute():
    json_schema = """
    {
        "type": "object",
        "properties": {
            "a_string": {
                "type": "string",
                "description": "This is a string"
            }
        }
    }
    """
    struct_type = JSONSchemaConverter().convert(json_schema)
    assert isinstance(struct_type, StructType)
    assert struct_type.fields == [
        UnionType(
            [NullType(), StringType(bytes_=9_223_372_036_854_775_807)],
            name="a_string",
            doc="This is a string",
            default=None,
        ),
    ]


def test_name_attribute():
    json_schema = """
    {
        "type": "object",
        "properties": {
            "a_string": {
                "type": "string",
                "title": "A String"
            }
        }
    }
    """
    struct_type = JSONSchemaConverter().convert(json_schema)
    assert isinstance(struct_type, StructType)
    assert struct_type.fields == [
        UnionType(
            [NullType(), StringType(bytes_=9_223_372_036_854_775_807)],
            name="a_string",
            default=None,
        ),
    ]


def test_default_attribute():
    json_schema = """
    {
        "type": "object",
        "properties": {
            "a_string": {
                "type": "string",
                "default": "Default value"
            }
        }
    }
    """
    struct_type = JSONSchemaConverter().convert(json_schema)
    assert isinstance(struct_type, StructType)
    assert struct_type.fields == [
        UnionType(
            [
                NullType(),
                StringType(bytes_=9_223_372_036_854_775_807),
            ],
            name="a_string",
            default="Default value",
        ),
    ]


def test_convert_date():
    converter = JSONSchemaConverter()
    schema = """
    {
        "type": "object",
        "properties": {
            "date": {
                "type": "string",
                "format": "date"
            }
        },
        "required": ["date"]
    }
    """
    result = converter.convert(schema)
    assert isinstance(result, StructType)
    assert isinstance(result.fields[0], StringType)
    assert result.fields[0].logical == "org.iso.8601.Date"


def test_convert_datetime():
    converter = JSONSchemaConverter()
    schema = """
    {
        "type": "object",
        "properties": {
            "datetime": {
                "type": "string",
                "format": "date-time"
            }
        },
        "required": ["datetime"]
    }
    """
    result = converter.convert(schema)
    assert isinstance(result, StructType)
    assert isinstance(result.fields[0], StringType)
    assert result.fields[0].logical == "org.iso.8601.DateTime"


def test_convert_time():
    converter = JSONSchemaConverter()
    schema = """
    {
        "type": "object",
        "properties": {
            "time": {
                "type": "string",
                "format": "time"
            }
        },
        "required": ["time"]
    }
    """
    result = converter.convert(schema)
    assert isinstance(result, StructType)
    assert isinstance(result.fields[0], StringType)
    assert result.fields[0].logical == "org.iso.8601.Time"
