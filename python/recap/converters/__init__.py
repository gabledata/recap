from recap.converters.converter import Converter

Converter.register_converter(
    "frictionless",
    "recap.converters.avro.FrictionlessConverter",
)
Converter.register_converter(
    "sqlalchemy",
    "recap.converters.avro.SQLAlchemyConverter",
)
Converter.register_converter(
    "json",
    "recap.converters.json_schema.JsonSchemaConverter",
)
Converter.register_converter(
    "recap",
    "recap.converters.recap.RecapConverter",
)
