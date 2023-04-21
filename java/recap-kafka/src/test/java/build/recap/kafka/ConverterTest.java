package build.recap.kafka;

import build.recap.Data;
import build.recap.Type;
import junit.framework.*;
import org.apache.kafka.connect.data.*;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class ConverterTest extends TestCase {
    public void testConverter() {
        Schema addressSchema = SchemaBuilder.struct().name("build.recap.kafka.Address")
                .field("number", Schema.INT8_SCHEMA)
                .build();
        Schema personSchema = SchemaBuilder.struct().name("build.recap.kafka.Person")
                .field("age", Schema.INT8_SCHEMA)
                .field("address", addressSchema)
                .build();
        Struct addressStruct = new Struct(addressSchema)
                .put("number", (byte) 100);
        Struct personStruct = new Struct(personSchema)
                .put("age", (byte) 75)
                .put("address", addressStruct);

        Converter connectConverter = new Converter();
        Data convertedRecapData = connectConverter.convert(new SchemaAndValue(personStruct.schema(), personStruct));
        SchemaAndValue convertedConnectData = connectConverter.convert(convertedRecapData);
        assertEquals(personStruct, convertedConnectData.value());
    }

    public void testOptionalPrimitive() {
        Schema addressSchema = SchemaBuilder.struct().name("build.recap.kafka.Address")
                .field("number", Schema.OPTIONAL_INT8_SCHEMA)
                .build();
        Schema personSchema = SchemaBuilder.struct().name("build.recap.kafka.Person")
                .field("age", Schema.INT8_SCHEMA)
                .field("address", addressSchema)
                .build();
        Struct addressStruct = new Struct(addressSchema)
                .put("number", (byte) 100);
        Struct personStruct = new Struct(personSchema)
                .put("age", (byte) 75)
                .put("address", addressStruct);
        Converter connectConverter = new Converter();
        Data convertedRecapData = connectConverter.convert(new SchemaAndValue(personStruct.schema(), personStruct));
        SchemaAndValue convertedConnectData = connectConverter.convert(convertedRecapData);
        assertEquals(personStruct, convertedConnectData.value());
    }

    public void testPrimitives() {
        Schema schema = SchemaBuilder.struct()
                .field("int8", Schema.INT8_SCHEMA)
                .field("int16", Schema.INT16_SCHEMA)
                .field("int32", Schema.INT32_SCHEMA)
                .field("int64", Schema.INT64_SCHEMA)
                .field("float32", Schema.FLOAT32_SCHEMA)
                .field("float64", Schema.FLOAT64_SCHEMA)
                .field("string", Schema.STRING_SCHEMA)
                .field("bytes", Schema.BYTES_SCHEMA)
                .field("bool", Schema.BOOLEAN_SCHEMA)
                .build();
        Struct struct = new Struct(schema)
                .put("int8", Byte.MAX_VALUE)
                .put("int16", Short.MAX_VALUE)
                .put("int32", Integer.MAX_VALUE)
                .put("int64", Long.MAX_VALUE)
                .put("float32", Float.MAX_VALUE)
                .put("float64", Double.MAX_VALUE)
                .put("string", "A string")
                .put("bytes", new byte[] {0x1})
                .put("bool", false);
        Converter connectConverter = new Converter();
        Data convertedRecapData = connectConverter.convert(new SchemaAndValue(struct.schema(), struct));
        SchemaAndValue convertedConnectData = connectConverter.convert(convertedRecapData);
        assertEquals(struct, convertedConnectData.value());
    }

    public void testOptionalPrimitives() {
        Schema schema = SchemaBuilder.struct()
                .field("int8", Schema.OPTIONAL_INT8_SCHEMA)
                .field("int16", Schema.OPTIONAL_INT16_SCHEMA)
                .field("int32", Schema.OPTIONAL_INT32_SCHEMA)
                .field("int64", Schema.OPTIONAL_INT64_SCHEMA)
                .field("float32", Schema.OPTIONAL_FLOAT32_SCHEMA)
                .field("float64", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .field("string", Schema.OPTIONAL_STRING_SCHEMA)
                .field("bytes", Schema.OPTIONAL_BYTES_SCHEMA)
                .field("bool", Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .build();
        // First test with unset values
        Struct struct = new Struct(schema);
        Converter connectConverter = new Converter();
        Data convertedRecapData = connectConverter.convert(new SchemaAndValue(struct.schema(), struct));
        SchemaAndValue convertedConnectData = connectConverter.convert(convertedRecapData);
        assertEquals(struct, convertedConnectData.value());
        // Now test with values set to null
        struct
                .put("int8", null)
                .put("int16", null)
                .put("int32", null)
                .put("int64", null)
                .put("float32", null)
                .put("float64", null)
                .put("string", null)
                .put("bytes", null)
                .put("bool", null);
        convertedRecapData = connectConverter.convert(new SchemaAndValue(struct.schema(), struct));
        convertedConnectData = connectConverter.convert(convertedRecapData);
        assertEquals(struct, convertedConnectData.value());
    }

    public void testList() {
        Schema nestedStructSchema = SchemaBuilder.struct().field("nested", Schema.INT32_SCHEMA).build();
        Schema listSchema = SchemaBuilder.array(nestedStructSchema).build();
        Schema structSchema = SchemaBuilder.struct().field("list", listSchema).build();
        Struct nestedStruct = new Struct(nestedStructSchema).put("nested", 123);
        Struct struct = new Struct(structSchema)
                .put("list", Collections.singletonList(nestedStruct));
        Converter connectConverter = new Converter();
        Data convertedRecapData = connectConverter.convert(new SchemaAndValue(struct.schema(), struct));
        SchemaAndValue convertedConnectData = connectConverter.convert(convertedRecapData);
        assertEquals(struct, convertedConnectData.value());
    }

    public void testMap() {
        Schema nestedStructSchema = SchemaBuilder.struct().field("nested", Schema.INT32_SCHEMA).build();
        Schema mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, nestedStructSchema).build();
        Schema structSchema = SchemaBuilder.struct().field("map", mapSchema).build();
        Struct nestedStruct = new Struct(nestedStructSchema).put("nested", 123);
        Struct struct = new Struct(structSchema)
                .put("map", Collections.singletonMap("test", nestedStruct));
        Converter connectConverter = new Converter();
        Data convertedRecapData = connectConverter.convert(new SchemaAndValue(struct.schema(), struct));
        SchemaAndValue convertedConnectData = connectConverter.convert(convertedRecapData);
        assertEquals(struct, convertedConnectData.value());
    }

    public void testEnum() {
        Type.Enum recapEnumType = new Type.Enum(
                Arrays.asList("red", "green", "blue"),
                null,
                null,
                Collections.singletonMap("name", "enum"));
        Type.Struct recapStructType = new Type.Struct(Collections.singletonList(recapEnumType));
        Map<String, String> structMap = Collections.singletonMap("enum", "red");
        Converter connectConverter = new Converter();
        SchemaAndValue convertedConnectData = connectConverter.convert(new Data(recapStructType, structMap));
        Schema structSchema = SchemaBuilder.struct().field("enum", Schema.STRING_SCHEMA).build();
        Struct struct = new Struct(structSchema).put("enum", "red");
        assertEquals(struct, convertedConnectData.value());
    }

    public void testLogicals() {
        Schema schema = SchemaBuilder.struct()
                .field("date", Date.SCHEMA)
                .field("decimal", Decimal.schema(3))
                .field("time", Time.SCHEMA)
                .field("timestamp", Timestamp.SCHEMA)
                .build();
        Struct struct = new Struct(schema)
                .put("date", new java.util.Date(1234567890))
                .put("decimal", new BigDecimal("123.456"))
                .put("time", new java.util.Date(12345L))
                .put("timestamp", new java.util.Date(12345678901234L));
        Converter connectConverter = new Converter();
        Data convertedRecapData = connectConverter.convert(new SchemaAndValue(struct.schema(), struct));
        SchemaAndValue convertedConnectData = connectConverter.convert(convertedRecapData);
        // Can't do assertEquals(struct, convertedConnectData.value()); because convertedConnectData
        // inherits the `precision` parameter from recap's decimal type, but that parameter isn't in the
        // original struct schema.
        for (Field field : schema.fields()) {
            Type.Struct recapStructType = ((Type.Struct) convertedRecapData.getType());
            Type recapFieldType = recapStructType.getFieldTypes().get(field.index());
            assertTrue(recapFieldType.getLogicalType().startsWith("build.recap."));
            assertEquals(field.schema().name(), convertedConnectData.schema().field(field.name()).schema().name());
            assertEquals(field.schema().type(), convertedConnectData.schema().field(field.name()).schema().type());
            assertEquals(struct.get(field), ((Struct) convertedConnectData.value()).get(field));
        }
    }
}
