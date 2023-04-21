package build.recap.kafka;

import build.recap.Data;
import build.recap.Type;
import build.recap.kafka.logical.DateConverter;
import build.recap.kafka.logical.DecimalConverter;
import build.recap.kafka.logical.LogicalConverter;
import build.recap.kafka.logical.TimeConverter;
import build.recap.kafka.logical.TimestampConverter;
import org.apache.kafka.connect.data.*;

import java.util.*;
import java.util.stream.Collectors;

public class Converter {
    private final List<LogicalConverter> logicalConverters;

    public Converter() {
        this(List.of(
                new DecimalConverter(),
                new DateConverter(),
                new TimeConverter(),
                new TimestampConverter()
        ));
    }

    public Converter(List<LogicalConverter> logicalConverters) {
        this.logicalConverters = Collections.unmodifiableList(logicalConverters);
    }

    public Data convert(SchemaAndValue connectSchemaAndValue) {
        Data convertedData = null;
        SchemaAndValue connectSchemaAndValueLogical = convertLogical(connectSchemaAndValue);
        Schema connectSchema = connectSchemaAndValue.schema();

        switch (connectSchema.type()) {
            case STRING:
                convertedData = convertString(connectSchemaAndValueLogical);
                break;
            case INT8:
            case INT16:
            case INT32:
            case INT64:
                convertedData = convertInt(connectSchemaAndValueLogical);
                break;
            case FLOAT32:
            case FLOAT64:
                convertedData = convertFloat(connectSchemaAndValueLogical);
                break;
            case BOOLEAN:
                convertedData = convertBool(connectSchemaAndValueLogical);
                break;
            case BYTES:
                convertedData = convertBytes(connectSchemaAndValueLogical);
                break;
            case STRUCT:
                convertedData = convertStruct((Struct) connectSchemaAndValueLogical.value());
                break;
            case ARRAY:
                convertedData = convertList(connectSchemaAndValueLogical);
                break;
            case MAP:
                convertedData = convertMap(connectSchemaAndValueLogical);
                break;
        }

        if (convertedData != null) {
            if (connectSchema.isOptional()) {
                Type optionalType = new Type.Union(Arrays.asList(new Type.Null(), convertedData.getType()));
                // KC does not differentiate between null default and unset default.
                // Always set default even if it's null, since we don't know if it's explicitly so or not.
                optionalType.getExtraAttributes().put("default", connectSchema.defaultValue());
                convertedData = new Data(optionalType, convertedData.getObject());
            }
            return convertedData;
        }

        throw new RuntimeException("Unable to convert Kafka Connect data " + connectSchemaAndValue);
    }

    public SchemaAndValue convert(Data data) {
        Data dataLogical = convertLogical(data);

        if (dataLogical.getType() instanceof Type.String_) {
            return convertString(dataLogical);
        } else if (dataLogical.getType() instanceof Type.Int) {
            return convertInt(dataLogical);
        } else if (dataLogical.getType() instanceof Type.Float) {
            return convertFloat(dataLogical);
        } else if (dataLogical.getType() instanceof Type.Bytes) {
            return convertBytes(dataLogical);
        } else if (dataLogical.getType() instanceof Type.Bool) {
            return convertBool(dataLogical);
        } else if (dataLogical.getType() instanceof Type.Struct) {
            return convertStruct(dataLogical);
        } else if (dataLogical.getType() instanceof Type.List_) {
            return convertList(dataLogical);
        } else if (dataLogical.getType() instanceof Type.Map_) {
            return convertMap(dataLogical);
        } else if (dataLogical.getType() instanceof Type.Enum) {
            return convertEnum(dataLogical);
        } else if (dataLogical.getType() instanceof Type.Union) {
            return convertUnion(dataLogical);
        }
        throw new UnsupportedOperationException("Unable to convert Recap data " + data);
    }

    protected Data convertString(SchemaAndValue connectSchemaAndValue) {
        Schema connectSchema = connectSchemaAndValue.schema();
        Object connectValue = connectSchemaAndValue.value();
        Type.String_ recapStringType = new Type.String_(
                Integer.MAX_VALUE,
                true,
                connectSchema.name(),
                connectSchema.doc(),
                (Map) connectSchema.parameters()
        );
        return new Data(recapStringType, connectValue);
    }

    protected SchemaAndValue convertString(Data data) {
        Type.String_ recapStringType = (Type.String_) data.getType();
        Schema connectSchema = setStandardAttributes(SchemaBuilder.string(), recapStringType).build();
        if (recapStringType.getBytes() <= Integer.MAX_VALUE) {
            return new SchemaAndValue(
                    connectSchema,
                    data.getObject()
            );
        }
        throw new RuntimeException("Unable to convert Recap string " + data);
    }

    protected Data convertInt(SchemaAndValue connectSchemaAndValue) {
        Schema connectSchema = connectSchemaAndValue.schema();
        Object connectValue = connectSchemaAndValue.value();
        Integer bits = null;
        switch (connectSchema.type()) {
            case INT8:
                bits = 8;
                break;
            case INT16:
                bits = 16;
                break;
            case INT32:
                bits = 32;
                break;
            case INT64:
                bits = 64;
                break;
        }
        if (bits != null) {
            Type.Int recapIntType = new Type.Int(
                    bits,
                    true,
                    connectSchema.name(),
                    connectSchema.doc(),
                    (Map) connectSchema.parameters()
            );
            return new Data(recapIntType, connectValue);
        }
        throw new RuntimeException("Unable to convert Kafka Connect integer " + connectSchemaAndValue);
    }

    protected SchemaAndValue convertInt(Data data) {
        Type.Int recapIntType = (Type.Int) data.getType();
        if (recapIntType.getBits() <= 8) {
            if (recapIntType.isSigned()) {
                return new SchemaAndValue(
                        setStandardAttributes(SchemaBuilder.int8(), recapIntType).build(),
                        data.getObject());
            } else {
                return new SchemaAndValue(
                        setStandardAttributes(SchemaBuilder.int16(), recapIntType).build(),
                        data.getObject());
            }
        } else if (recapIntType.getBits() <= 16) {
            if (recapIntType.isSigned()) {
                return new SchemaAndValue(
                        setStandardAttributes(SchemaBuilder.int16(), recapIntType).build(),
                        data.getObject());
            } else {
                return new SchemaAndValue(
                        setStandardAttributes(SchemaBuilder.int32(), recapIntType).build(),
                        data.getObject());
            }
        } else if (recapIntType.getBits() <= 32) {
            if (recapIntType.isSigned()) {
                return new SchemaAndValue(
                        setStandardAttributes(SchemaBuilder.int32(), recapIntType).build(),
                        data.getObject());
            } else {
                return new SchemaAndValue(
                        setStandardAttributes(SchemaBuilder.int64(), recapIntType).build(),
                        data.getObject());
            }
        } else if (recapIntType.getBits() <= 64) {
            if (recapIntType.isSigned()) {
                return new SchemaAndValue(
                        setStandardAttributes(SchemaBuilder.int64(), recapIntType).build(),
                        data.getObject());
            } else {
                return new SchemaAndValue(
                        setStandardAttributes(Decimal.builder(0), recapIntType).build(),
                        data.getObject());
            }
        }
        throw new RuntimeException("Unable to convert Recap integer " + data);
    }

    protected Data convertFloat(SchemaAndValue connectSchemaAndValue) {
        Schema connectSchema = connectSchemaAndValue.schema();
        Object connectValue = connectSchemaAndValue.value();
        Integer bits = null;
        switch (connectSchema.type()) {
            case FLOAT32:
                bits = 32;
                break;
            case FLOAT64:
                bits = 64;
                break;
        }
        if (bits != null) {
            Type.Float recapFloatType = new Type.Float(
                    bits,
                    connectSchema.name(),
                    connectSchema.doc(),
                    (Map) connectSchema.parameters()
            );
            return new Data(recapFloatType, connectValue);
        }
        throw new RuntimeException("Unable to convert Kafka Connect float " + connectSchemaAndValue);
    }

    protected SchemaAndValue convertFloat(Data data) {
        Type.Float recapFloatType = (Type.Float) data.getType();
        if (recapFloatType.getBits() <= 32) {
            return new SchemaAndValue(
                    setStandardAttributes(SchemaBuilder.float32(), recapFloatType).build(),
                    data.getObject());
        } else if (recapFloatType.getBits() <= 64) {
            return new SchemaAndValue(
                    setStandardAttributes(SchemaBuilder.float64(), recapFloatType).build(),
                    data.getObject());
        }
        throw new RuntimeException("Unable to convert Recap float " + data);
    }

    protected Data convertBytes(SchemaAndValue connectSchemaAndValue) {
        Schema connectSchema = connectSchemaAndValue.schema();
        Object connectValue = connectSchemaAndValue.value();
        Type.Bytes recapBytesType = new Type.Bytes(
                Integer.MAX_VALUE,
                true,
                connectSchema.name(),
                connectSchema.doc(),
                (Map) connectSchema.parameters()
        );
        return new Data(recapBytesType, connectValue);
    }

    protected SchemaAndValue convertBytes(Data data) {
        Type.Bytes recapBytesType = (Type.Bytes) data.getType();
        if (recapBytesType.getBytes() <= Integer.MAX_VALUE) {
            return new SchemaAndValue(
                    setStandardAttributes(SchemaBuilder.bytes(), recapBytesType).build(),
                    data.getObject());
        }
        throw new RuntimeException("Unable to convert Recap bytes " + data);
    }

    protected Data convertBool(SchemaAndValue connectSchemaAndValue) {
        Schema connectSchema = connectSchemaAndValue.schema();
        Object connectValue = connectSchemaAndValue.value();
        Type.Bool recapBoolType = new Type.Bool(
                connectSchema.name(),
                connectSchema.doc(),
                (Map) connectSchema.parameters()
        );
        return new Data(recapBoolType, connectValue);
    }

    protected SchemaAndValue convertBool(Data data) {
        return new SchemaAndValue(
                setStandardAttributes(SchemaBuilder.bool(), data.getType()).build(),
                data.getObject());
    }

    protected Data convertStruct(Struct connectStruct) {
        // TODO: Need to handle null structs here.
        Schema connectSchema = connectStruct.schema();
        Type[] recapFieldTypes = new Type[connectSchema.fields().size()];
        Map<Object, Object> recapStructFields = new LinkedHashMap<>();
        for (Field connectField : connectSchema.fields()) {
            Object connectFieldObject = connectStruct.get(connectField);
            Data fieldData = convert(new SchemaAndValue(connectField.schema(), connectFieldObject));
            recapFieldTypes[connectField.index()] = fieldData.getType();
            recapFieldTypes[connectField.index()].getExtraAttributes().put("name", connectField.name());
            recapStructFields.put(connectField.name(), fieldData.getObject());
        }
        Type.Struct recapStructType = new Type.Struct(
                Arrays.asList(recapFieldTypes),
                connectSchema.name(),
                connectSchema.doc(),
                (Map) connectSchema.parameters()
        );
        return new Data(recapStructType, recapStructFields);
    }

    protected SchemaAndValue convertStruct(Data data) {
        Map<String, Object> connectStructFields = new LinkedHashMap<>();
        Map<Object, Object> recapStructFields = (Map<Object, Object>) data.getObject();
        Type.Struct recapStructType = (Type.Struct) data.getType();
        SchemaBuilder connectSchemaBuilder = setStandardAttributes(SchemaBuilder.struct(), data.getType());
        for (Type recapFieldType : recapStructType.getFieldTypes()) {
            String fieldName = (String) recapFieldType.getExtraAttributes().get("name");
            assert fieldName != null : "Can't convert unnamed field to Kafka Connect field";
            // Allow null objects on non-null types like structs, so we can convert schemas for optional types.
            Object recapFieldObject = recapStructFields != null ? recapStructFields.get(fieldName) : null;
            Data fieldData = new Data(recapFieldType, recapFieldObject);
            SchemaAndValue fieldSchemaAndValue = convert(fieldData);
            connectStructFields.put(fieldName, fieldSchemaAndValue.value());
            connectSchemaBuilder.field(fieldName, fieldSchemaAndValue.schema());
        }
        Struct connectStruct = new Struct(connectSchemaBuilder.build());
        for (Map.Entry<String, Object> connectStructField : connectStructFields.entrySet()) {
            connectStruct.put(connectStructField.getKey(), connectStructField.getValue());
        }
        return new SchemaAndValue(connectStruct.schema(), connectStruct);
    }

    protected Data convertList(SchemaAndValue connectSchemaAndValue) {
        Schema connectSchema = connectSchemaAndValue.schema();
        Schema connectValueSchema = connectSchema.valueSchema();
        List<Object> connectList = connectSchemaAndValue.value() != null ? (List<Object>) connectSchemaAndValue.value() : Collections.emptyList();
        List<Object> recapList = new ArrayList<>(connectList.size());
        Type recapValueType = null;
        for (Object valueObject : connectList) {
            Data data = convert(new SchemaAndValue(connectValueSchema, valueObject));
            recapValueType = data.getType();
            recapList.add(data.getObject());
        }
        if (recapValueType == null) {
            // Assume an empty list or null list, so send a null object to get schema.
            recapValueType = convert(new SchemaAndValue(connectValueSchema, null)).getType();
        }
        Type.List_ recapListType = new Type.List_(
                recapValueType,
                null,
                true,
                connectSchema.name(),
                connectSchema.doc(),
                (Map) connectSchema.parameters()
        );
        return new Data(recapListType, recapList);
    }

    protected SchemaAndValue convertList(Data data) {
        Type.List_ recapListType = (Type.List_) data.getType();
        Type recapValueType = recapListType.getValueType();
        List<Object> recapList = data.getObject() != null ? (List<Object>) data.getObject() : Collections.emptyList();
        List<Object> connectList = new ArrayList<>(recapList.size());
        Schema connectValueSchema = null;
        // Kafka Connect has no concept of fixed-size or length-limited arrays.
        // Put all Recap lists into KC's standard array.
        for (Object valueObject : recapList) {
            SchemaAndValue schemaAndValue = convert(new Data(recapValueType, valueObject));
            connectValueSchema = schemaAndValue.schema();
            connectList.add(schemaAndValue.value());
        }
        if (connectValueSchema == null) {
            // Assume an empty list or null list, so send a null object to get schema.
            connectValueSchema = convert(new Data(recapValueType, null)).schema();
        }
        Schema connectListSchema = setStandardAttributes(SchemaBuilder.array(connectValueSchema), data.getType()).build();
        return new SchemaAndValue(connectListSchema, connectList);
    }

    protected Data convertMap(SchemaAndValue connectSchemaAndValue) {
        Schema connectSchema = connectSchemaAndValue.schema();
        Schema connectKeySchema = connectSchema.keySchema();
        Schema connectValueSchema = connectSchema.valueSchema();
        Map<Object, Object> connectMap = connectSchemaAndValue.value() != null ? (Map<Object, Object>) connectSchemaAndValue.value() : Collections.emptyMap();
        Map<Object, Object> recapMap = new LinkedHashMap<>(connectMap.size());
        Type recapKeyType = null;
        Type recapValueType = null;
        for (Map.Entry<Object, Object> mapEntry : connectMap.entrySet()) {
            Data keyData = convert(new SchemaAndValue(connectKeySchema, mapEntry.getKey()));
            Data valueData = convert(new SchemaAndValue(connectValueSchema, mapEntry.getValue()));
            recapKeyType = keyData.getType();
            recapValueType = valueData.getType();
            recapMap.put(keyData.getObject(), valueData.getObject());
        }
        if (recapKeyType == null || recapValueType == null) {
            assert recapValueType == recapKeyType : "Got null key type or value type, but not both. This is unexpected.";
            // Assume an empty map or null map, so send a null object to get schemas.
            recapKeyType = convert(new SchemaAndValue(connectKeySchema, null)).getType();
            recapValueType = convert(new SchemaAndValue(connectValueSchema, null)).getType();
        }
        Type.Map_ recapMapType = new Type.Map_(
                recapKeyType,
                recapValueType,
                connectSchema.name(),
                connectSchema.doc(),
                (Map) connectSchema.parameters()
        );
        return new Data(recapMapType, recapMap);
    }

    protected SchemaAndValue convertMap(Data data) {
        Type.Map_ recapMapType = (Type.Map_) data.getType();
        Type recapKeyType = recapMapType.getKeyType();
        Type recapValueType = recapMapType.getValueType();
        Map<Object, Object> recapMap = data.getObject() != null ? (Map<Object, Object>) data.getObject() : Collections.emptyMap();
        Map<Object, Object> connectMap = new LinkedHashMap<>(recapMap.size());
        Schema connectKeySchema = null;
        Schema connectValueSchema = null;
        for (Map.Entry<Object, Object> mapEntry : recapMap.entrySet()) {
            SchemaAndValue keySchemaAndValue = convert(new Data(recapKeyType, mapEntry.getKey()));
            SchemaAndValue valueSchemaAndValue = convert(new Data(recapValueType, mapEntry.getValue()));
            connectKeySchema = keySchemaAndValue.schema();
            connectValueSchema = valueSchemaAndValue.schema();
            connectMap.put(keySchemaAndValue.value(), valueSchemaAndValue.value());
        }
        if (connectKeySchema == null || connectValueSchema == null) {
            assert recapValueType == recapKeyType : "Got null key type or value type, but not both. This is unexpected.";
            // Assume an empty map or null map, so send a null object to get schemas.
            connectKeySchema = convert(new Data(recapKeyType, null)).schema();
            connectValueSchema = convert(new Data(recapValueType, null)).schema();
        }
        Schema connectMapSchema = setStandardAttributes(SchemaBuilder.map(connectKeySchema, connectValueSchema), data.getType()).build();
        return new SchemaAndValue(connectMapSchema, connectMap);
    }

    protected SchemaAndValue convertEnum(Data data) {
        Schema connectSchema = setStandardAttributes(SchemaBuilder.string(), data.getType()).build();
        return new SchemaAndValue(connectSchema, data.getObject());
    }

    protected SchemaAndValue convertUnion(Data data) {
        Type.Union recapUnionType = (Type.Union) data.getType();
        int recapNullTypeIndex = recapUnionType.getTypes().indexOf(new Type.Null());
        if (recapNullTypeIndex >= 0 && recapUnionType.getTypes().size() == 2) {
            Type recapNonNullType = recapUnionType.getTypes().get(1 - recapNullTypeIndex);
            SchemaAndValue schemaAndValue = convert(new Data(recapNonNullType, data.getObject()));
            SchemaBuilder schemaBuilder = null;
            // TODO: Figure out a better way to clone a schema and make it optional.
            if (schemaAndValue.schema().type().equals(Schema.Type.ARRAY)) {
                schemaBuilder = SchemaBuilder.array(schemaAndValue.schema().valueSchema());
            } else if (schemaAndValue.schema().type().equals(Schema.Type.MAP)) {
                schemaBuilder = SchemaBuilder.map(
                        schemaAndValue.schema().keySchema(),
                        schemaAndValue.schema().valueSchema());
            } else if (schemaAndValue.schema().type().equals(Schema.Type.STRUCT)) {
                schemaBuilder = SchemaBuilder.struct();
                for (Field structField : schemaAndValue.schema().fields()) {
                    schemaBuilder.field(structField.name(), structField.schema());
                }
            } else {
                schemaBuilder = SchemaBuilder.type(schemaAndValue.schema().type());
            }
            Schema optionalConnectSchema = setStandardAttributes(schemaBuilder, data.getType())
                    .optional()
                    .defaultValue(data.getType().getExtraAttributes().get("default"))
                    .build();
            return new SchemaAndValue(optionalConnectSchema, schemaAndValue.value());
        }
        throw new RuntimeException("Unable to convert Recap union " + data);
    }

    protected SchemaAndValue convertLogical(SchemaAndValue schemaAndValue) {
        Schema connectSchema = schemaAndValue.schema();
        if (connectSchema.name() != null) {
            for (LogicalConverter logicalConverter : logicalConverters) {
                if (logicalConverter.canConvertToRecap(connectSchema.name(), connectSchema.version())) {
                    return logicalConverter.convert(schemaAndValue);
                }
            }
        }
        return schemaAndValue;
    }

    protected Data convertLogical(Data data) {
        String recapLogicalType = data.getType().getLogicalType();
        if (recapLogicalType != null) {
            for (LogicalConverter logicalConverter : logicalConverters) {
                if (logicalConverter.canConvertToConnect(recapLogicalType)) {
                    return logicalConverter.convert(data);
                }
            }
        }
        return data;
    }

    public static SchemaBuilder setStandardAttributes(SchemaBuilder schemaBuilder, Type recapType) {
        return schemaBuilder
                .name(recapType.getLogicalType())
                .doc(recapType.getDocString())
                .parameters(convertExtraAttributes(recapType.getExtraAttributes()));
    }

    public static Map<String, String> convertExtraAttributes(Map<String, Object> recapExtraAttributes) {
        if (recapExtraAttributes != null) {
            return recapExtraAttributes
                    .entrySet()
                    .stream()
                    // Remove reserved field attributes (name and default)
                    .filter(e -> e.getValue() != null
                            && !"name".equals(e.getKey())
                            && !"default".equals(e.getKey()))
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> e.getValue().toString()));
        }
        return null;
    }
}