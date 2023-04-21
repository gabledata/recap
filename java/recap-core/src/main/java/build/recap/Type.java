package build.recap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class Type {
    // TODO: Add alias support
    protected String logicalType;
    protected String docString;
    protected Map<String, Object> extraAttributes;

    public Type() {
        this(null, null, null);
    }

    public Type(String logicalType, String docString, Map<String, Object> extraAttributes) {
        this.logicalType = logicalType;
        this.docString = docString;
        this.extraAttributes = extraAttributes != null ? new HashMap<>(extraAttributes) : new HashMap<>();
    }

    public String getLogicalType() {
        return logicalType;
    }

    public String getDocString() {
        return docString;
    }

    public Map<String, Object> getExtraAttributes() {
        return extraAttributes;
    }

    public Map<String, Object> toTypeDescription() {
        Map<String, Object> typeDescription = new HashMap<>();
        if (logicalType != null) {
            typeDescription.put("logical", logicalType);
        }
        if (docString != null) {
            typeDescription.put("doc", docString);
        }
        typeDescription.putAll(extraAttributes);
        return typeDescription;
    }

    @Override
    public String toString() {
        return "Type{" +
                "logicalType='" + logicalType + '\'' +
                ", docString='" + docString + '\'' +
                ", extraAttributes=" + extraAttributes +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Type type = (Type) o;
        return Objects.equals(logicalType, type.logicalType) && Objects.equals(docString, type.docString) && Objects.equals(extraAttributes, type.extraAttributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(logicalType, docString, extraAttributes);
    }

    public static class Bool extends Type {
        public Bool() {
            this(null, null, null);
        }

        public Bool(String logicalType, String docString, Map<String, Object> extraAttributes) {
            super(logicalType, docString, extraAttributes);
        }

        public Map<String, Object> toTypeDescription() {
            Map<String, Object> typeDescription = super.toTypeDescription();
            typeDescription.put("type", "bool");
            return typeDescription;
        }
    }

    public static class Null extends Type {
        public Null() {
            this(null, null, null);
        }

        public Null(String logicalType, String docString, Map<String, Object> extraAttributes) {
            super(logicalType, docString, extraAttributes);
        }

        public Map<String, Object> toTypeDescription() {
            Map<String, Object> typeDescription = super.toTypeDescription();
            typeDescription.put("type", "null");
            return typeDescription;
        }
    }

    public static class Int extends Type {
        private final int bits;
        private final boolean signed;

        public Int(int bits, boolean signed) {
            this(bits, signed, null, null, null);
        }

        public Int(int bits, boolean signed, String logicalType, String docString, Map<String, Object> extraAttributes) {
            super(logicalType, docString, extraAttributes);
            this.bits = bits;
            this.signed = signed;
        }

        public int getBits() {
            return bits;
        }

        public boolean isSigned() {
            return signed;
        }

        public Map<String, Object> toTypeDescription() {
            Map<String, Object> typeDescription = super.toTypeDescription();
            typeDescription.putAll(Map.of(
                    "type", "int",
                    "bits", bits,
                    "signed", signed
            ));
            return typeDescription;
        }

        @Override
        public String toString() {
            return "Int{" +
                    "bits=" + bits +
                    ", signed=" + signed +
                    ", logicalType='" + logicalType + '\'' +
                    ", docString='" + docString + '\'' +
                    ", extraAttributes=" + extraAttributes +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Int anInt = (Int) o;
            return bits == anInt.bits && signed == anInt.signed;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), bits, signed);
        }
    }

    public static class Float extends Type {
        private final int bits;

        public Float(int bits) {
            this(bits, null, null, null);
        }

        public Float(int bits, String logicalType, String docString, Map<String, Object> extraAttributes) {
            super(logicalType, docString, extraAttributes);
            this.bits = bits;
        }

        public int getBits() {
            return bits;
        }

        public Map<String, Object> toTypeDescription() {
            Map<String, Object> typeDescription = super.toTypeDescription();
            typeDescription.putAll(Map.of(
                    "type", "float",
                    "bits", bits
            ));
            return typeDescription;
        }

        @Override
        public String toString() {
            return "Float{" +
                    "bits=" + bits +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Float aFloat = (Float) o;
            return bits == aFloat.bits;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), bits);
        }
    }

    public static class String_ extends Type {
        private final long bytes;
        private final boolean variable;

        public String_(long bytes, boolean variable) {
            this(bytes, variable, null, null, null);
        }

        public String_(long bytes, boolean variable, String logicalType, String docString, Map<String, Object> extraAttributes) {
            super(logicalType, docString, extraAttributes);
            this.bytes = bytes;
            this.variable = variable;
        }

        public long getBytes() {
            return bytes;
        }

        public boolean isVariable() {
            return variable;
        }

        public Map<String, Object> toTypeDescription() {
            Map<String, Object> typeDescription = super.toTypeDescription();
            typeDescription.putAll(Map.of(
                    "type", "string",
                    "bytes", bytes,
                    "variable", variable
            ));
            return typeDescription;
        }

        @Override
        public String toString() {
            return "String_{" +
                    "bytes=" + bytes +
                    ", variable=" + variable +
                    ", logicalType='" + logicalType + '\'' +
                    ", docString='" + docString + '\'' +
                    ", extraAttributes=" + extraAttributes +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            String_ string = (String_) o;
            return bytes == string.bytes && variable == string.variable;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), bytes, variable);
        }
    }

    public static class Bytes extends Type {
        private final long bytes;
        private final boolean variable;

        public Bytes(long bytes, boolean variable) {
            this(bytes, variable, null, null, null);
        }

        public Bytes(long bytes, boolean variable, String logicalType, String docString, Map<String, Object> extraAttributes) {
            super(logicalType, docString, extraAttributes);
            this.bytes = bytes;
            this.variable = variable;
        }

        public long getBytes() {
            return bytes;
        }

        public boolean isVariable() {
            return variable;
        }

        public Map<String, Object> toTypeDescription() {
            Map<String, Object> typeDescription = super.toTypeDescription();
            typeDescription.putAll(Map.of(
                    "type", "bytes",
                    "bytes", bytes,
                    "variable", variable
            ));
            return typeDescription;
        }

        @Override
        public String toString() {
            return "Bytes{" +
                    "bytes=" + bytes +
                    ", variable=" + variable +
                    ", logicalType='" + logicalType + '\'' +
                    ", docString='" + docString + '\'' +
                    ", extraAttributes=" + extraAttributes +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Bytes bytes1 = (Bytes) o;
            return bytes == bytes1.bytes && variable == bytes1.variable;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), bytes, variable);
        }
    }

    public static class List_ extends Type {
        private final Type valueType;
        private final Integer length;
        private final boolean variable;

        public List_(Type valueType) {
            this(valueType, null, true);
        }

        public List_(Type valueType, boolean variable) {
            this(valueType, null, variable);
        }

        public List_(Type valueType, Integer length, boolean variable) {
            this(valueType, length, variable, null, null, null);
        }

        public List_(Type valueType, Integer length, boolean variable, String logicalType, String docString, Map<String, Object> extraAttributes) {
            super(logicalType, docString, extraAttributes);
            this.valueType = valueType;
            this.length = length;
            this.variable = variable;
        }

        public Type getValueType() {
            return valueType;
        }

        public Integer getLength() {
            return length;
        }

        public boolean isVariable() {
            return variable;
        }

        public Map<String, Object> toTypeDescription() {
            Map<String, Object> typeDescription = super.toTypeDescription();
            typeDescription.putAll(Map.of(
                    "type", "list",
                    "variable", variable,
                    "values", valueType.toTypeDescription()
            ));
            if (length != null) {
                typeDescription.put("length", length);
            }
            return typeDescription;
        }

        @Override
        public String toString() {
            return "List_{" +
                    "valueType=" + valueType +
                    ", length=" + length +
                    ", variable=" + variable +
                    ", logicalType='" + logicalType + '\'' +
                    ", docString='" + docString + '\'' +
                    ", extraAttributes=" + extraAttributes +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            List_ list = (List_) o;
            return length == list.length && variable == list.variable && Objects.equals(valueType, list.valueType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), valueType, length, variable);
        }
    }

    public static class Map_ extends Type {
        private final Type keyType;
        private final Type valueType;

        public Map_(Type keyType, Type valueType) {
            this(keyType, valueType, null, null, null);
        }

        public Map_(Type keyType, Type valueType, String logicalType, String docString, Map<String, Object> extraAttributes) {
            super(logicalType, docString, extraAttributes);
            this.keyType = keyType;
            this.valueType = valueType;
        }

        public Type getKeyType() {
            return keyType;
        }

        public Type getValueType() {
            return valueType;
        }

        public Map<String, Object> toTypeDescription() {
            Map<String, Object> typeDescription = super.toTypeDescription();
            typeDescription.putAll(Map.of(
                    "type", "map",
                    "keys", keyType.toTypeDescription(),
                    "values", valueType.toTypeDescription()
            ));
            return typeDescription;
        }

        @Override
        public String toString() {
            return "Map_{" +
                    "keyType=" + keyType +
                    ", valueType=" + valueType +
                    ", logicalType='" + logicalType + '\'' +
                    ", docString='" + docString + '\'' +
                    ", extraAttributes=" + extraAttributes +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Map_ map = (Map_) o;
            return Objects.equals(keyType, map.keyType) && Objects.equals(valueType, map.valueType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), keyType, valueType);
        }
    }

    public static class Struct extends Type {
        List<? extends Type> fieldTypes;

        public Struct(List<? extends Type> fieldTypes) {
            this(fieldTypes, null, null, null);
        }

        public Struct(List<? extends Type> fieldTypes, String logicalType, String docString, Map<String, Object> extraAttributes) {
            super(logicalType, docString, extraAttributes);
            this.fieldTypes = fieldTypes;
        }

        public List<? extends Type> getFieldTypes() {
            return fieldTypes;
        }

        public Map<String, Object> toTypeDescription() {
            Map<String, Object> typeDescription = super.toTypeDescription();
            typeDescription.putAll(Map.of(
                    "type", "struct",
                    "fields", fieldTypes
                            .stream()
                            .map(Type::toTypeDescription)
                            .collect(Collectors.toList())
            ));
            return typeDescription;
        }

        @Override
        public String toString() {
            return "Struct{" +
                    "fieldTypes=" + fieldTypes +
                    ", logicalType='" + logicalType + '\'' +
                    ", docString='" + docString + '\'' +
                    ", extraAttributes=" + extraAttributes +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Struct struct = (Struct) o;
            return Objects.equals(fieldTypes, struct.fieldTypes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), fieldTypes);
        }
    }

    public static class Enum extends Type {
        private final List<String> symbols;

        public Enum(List<String> symbols) {
            this(symbols, null, null, null);
        }

        public Enum(List<String> symbols, String logicalType, String docString, Map<String, Object> extraAttributes) {
            super(logicalType, docString, extraAttributes);
            this.symbols = symbols;
        }

        public List<String> getSymbols() {
            return symbols;
        }

        public Map<String, Object> toTypeDescription() {
            Map<String, Object> typeDescription = super.toTypeDescription();
            typeDescription.putAll(Map.of(
                    "type", "enum",
                    "symbols", symbols
            ));
            return typeDescription;
        }

        @Override
        public String toString() {
            return "Enum{" +
                    "symbols=" + symbols +
                    ", logicalType='" + logicalType + '\'' +
                    ", docString='" + docString + '\'' +
                    ", extraAttributes=" + extraAttributes +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Enum anEnum = (Enum) o;
            return Objects.equals(symbols, anEnum.symbols);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), symbols);
        }
    }

    public static class Union extends Type {
        private final List<? extends Type> types;

        public Union(List<? extends Type> types) {
            this(types, null, null, null);
        }

        public Union(List<? extends Type> types, String logicalType, String docString, Map<String, Object> extraAttributes) {
            super(logicalType, docString, extraAttributes);
            this.types = types;
        }

        public List<? extends Type> getTypes() {
            return types;
        }

        public Map<String, Object> toTypeDescription() {
            Map<String, Object> typeDescription = super.toTypeDescription();
            typeDescription.putAll(Map.of(
                    "type", "union",
                    "types", types
                            .stream()
                            .map(Type::toTypeDescription)
                            .collect(Collectors.toList())
            ));
            return typeDescription;
        }

        @Override
        public String toString() {
            return "Union{" +
                    "types=" + types +
                    ", logicalType='" + logicalType + '\'' +
                    ", docString='" + docString + '\'' +
                    ", extraAttributes=" + extraAttributes +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Union union = (Union) o;
            return Objects.equals(types, union.types);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), types);
        }
    }
}