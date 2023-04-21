package build.recap.kafka.logical;

import build.recap.Data;
import build.recap.Type;
import build.recap.kafka.Converter;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.math.BigDecimal;
import java.util.Map;

public class DecimalConverter implements LogicalConverter {
    public static final String LOGICAL_TYPE = "build.recap.Decimal";
    public static final String PRECISION_ATTRIBUTE = "precision";

    public boolean canConvertToConnect(String recapLogicalType) {
        return LOGICAL_TYPE.equals(recapLogicalType);
    }

    public boolean canConvertToRecap(String connectSchemaName, Integer version) {
        return Decimal.LOGICAL_NAME.equals(connectSchemaName) && Integer.valueOf(1).equals(version);
    }

    public SchemaAndValue convert(SchemaAndValue connectSchemaAndValue) {
        Schema connectSchema = connectSchemaAndValue.schema();
        Object connectValue = connectSchemaAndValue.value();
        assert connectValue instanceof BigDecimal : "Expected decimal type to be BigDecimal, but wasn't.";
        assert connectSchema.version() == 1 : "Expected decimal logical type version to be 1 but wasn't.";
        SchemaBuilder schemaBuilder = Converter.setStandardAttributes(
                SchemaBuilder.bytes(),
                new Type(
                    LOGICAL_TYPE,
                    connectSchema.doc(),
                    (Map) connectSchema.parameters()
            ))
            // BigDecimals are capped to a 2 gig limit in Java
            .parameter(PRECISION_ATTRIBUTE, Integer.toString(Integer.MAX_VALUE));
        if (connectSchema.isOptional()) {
            schemaBuilder
                    .optional()
                    .defaultValue(connectSchema.defaultValue());
        }
        return new SchemaAndValue(schemaBuilder, connectValue);
    }

    public Data convert(Data data) {
        Type.Bytes recapBytesType = (Type.Bytes) data.getType();
        Object decimal = data.getObject();
        assert decimal instanceof BigDecimal : "Expected decimal type to be BigDecimal, but wasn't.";
        Type.Bytes recapBytesTypeWithKCLogical = new Type.Bytes(
                recapBytesType.getBytes(),
                recapBytesType.isVariable(),
                Decimal.LOGICAL_NAME,
                recapBytesType.getDocString(),
                recapBytesType.getExtraAttributes()
        );
        return new Data(recapBytesTypeWithKCLogical, decimal);
    }
}
