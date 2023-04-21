package build.recap.kafka.logical;

import build.recap.Data;
import build.recap.Type;
import build.recap.kafka.Converter;
import org.apache.kafka.connect.data.*;

import java.util.Map;

public class DateConverter implements LogicalConverter {
    public static final String LOGICAL_TYPE = "build.recap.Date";
    public static final String UNIT_ATTRIBUTE = "unit";

    public boolean canConvertToConnect(String recapLogicalType) {
        return LOGICAL_TYPE.equals(recapLogicalType);
    }

    public boolean canConvertToRecap(String connectSchemaName, Integer version) {
        return Date.LOGICAL_NAME.equals(connectSchemaName) && Integer.valueOf(1).equals(version);
    }

    public SchemaAndValue convert(SchemaAndValue connectSchemaAndValue) {
        Schema connectSchema = connectSchemaAndValue.schema();
        Object connectValue = connectSchemaAndValue.value();
        assert connectValue instanceof java.util.Date : "Expected date type to be Date, but wasn't.";
        assert connectSchema.version() == 1 : "Expected date logical type version to be 1 but wasn't.";
        SchemaBuilder schemaBuilder = Converter.setStandardAttributes(
                        SchemaBuilder.int32(),
                        new Type(
                                LOGICAL_TYPE,
                                connectSchema.doc(),
                                (Map) connectSchema.parameters()
                        ))
                .parameter(UNIT_ATTRIBUTE, "millisecond");
        if (connectSchema.isOptional()) {
            schemaBuilder
                    .optional()
                    .defaultValue(connectSchema.defaultValue());
        }
        return new SchemaAndValue(schemaBuilder, connectValue);
    }

    public Data convert(Data data) {
        Type.Int recapIntType = (Type.Int) data.getType();
        Object date = data.getObject();
        assert date instanceof java.util.Date : "Expected date type to be Date, but wasn't.";
        Type.Int recapIntTypeWithKCLogical = new Type.Int(
                32,
                true,
                Date.LOGICAL_NAME,
                recapIntType.getDocString(),
                recapIntType.getExtraAttributes()
        );
        return new Data(recapIntTypeWithKCLogical, date);
    }
}
