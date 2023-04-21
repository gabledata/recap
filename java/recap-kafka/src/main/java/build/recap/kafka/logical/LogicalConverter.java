package build.recap.kafka.logical;

import build.recap.Data;
import org.apache.kafka.connect.data.SchemaAndValue;

public interface LogicalConverter {
    boolean canConvertToConnect(String recapLogicalType);
    boolean canConvertToRecap(String connectSchemaName, Integer version);
    SchemaAndValue convert(SchemaAndValue connectSchemaAndValue);
    Data convert(Data data);
}
