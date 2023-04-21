package build.recap;

import junit.framework.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TypeTest extends TestCase {
    public void testTypeDescriptions() {
        Type.Bool bool = new Type.Bool(null, null, Map.of("name", "bool"));
        Type.Null null_ = new Type.Null(null, null, Map.of("name", "null"));
        List<Type> fields = new ArrayList<>();
        fields.add(bool);
        fields.add(null_);
        fields.add(new Type.Int(32, true, "build.recap.Time", "A logical type test", Map.of("name", "int32")));
        fields.add(new Type.Float(64, null, null, Map.of("name", "double", "default", 32f)));
        fields.add(new Type.String_(Integer.MAX_VALUE, true, null, null, Map.of("name", "string32")));
        fields.add(new Type.Bytes(Integer.MAX_VALUE, false, null, null, Map.of("name", "bytes32")));
        fields.add(new Type.List_(bool, Integer.MAX_VALUE, true, null, null, Map.of("name", "list32")));
        fields.add(new Type.Map_(bool, bool, null, null, Map.of("name", "map")));
        fields.add(new Type.Enum(List.of("red", "green", "blue"), null, null, Map.of("name", "enum")));
        fields.add(new Type.Union(List.of(null_, bool), null, null, Map.of("name", "union")));
        Type.Struct struct = new Type.Struct(fields);
        Map<String, Object> description = struct.toTypeDescription();
        assertEquals("struct", description.get("type"));
        assertTrue("Struct fields attribute should be a list", description.get("fields") instanceof List<?>);
        List<? extends Map<String, Object>> fieldDescriptions = (List<? extends Map<String, Object>>) description.get("fields");
        assertEquals(fields.size(), fieldDescriptions.size());
        Map<String, Object> boolDescription = Map.of("name", "bool", "type", "bool");
        Map<String, Object> nullDescription = Map.of("name", "null", "type", "null");
        assertEquals(boolDescription, fieldDescriptions.get(0));
        assertEquals(nullDescription, fieldDescriptions.get(1));
        assertEquals(Map.of(
                "name", "int32",
                "type", "int",
                "bits", 32,
                "signed", true,
                "logical", "build.recap.Time",
                "doc", "A logical type test"
        ), fieldDescriptions.get(2));
        assertEquals(Map.of(
                "name", "double",
                "type", "float",
                "bits", 64,
                "default", 32f
        ), fieldDescriptions.get(3));
        assertEquals(Map.of(
                "name", "string32",
                "type", "string",
                "bytes", (long) Integer.MAX_VALUE,
                "variable", true
        ), fieldDescriptions.get(4));
        assertEquals(Map.of(
                "name", "bytes32",
                "type", "bytes",
                "bytes", (long) Integer.MAX_VALUE,
                "variable", false
        ), fieldDescriptions.get(5));
        assertEquals(Map.of(
                "name", "list32",
                "type", "list",
                "length", Integer.MAX_VALUE,
                "variable", true,
                "values", boolDescription
        ), fieldDescriptions.get(6));
        assertEquals(Map.of(
                "name", "map",
                "type", "map",
                "keys", boolDescription,
                "values", boolDescription
        ), fieldDescriptions.get(7));
        assertEquals(Map.of(
                "name", "enum",
                "type", "enum",
                "symbols", List.of("red", "green", "blue")
        ), fieldDescriptions.get(8));
        assertEquals(Map.of(
                "name", "union",
                "type", "union",
                "types", List.of(nullDescription, boolDescription)
        ), fieldDescriptions.get(9));
    }
}
