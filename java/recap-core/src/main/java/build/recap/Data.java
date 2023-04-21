package build.recap;

import java.util.Objects;

public class Data {
    private final Type type;
    private final Object object;

    public Data(Type type, Object object) {
        this.type = type;
        this.object = object;
    }

    public Type getType() {
        return type;
    }

    public Object getObject() {
        return object;
    }

    @Override
    public String toString() {
        return "Data{" +
                "type=" + type +
                ", object=" + object +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Data data = (Data) o;
        return Objects.equals(type, data.type) && Objects.equals(object, data.object);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, object);
    }
}
