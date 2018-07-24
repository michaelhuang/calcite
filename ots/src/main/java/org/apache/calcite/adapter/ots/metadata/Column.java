package org.apache.calcite.adapter.ots.metadata;

import org.apache.calcite.adapter.ots.OtsFieldType;

import java.io.Serializable;
import java.util.Objects;

/**
 * ots column to {@link TableDesc}
 *
 * @author huangzf@thfund.com.cn
 * @version 1.0
 * @since 2018-07-13
 */
public class Column implements Serializable {

    private String name;
    private OtsFieldType type;

    public Column(String name, OtsFieldType type) {
        this.name = name;
        this.type = type;
    }

    public Column() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public OtsFieldType getType() {
        return type;
    }

    public void setType(OtsFieldType type) {
        this.type = type;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Column column = (Column) o;
        return Objects.equals(getName(), column.getName());
    }

    @Override public int hashCode() {
        return Objects.hash(getName());
    }

    @Override public String toString() {
        final StringBuilder sb = new StringBuilder("Column{");
        sb.append("name='").append(name).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
