package org.apache.calcite.adapter.ots.metadata;

import org.apache.calcite.adapter.ots.OtsFieldType;

import java.io.Serializable;
import java.util.Objects;

/**
 * ots rowkey to {@link TableDesc}
 *
 * @author huangzf@thfund.com.cn
 * @version 1.0
 * @since 2018-07-13
 */
public class RowKey implements Serializable {

    private String name;
    private OtsFieldType type;

    public RowKey(String name, OtsFieldType type) {
        this.name = name;
        this.type = type;
    }

    public RowKey() {
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

    @Override public String toString() {
        final StringBuilder sb = new StringBuilder("RowKey{");
        sb.append("name='").append(name).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append('}');
        return sb.toString();
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        RowKey rowKey = (RowKey) o;
        return Objects.equals(getName(), rowKey.getName());
    }

    @Override public int hashCode() {
        return Objects.hash(getName());
    }
}
