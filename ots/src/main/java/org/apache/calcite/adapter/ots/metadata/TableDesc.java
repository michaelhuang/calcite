package org.apache.calcite.adapter.ots.metadata;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * ots TableDesc with {@link RowKey} and {@link Column}
 *
 * @author huangzf@thfund.com.cn
 * @version 1.0
 * @since 2018-07-13
 */
public class TableDesc implements Serializable {
    private List<RowKey> rowKeys;
    private List<Column> columns;

    public TableDesc(List<RowKey> rowKeys, List<Column> columns) {
        this.rowKeys = rowKeys;
        this.columns = columns;
    }

    public List<RowKey> getRowKeys() {
        return rowKeys;
    }

    public Set<String> pkNames() {
        Set<String> pkNames = new HashSet<>();
        for (RowKey key : rowKeys)
            pkNames.add(key.getName().toLowerCase(Locale.ROOT));
        return pkNames;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public boolean isPrimaryKey(int i) {
        return i >=0 && i < rowKeys.size();
    }

    public RowKey getRowKey(int i) {
        return rowKeys.get(i);
    }

    public Column getColumn(int i) {
        return columns.get(i - rowKeys.size());
    }

    @Override public String toString() {
        final StringBuilder sb = new StringBuilder("TableDesc{");
        sb.append("rowKeys=").append(rowKeys);
        sb.append(", columns=").append(columns);
        sb.append('}');
        return sb.toString();
    }
}
