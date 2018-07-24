package org.apache.calcite.adapter.ots;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.filter.ColumnValueFilter;
import com.google.common.collect.Multimap;
import org.apache.calcite.adapter.ots.metadata.TableDesc;
import org.apache.calcite.linq4j.Enumerator;

import java.util.*;

/**
 * Enumerator
 * <p>
 *     初始化ots原生api, 执行scan操作
 *
 * @author huangzf@thfund.com.cn
 * @version 1.0
 * @since 2018-07-13
 */
public class OtsEnumerator<E> implements Enumerator<E> {

    final SyncClient client;
    final String tableName;

    Iterator<Row> iterator;
    private E current;
    final ColumnValueFilter columnValueFilter;
    final Multimap<Integer, String> primaryKeys;
    final TableDesc tableDesc;

    public OtsEnumerator(SyncClient client, String tableName,
        ColumnValueFilter columnValueFilter,
        Multimap<Integer, String> primaryKeys, TableDesc tableDesc) {

        this.client = client;
        this.tableName = tableName;
        this.columnValueFilter = columnValueFilter;
        this.primaryKeys = primaryKeys;
        this.tableDesc = tableDesc;

        RangeIteratorParameter rangeIteratorParameter = new RangeIteratorParameter(tableName);

        // 设置起始主键
        PrimaryKeyBuilder pkBegin = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        PrimaryKeyBuilder pkEnd = PrimaryKeyBuilder.createPrimaryKeyBuilder();

        Set<String> usedPks = new HashSet<>();
        for (Map.Entry<Integer, Collection<String>> pk: primaryKeys.asMap().entrySet()) {
            String pkName = tableDesc.getRowKey(pk.getKey()).getName().toLowerCase(Locale.ROOT);
            OtsFieldType pkType = tableDesc.getRowKey(pk.getKey()).getType();
            List<String> pkValue = (List<String>) pk.getValue();
            pkBegin.addPrimaryKeyColumn(pkName, pkType.toPK(pkValue.get(0)));
            // pk的between .. and.. 处理
            if (pkValue.size() > 1) {
                pkEnd.addPrimaryKeyColumn(pkName, pkType.toPK(pkValue.get(1)));
            } else {
                pkEnd.addPrimaryKeyColumn(pkName, pkType.toPK(pkValue.get(0)));
            }
            usedPks.add(pkName);
        }
        // 遗留PK处理
        Set<String> pkNames = tableDesc.pkNames();
        pkNames.removeAll(usedPks);
        Iterator<String> iterator = pkNames.iterator();
        while (iterator.hasNext()) {
            String pkName = iterator.next().toLowerCase(Locale.ROOT);
            pkBegin.addPrimaryKeyColumn(pkName, PrimaryKeyValue.INF_MIN);
            pkEnd.addPrimaryKeyColumn(pkName, PrimaryKeyValue.INF_MAX);
        }

        rangeIteratorParameter.setInclusiveStartPrimaryKey(pkBegin.build());
        rangeIteratorParameter.setExclusiveEndPrimaryKey(pkEnd.build());
        if (null != columnValueFilter) {
            rangeIteratorParameter.setFilter(columnValueFilter);
        }
        rangeIteratorParameter.setMaxVersions(1);

        this.iterator = client.createRangeIterator(rangeIteratorParameter);
    }

    @Override public E current() {
        return current;
    }

    @Override public boolean moveNext() {
        while (iterator.hasNext()) {
            Row row = iterator.next();
            ArrayList<String> data = new ArrayList<>();
            for (PrimaryKeyColumn primaryKey : row.getPrimaryKey().getPrimaryKeyColumns()) {
                data.add(primaryKey.getValue().toString());
            }
            // 实际返回列数必须与json配置中的条数一致
            if (tableDesc.getColumns().size() != row.getColumns().length)
                throw new RuntimeException("Columns size not equal! No surpport schema on read!");
            for (Column column: row.getColumns()) {
                data.add(column.getValue().toString());
            }
            current = (E) data.toArray();
            return true;
        }
        return false;
    }

    @Override public void reset() {
        throw new UnsupportedOperationException();
    }

    @Override public void close() {
        iterator = null;
    }
}
