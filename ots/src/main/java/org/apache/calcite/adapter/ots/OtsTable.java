package org.apache.calcite.adapter.ots;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.filter.ColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.CompositeColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.ots.metadata.Column;
import org.apache.calcite.adapter.ots.metadata.RowKey;
import org.apache.calcite.adapter.ots.metadata.TableDesc;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * ots table
 * <p>
 *     需要实现 {@link FilterableTable}接口, 获取到where条件
 *
 * @author huangzf@thfund.com.cn
 * @version 1.0
 * @since 2018-07-13
 */
public class OtsTable extends AbstractTable implements FilterableTable {

    final SyncClient client;
    final String tableName;
    final TableDesc tableDesc;

    public OtsTable(SyncClient client, String tableName, TableDesc tableDesc) {
        super();
        this.client = client;
        this.tableName = tableName;
        this.tableDesc = tableDesc;
    }

    /*
    目前全按照string处理
     */
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        final List<String> names = new ArrayList<>();
        final List<RelDataType> types = new ArrayList<>();

        for (RowKey rowKey : tableDesc.getRowKeys()) {
            names.add(rowKey.getName());
            final RelDataType relDataType = typeFactory.createJavaType(String.class);
            types.add(relDataType);
        }
        for (Column column: tableDesc.getColumns()) {
            names.add(column.getName());
            final RelDataType relDataType = typeFactory.createJavaType(String.class);
            types.add(relDataType);
        }

        if (names.isEmpty()) {
            names.add("line");
            types.add(typeFactory.createJavaType(String.class));
        }
        return typeFactory.createStructType(Pair.zip(names, types));
    }

    @Override public String toString() {
        return "OtsTable{" + tableName + "}";
    }

    @Override public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
        if (null == filters || filters.size() != 1) {
            throw new RuntimeException("filters invalid! " + filters);
        }
        final RexNode filter = filters.get(0);
        Multimap<Integer, String> primaryKeys = ArrayListMultimap.create();
        final ColumnValueFilter columnValueFilter = addFilter(filter, primaryKeys);
        // 过滤放到ots里做
        filters.clear();
        return new AbstractEnumerable<Object[]>() {
            public Enumerator<Object[]> enumerator() {
                return new OtsEnumerator<>(client,tableName, columnValueFilter,
                    primaryKeys, tableDesc);
            }
        };
    }

    /**
     * sql解析: 区分出 PK, filter
     *
     * @param filter
     * @param primaryKeys
     * @return
     */
    private ColumnValueFilter addFilter(RexNode filter,
        Multimap<Integer, String> primaryKeys) {
        ColumnValueFilter result = null;
        if (filter.isA(SqlKind.EQUALS)
            || filter.isA(SqlKind.NOT_EQUALS)
            || filter.isA(SqlKind.GREATER_THAN)
            || filter.isA(SqlKind.GREATER_THAN_OR_EQUAL)
            || filter.isA(SqlKind.LESS_THAN)
            || filter.isA(SqlKind.LESS_THAN_OR_EQUAL)) {
            final RexCall call = (RexCall) filter;
            RexNode left = call.getOperands().get(0);
            if (left.isA(SqlKind.CAST)) {
                left = ((RexCall) left).operands.get(0);
            }
            final RexNode right = call.getOperands().get(1);
            if (left instanceof RexInputRef
                && right instanceof RexLiteral) {
                final int index = ((RexInputRef) left).getIndex();
                final String value = ((RexLiteral) right).getValue2().toString();
                if (tableDesc.isPrimaryKey(index)) {
                    primaryKeys.put(index, value);
                } else {
                    Column column = tableDesc.getColumn(index);
                    SingleColumnValueFilter single = new SingleColumnValueFilter(
                        column.getName().toLowerCase(Locale.ROOT),
                        toOtsOperator(filter.getKind()),
                        column.getType().toColumn(value));
                    result = single;
                }
            }
        } else if (filter.isA(SqlKind.AND)
            || filter.isA(SqlKind.OR)
            || filter.isA(SqlKind.NOT)) {

            CompositeColumnValueFilter filter1;
            if (filter.isA(SqlKind.AND))
                filter1 = new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.AND);
            else if (filter.isA(SqlKind.OR))
                filter1 = new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.OR);
            else
                filter1 = new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.NOT);
            final RexCall call = (RexCall) filter;
            for (RexNode node : call.getOperands()) {
                ColumnValueFilter valueFilter = addFilter(node, primaryKeys);
                if (null != valueFilter)
                    filter1.addFilter(valueFilter);
            }
            // AND/OR operator: the number of sub-filters must be more than 2
            if (filter1.getSubFilters().size() > 0) {
                result = filter1;
                if (filter.isA(SqlKind.AND)
                    || filter.isA(SqlKind.OR)) {
                    if (filter1.getSubFilters().size() == 1)
                        result = filter1.getSubFilters().get(0);
                }
            }
        } else
            throw new RuntimeException("Unknown sqlKind: " + filter.getKind());
        return result;
    }

    /*
    sql逻辑关系映射到OTS filter
     */
    static SingleColumnValueFilter.CompareOperator toOtsOperator(SqlKind sqlKind) {
        SingleColumnValueFilter.CompareOperator filterOperator;
        switch (sqlKind) {
            case EQUALS:
                filterOperator = SingleColumnValueFilter.CompareOperator.EQUAL;
                break;
            case NOT_EQUALS:
                filterOperator = SingleColumnValueFilter.CompareOperator.NOT_EQUAL;
                break;
            case GREATER_THAN:
                filterOperator = SingleColumnValueFilter.CompareOperator.GREATER_THAN;
                break;
            case GREATER_THAN_OR_EQUAL:
                filterOperator = SingleColumnValueFilter.CompareOperator.GREATER_EQUAL;
                break;
            case LESS_THAN:
                filterOperator = SingleColumnValueFilter.CompareOperator.LESS_THAN;
                break;
            case LESS_THAN_OR_EQUAL:
                filterOperator = SingleColumnValueFilter.CompareOperator.LESS_EQUAL;
                break;
            default:
                throw new RuntimeException("Unknown sqlKind: " + sqlKind);
        }

        return filterOperator;
    }
}
