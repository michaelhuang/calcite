package org.apache.calcite.adapter.ots;

import com.alicloud.openservices.tablestore.SyncClient;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.adapter.ots.metadata.MetaManager;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Locale;
import java.util.Map;

/**
 * ots schema
 * <p>
 *     初始化 ots client, tables
 *
 * @author huangzf@thfund.com.cn
 * @version 1.0
 * @since 2018-07-13
 */
public class OtsSchema extends AbstractSchema {

    final SyncClient client;
    private ImmutableMap<String, Table> tableMap;

    public OtsSchema(String instanceName, String endPoint,
        String accessKeyId, String accessKeySecret) {
        super();
        try {
            this.client = new SyncClient(endPoint, accessKeyId, accessKeySecret, instanceName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override protected Map<String, Table> getTableMap() {
        if (null == tableMap) {
            final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
            for (String tableName : MetaManager.getTables()) {
                builder.put(tableName.toUpperCase(Locale.ROOT), new OtsTable(client, tableName, MetaManager.getTableDesc(tableName)));
            }
            tableMap = builder.build();
        }
        return tableMap;
    }
}
