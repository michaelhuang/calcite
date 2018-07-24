package org.apache.calcite.adapter.ots.metadata;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ots table元数据管理器
 *
 * @author huangzf@thfund.com.cn
 * @version 1.0
 * @since 2018-07-13
 */
public class MetaManager {

    private static Map<String, TableDesc> tableDescs = new ConcurrentHashMap<>();

    private MetaManager() {

    }

    static final void putTableDesc (String tableName, TableDesc tableDesc) {
        tableDescs.put(tableName, tableDesc);
    }

    public static final TableDesc getTableDesc (String tableName) {
        return tableDescs.get(tableName);
    }

    public static final Set<String> getTables() {
        return tableDescs.keySet();
    }
}
