package org.apache.calcite.adapter.ots.metadata;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.ots.OtsFieldType;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

/**
 * json配置读取加载器
 *
 * <p>
 * 1. 读取ots表json配置 <br>
 * {"rowkeys":"pk1:string,pk2:integer...", "columns":"column1:string,column2:string,column3:string..."} <br>
 * </p><p>
 * 2. 初始化 {@link MetaManager}
 * </p>
 * @author huangzf@thfund.com.cn
 * @version 1.0
 * @since 2018-07-13
 */
public class TableConfigReader {

    /**
     * 初始化 {@link MetaManager}
     *
     * @param tableName
     * @param tableConf
     */
    private final static void installConfig(String tableName, String tableConf) {
        JSONObject jsonObject = JSONObject.parseObject(tableConf);
        String rowkeys = jsonObject.getString("rowkeys");
        String columns = jsonObject.getString("columns");

        ImmutableList.Builder<RowKey> rowKeyList = ImmutableList.builder();
        for (String column : rowkeys.split(",")) {
            String[] config = column.split(":");
            rowKeyList.add(new RowKey(config[0].toUpperCase(Locale.ROOT), OtsFieldType.of(config[1])));
        }
        ImmutableList.Builder<Column> columnList = ImmutableList.builder();
        for (String column : columns.split(",")) {
            String[] config = column.split(":");
            columnList.add(new Column(config[0].toUpperCase(Locale.ROOT), OtsFieldType.of(config[1])));
        }

        MetaManager
            .putTableDesc(tableName, new TableDesc(rowKeyList.build(), columnList.build()));
    }

    /**
     * 读取json配置
     *
     * @param dir
     */
    public final static void tableConfigs(Path dir) {
        StringBuilder conf = new StringBuilder();
        try(DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.json")) {
            for (Path file: stream) {
                try(BufferedReader reader = Files.newBufferedReader(file)) {
                    String line, name;
                    while ((line = reader.readLine()) != null)
                        conf.append(line.trim());
                    name = file.getFileName().toString();
                    installConfig(name.substring(0, name.indexOf(".")), conf.toString());
                    conf.setLength(0);
                    conf.trimToSize();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("read table config error!", e);
        }
    }
}
