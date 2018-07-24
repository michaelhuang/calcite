## Aliyun ots sql解析支持

即将sql翻译成ots的API语句

- 只支持string, integer类型 [够用了]
- 不支持多版本, 不支持family [ots也木有family]
- 不支持schema on read [没辙, 一般也不需要这个功能]
- 只实现scan操作 [基本都是scan操作]
- 支持ots所有filter关系代数 [必须全]
AND, NOT, IN, EQUAL, NOT_EQUAL, GREATER_THAN, GREATER_EQUAL, LESS_THAN, LESS_EQUAL

## howto
###### model.json
```
{
    "version": "1.0",
    "defaultSchema": "crm",
    "schemas": [
        {
        "name": "crm",
        "type": "custom",
        "factory": "org.apache.calcite.adapter.ots.OtsSchemaFactory",
            "operand": {
                "instanceName": "",
                "endPoint": "",
                "accessKeyId": "",
                "accessKeySecret": "",
                "directory": "table元数据配置目录"
            }
        }
    ]
  }
```

###### table元数据配置
放到上面model.json配置中的`directory`下, `文件名为表名`, 样例内容如下[ots最多只支持4个pk]:
```
{
  "rowkeys": "
  pk1:string,
  pk2:string,
  pk3:integer,
  pk4:string",
  
  "columns": "
  column1:string,
  column2:string,
  column3:string,
  column4:string,
  ...
  ...
  ..."
}
```

###### 执行sql
```
$ ./sqlline
# sqlline> !connect jdbc:calcite:model=target/test-classes/model.json admin admin
```

## todo
- ots client依赖注入
- 动态刷新ots表配置json, watchservice文件更改监控
- 有时候为了性能, PK时间倒排, 所以sql支持运算会很方便
