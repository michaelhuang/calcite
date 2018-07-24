/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.ots;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;

import java.util.HashMap;
import java.util.Map;

/**
 * 表格存储支持 5 种类型的列值：
 * 数据类型	定义	是否可为主键	大小限制
 * String	UTF-8，可为空	是	为主键列时最大为 1 KB，为属性列时请参考限制说明。
 * Integer	64 bit，整型	是	8 Bytes
 * Double	64 bit，Double 类型	否	8 Bytes
 * Boolean	True/False，布尔类型	否	1 Byte
 * Binary	二进制数据，可为空	是	为主键列时最大为 1 KB，为属性列时请参考限制说明。
 */
public enum OtsFieldType {
    STRING("string"),
    INTEGER("integer"),
    DOUBLE("double"),
    BOOLEAN("boolean"),
    BINARY("binary");

    private final String simpleName;

    private static final Map<String, OtsFieldType> MAP = new HashMap<>();

    static {
        for (OtsFieldType value : values()) {
            MAP.put(value.simpleName, value);
        }
    }

    OtsFieldType(String simpleName) {
        this.simpleName = simpleName;
    }

    public static OtsFieldType of(String typeString) {
        return MAP.get(typeString);
    }

    public PrimaryKeyValue toPK(String val) {
        switch (this) {
            case STRING:
                return PrimaryKeyValue.fromString(val);
            case INTEGER:
                return PrimaryKeyValue.fromLong(Long.parseLong(val));
            default:
                throw new RuntimeException("Unknown ots type: " + this);
        }
    }

    public ColumnValue toColumn(String val) {
        switch (this) {
            case STRING:
                return ColumnValue.fromString(val);
            case INTEGER:
                return ColumnValue.fromLong(Long.parseLong(val));
            default:
                throw new RuntimeException("Unknown ots type: " + this);
        }
    }
}
