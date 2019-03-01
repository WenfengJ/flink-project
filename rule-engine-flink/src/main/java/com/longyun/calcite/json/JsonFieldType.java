package com.longyun.calcite.json;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lynn
 * @ClassName com.longyun.calcite.json.JsonFieldType
 * @Description TODO
 * @Date 19-2-27 下午1:59
 * @Version 1.0
 **/
enum JsonFieldType {

    STRING(String.class, "string"),
    BOOLEAN(Primitive.BOOLEAN),
    BYTE(Primitive.BYTE),
    CHAR(Primitive.CHAR),
    SHORT(Primitive.SHORT),
    INT(Primitive.INT),
    LONG(Primitive.LONG),
    FLOAT(Primitive.FLOAT),
    DOUBLE(Primitive.DOUBLE),
    DATE(java.sql.Date.class, "date"),
    TIME(java.sql.Time.class, "time"),
    TIMESTAMP(java.sql.Timestamp.class, "timestamp");

    private final Class clazz;
    private final String simpleName;

    private static final Map<String, JsonFieldType> MAP = new HashMap<>();

    static {
        for (JsonFieldType value : values()) {
            MAP.put(value.simpleName, value);
        }
    }

    JsonFieldType(Primitive primitive) {
        this(primitive.boxClass, primitive.primitiveClass.getSimpleName());
    }

    JsonFieldType(Class clazz, String simpleName) {
        this.clazz = clazz;
        this.simpleName = simpleName;
    }

    public RelDataType toType(JavaTypeFactory typeFactory) {
        RelDataType javaType = typeFactory.createJavaType(clazz);
        RelDataType sqlType = typeFactory.createSqlType(javaType.getSqlTypeName());
        return typeFactory.createTypeWithNullability(sqlType, true);
    }

    public static JsonFieldType of(String typeString) {
        return MAP.get(typeString);
    }
}

// End JsonFieldType.java