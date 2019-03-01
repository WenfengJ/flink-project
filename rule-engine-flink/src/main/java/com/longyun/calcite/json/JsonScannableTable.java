package com.longyun.calcite.json;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;

import java.util.Queue;

/**
 * @author lynn
 * @ClassName com.longyun.calcite.json.JsonScannableTable
 * @Description TODO
 * @Date 19-2-28 下午2:04
 * @Version 1.0
 **/
public class JsonScannableTable extends JsonTable implements ScannableTable {

    public JsonScannableTable(Queue<String> source, RelProtoDataType protoRowType) {
        super(source, protoRowType);
    }

    public String toString() {
        return "JsonScannableTable";
    }

    public Enumerable<Object[]> scan(DataContext root) {
        return new AbstractEnumerable<Object[]>() {
            public Enumerator<Object[]> enumerator() {
                return new JsonEnumerator<>(source,
                        new JsonEnumerator.JsonRowConverter(fieldTypes, fieldNames));
            }
        };
    }
}

// End JsonScannableTable.java
