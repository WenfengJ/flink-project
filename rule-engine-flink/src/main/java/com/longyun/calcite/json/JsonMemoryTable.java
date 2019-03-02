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
 * @ClassName com.longyun.calcite.json.JsonMemoryTable
 * @Description TODO
 * @Date 19-2-28 下午2:04
 * @Version 1.0
 **/
public class JsonMemoryTable extends JsonTable implements ScannableTable {

    public JsonMemoryTable(Queue<String> queue, String tplString, RelProtoDataType protoRowType) {
        super(queue, tplString, protoRowType);
    }

    public String toString() {
        return "JsonMemoryTable";
    }

    public Enumerable<Object[]> scan(DataContext root) {
      return new AbstractEnumerable<Object[]>() {
                public Enumerator<Object[]> enumerator() {
                    return new JsonEnumerator<>(queue,
                            new JsonEnumerator.JsonRowConverter(fieldTypes, fieldNames));
                }
            };
    }
}

// End JsonMemoryTable.java
