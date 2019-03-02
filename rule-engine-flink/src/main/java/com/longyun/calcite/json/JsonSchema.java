package com.longyun.calcite.json;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;
import java.util.Queue;

/**
 * @author lynn
 * @ClassName com.longyun.calcite.json.JsonSchema
 * @Description TODO
 * @Date 19-2-27 下午1:44
 * @Version 1.0
 **/
public class JsonSchema extends AbstractSchema {

    private final String name;
    private final Map<String, String> tblTplMap;
    private final MemorySource<String> source;
    private Map<String, Table> tableMap;

    /**
     * Creates a Json schema.
     *
     * @param tblTplMap  the table name and the template json string
     * @param name  the schema name(simliar to db name)
     */
    public JsonSchema(String name, Map<String, String> tblTplMap, MemorySource<String> source) {
        super();
        this.name = name;
        this.tblTplMap = tblTplMap;
        this.source = source;
    }

    @Override protected Map<String, Table> getTableMap() {
        if (tableMap == null) {
            tableMap = createTableMap();
        }
        return tableMap;
    }

    private Map<String, Table> createTableMap() {
        // Build a map from table name to table; each file becomes a table.
        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

        for(Map.Entry<String, String> entry: tblTplMap.entrySet()){
            final Table table = createTable(entry.getValue(), source.getQueue(entry.getKey()));
            builder.put(entry.getKey(), table);
        }


        return builder.build();
    }

    /** Creates different sub-type of table based on the "flavor" attribute. */
    private Table createTable(String tplString, Queue<String> queue) {
        return new JsonMemoryTable(queue, tplString, null);
    }
}

// End JsonSchema.java
