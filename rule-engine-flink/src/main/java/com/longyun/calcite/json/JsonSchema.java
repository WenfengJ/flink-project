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

    private final Queue<String> source;
    private final JsonTable.Flavor flavor;
    private final String name;
    private Map<String, Table> tableMap;

    /**
     * Creates a Json schema.
     *
     * @param flavor     Whether to instantiate flavor tables that undergo
     *                   query optimization
     * @param  source
     */
    public JsonSchema(JsonTable.Flavor flavor, Queue<String> source, String name) {
        super();
        this.flavor = flavor;
        this.source = source;
        this.name = name;
    }

    /** Looks for a suffix on a string and returns
     * either the string with the suffix removed
     * or the original string. */
    private static String trim(String s, String suffix) {
        String trimmed = trimOrNull(s, suffix);
        return trimmed != null ? trimmed : s;
    }

    /** Looks for a suffix on a string and returns
     * either the string with the suffix removed
     * or null. */
    private static String trimOrNull(String s, String suffix) {
        return s.endsWith(suffix)
                ? s.substring(0, s.length() - suffix.length())
                : null;
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

        final Table table = createTable(source);
        builder.put(name, table);

        return builder.build();
    }

    /** Creates different sub-type of table based on the "flavor" attribute. */
    private Table createTable(Queue<String> source) {
        switch (flavor) {
            case SCANNABLE:
                return new JsonScannableTable(source, null);
            default:
                throw new AssertionError("Unknown flavor " + this.flavor);
        }
    }
}

// End JsonSchema.java
