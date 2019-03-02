package com.longyun.calcite.json;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

/**
 * @author lynn
 * @ClassName com.longyun.calcite.json.JsonSchemaFactory
 * @Description TODO
 * @Date 19-2-27 下午1:44
 * @Version 1.0
 **/
public class JsonSchemaFactory implements SchemaFactory {

    /** Public singleton, per factory contract. */
    public static final JsonSchemaFactory INSTANCE = new JsonSchemaFactory();

    private JsonSchemaFactory() {
    }

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        final Map<String, String> tblTplMap = (Map<String, String>) operand.get("tbl-tpl");
        final MemorySource<String> source = (MemorySource<String>) operand.get("source");
        return new JsonSchema(name, tblTplMap, source);
    }
}

// End JsonschemaFactory.java
