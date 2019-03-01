package com.longyun.calcite.json;

import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.io.File;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;

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
        final Queue<String> source = (Queue<String>)operand.get("source");
        String flavorName = (String) operand.get("flavor");
        String tableName = (String) operand.get("name");

        JsonTable.Flavor flavor;
        if (flavorName == null) {
            flavor = JsonTable.Flavor.SCANNABLE;
        } else {
            flavor = JsonTable.Flavor.valueOf(flavorName.toUpperCase(Locale.ROOT));
        }
        return new JsonSchema(flavor, source, tableName);
    }
}

// End JsonschemaFactory.java
