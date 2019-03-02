package com.longyun.calcite.json;

import com.google.common.collect.Maps;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Source;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * @author lynn
 * @ClassName com.longyun.calcite.json.JsonTable
 * @Description TODO
 * @Date 19-2-27 下午1:55
 * @Version 1.0
 **/
public abstract class JsonTable extends AbstractTable {

    protected final Queue<String> queue;
    protected final String tplString;
    protected final RelProtoDataType protoRowType;
    protected Map<String, JsonFieldType> fieldTypes;
    protected String[] fieldNames;

    public JsonTable(Queue<String> queue, String tplString, RelProtoDataType protoRowType) {
        this.queue = queue;
        this.tplString = tplString;
        this.protoRowType = protoRowType;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (protoRowType != null) {
            return protoRowType.apply(typeFactory);
        }

        if (fieldTypes == null) {
            fieldTypes = Maps.newTreeMap();

        }

        List<String> names = new ArrayList<>();

        RelDataType relDataType = JsonEnumerator.deduceRowType((JavaTypeFactory) typeFactory, tplString,
                fieldTypes, names);

        if(names.size() > 0)
            fieldNames = names.toArray(new String[0]);

        return relDataType;
    }
}

// End JsonTable.java