package org.apache.flink.formats.raw;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * @author yuanxiaolong
 * @ClassName org.apache.flink.table.descriptors.formats.raw.RawRowDeseralizationSchema
 * @Description TODO
 * @Date 2019/2/20 15:14
 * @Version 1.0
 **/
public class RawStringRowDeseralizationSchema implements DeserializationSchema<Row> {

    /** Type information describing the result type. */
    private final TypeInformation<Row> typeInfo;

    private String characterEncoding;

    public RawStringRowDeseralizationSchema(TypeInformation<Row> typeInfo, String characterEncoding){
        Preconditions.checkNotNull(typeInfo, "Type information");
        this.typeInfo = typeInfo;

        if (!(typeInfo instanceof RowTypeInfo)) {
            throw new IllegalArgumentException("Row type information expected.");
        }
        this.characterEncoding = characterEncoding;
    }

    @Override
    public Row deserialize(byte[] message) throws IOException {
        RowTypeInfo info = (RowTypeInfo) typeInfo;
        final String[] names = info.getFieldNames();
//        final TypeInformation<?>[] types = info.getFieldTypes();

        String rawString = new String(message, characterEncoding);
        final Row row = new Row(names.length);
        for (int i = 0; i < names.length; i++) {
//            final String name = names[i];
            row.setField(i, rawString);
        }
        return row;
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return typeInfo;
    }
}
