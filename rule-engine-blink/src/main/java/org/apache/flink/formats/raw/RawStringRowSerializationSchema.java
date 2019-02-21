package org.apache.flink.formats.raw;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.UnsupportedEncodingException;

/**
 * @author yuanxiaolong
 * @ClassName org.apache.flink.table.descriptors.formats.raw.RawRowSerializationSchema
 * @Description TODO
 * @Date 2019/2/20 14:15
 * @Version 1.0
 **/
@PublicEvolving
public class RawStringRowSerializationSchema implements SerializationSchema<Row> {

    private String characterEncoding;

    /** Type information describing the input type. */
    private final TypeInformation<Row> typeInfo;

    /**
     * Creates a Raw serialization schema for the given type information.
     *
     * @param typeInfo The field names of {@link Row} are used to map to JSON properties.
     */
    public RawStringRowSerializationSchema(RowTypeInfo typeInfo) {
        Preconditions.checkNotNull(characterEncoding, "Character Encoding");
        this.typeInfo = typeInfo;
    }

    /**
     * Creates a Raw serialization schema for the given type information.
     *
     * @param characterEncoding The field names of {@link Row} are used to map to JSON properties.
     */
    public RawStringRowSerializationSchema(TypeInformation<Row> typeInfo, String characterEncoding) {
        Preconditions.checkNotNull(characterEncoding, "Character Encoding");
        this.characterEncoding = characterEncoding;
        this.typeInfo = typeInfo;
    }

    @Override
    public byte[] serialize(Row row) {
        RowTypeInfo info = (RowTypeInfo) typeInfo;
        final String[] fieldNames = info.getFieldNames();
        final TypeInformation<?>[] fieldTypes = info.getFieldTypes();
        // validate the row
        if (row.getArity() != fieldNames.length) {
            throw new IllegalStateException(String.format(
                    "Number of elements in the row '%s' is different from number of field names: %d", row, fieldNames.length));
        }
        if(fieldNames.length != 1){
            throw new IllegalStateException(String.format("field names number :%d is not equal to %d", fieldNames.length, 1));
        }

//        final String name = fieldNames[0];
//        TypeInformation<?> typeInformation = fieldTypes[0];
        Object value = row.getField(0);
        if(null != value){
            try {
                return value.toString().getBytes(characterEncoding);
            }catch (UnsupportedEncodingException e){
                throw new IllegalStateException(e.getMessage());
            }
        }

        return new byte[0];
    }
 }
