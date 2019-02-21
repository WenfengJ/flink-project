package org.apache.flink.table.descriptors;

import org.apache.flink.util.Preconditions;

import java.util.Map;

import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA;
import static org.apache.flink.table.descriptors.RawStringValidator.*;

/**
 * @author yuanxiaolong
 * @ClassName org.apache.flink.table.descriptors.Raw
 * @Description TODO
 * @Date 2019/2/20 12:54
 * @Version 1.0
 **/
public class RawString extends FormatDescriptor {

    private Boolean deriveSchema;
    private String characterEncoding;
    private String schema;

    public RawString() {
        super(FORMAT_TYPE_VALUE, 1);
    }

    public RawString characterEncoding(String characterEncoding){
        Preconditions.checkNotNull(characterEncoding);
        this.characterEncoding = characterEncoding;
        return this;
    }

    public RawString deriveSchema() {
        this.deriveSchema = true;
        this.schema = null;
        return this;
    }

    @Override
    protected Map<String, String> toFormatProperties() {
        final DescriptorProperties properties = new DescriptorProperties();
        if (deriveSchema != null) {
            properties.putBoolean(FORMAT_DERIVE_SCHEMA, deriveSchema);
        }

        if (schema != null) {
            properties.putString(FORMAT_SCHEMA, schema);
        }

        if (characterEncoding != null) {
            properties.putString(FORMAT_CHARACTER_ENCODING, characterEncoding);
        }

        return properties.asMap();
    }
}
