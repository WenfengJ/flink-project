package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;

import java.io.UnsupportedEncodingException;

/**
 * @author yuanxiaolong
 * @ClassName org.apache.flink.table.descriptors.TextValidator
 * @Description TODO
 * @Date 2019/2/20 13:17
 * @Version 1.0
 **/
@Internal
public class RawStringValidator extends FormatDescriptorValidator {

    public static final String FORMAT_TYPE_VALUE = "raw";

    public static final String FORMAT_CHARACTER_ENCODING = "format.character-encoding";

    public static final String FORMAT_SCHEMA = "format.schema";

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        final boolean deriveSchema = properties.getOptionalBoolean(FORMAT_DERIVE_SCHEMA).orElse(false);
        final boolean hasSchema = properties.containsKey(FORMAT_SCHEMA);
        properties.validateBoolean(FORMAT_DERIVE_SCHEMA, true);
        if (deriveSchema && hasSchema) {
            throw new ValidationException(
                    "Format cannot define a schema and derive from the table's schema at the same time.");
        } else if (!deriveSchema && !hasSchema){
            throw new ValidationException("A definition of a schema is required.");
        } else if (hasSchema) {
            properties.validateType(FORMAT_SCHEMA, false, true);
        }

        final boolean hasCharacterEncoding = properties.containsKey(FORMAT_CHARACTER_ENCODING);
        if(!hasCharacterEncoding){
            throw new ValidationException("A definition of a character encoding is required.");
        }else{
            try {
                byte[] bytes = new byte[]{1, 0, 1, 0};
                new String(bytes, properties.getString(FORMAT_CHARACTER_ENCODING));
            }catch (UnsupportedEncodingException e){
                throw new ValidationException(e.getMessage());
            }
        }
    }
}
