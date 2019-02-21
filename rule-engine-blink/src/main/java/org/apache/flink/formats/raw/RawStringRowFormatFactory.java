package org.apache.flink.formats.raw;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.types.TypeConverters;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.RawStringValidator;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.TableFormatFactoryBase;
import org.apache.flink.table.util.TableSchemaUtil;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author yuanxiaolong
 * @ClassName org.apache.flink.table.descriptors.formats.raw.RawStringRowFormatFactory
 * @Description TODO
 * @Date 2019/2/20 15:05
 * @Version 1.0
 **/
public class RawStringRowFormatFactory extends TableFormatFactoryBase<Row>
        implements SerializationSchemaFactory<Row>, DeserializationSchemaFactory<Row> {

    public RawStringRowFormatFactory() {
        super(RawStringValidator.FORMAT_TYPE_VALUE, 1, true);
    }

    @Override
    public List<String> supportedFormatProperties() {
        final List<String> properties = new ArrayList<>();
        properties.add(RawStringValidator.FORMAT_CHARACTER_ENCODING);
        properties.add(RawStringValidator.FORMAT_SCHEMA);
        return properties;
    }
    @Override
    public DeserializationSchema<Row> createDeserializationSchema(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

        // create and configure
        return new RawStringRowDeseralizationSchema(createTypeInformation(descriptorProperties), descriptorProperties.getString(RawStringValidator.FORMAT_CHARACTER_ENCODING));
    }

    @Override
    public SerializationSchema<Row> createSerializationSchema(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

        return new RawStringRowSerializationSchema(createTypeInformation(descriptorProperties), descriptorProperties.getString(RawStringValidator.FORMAT_CHARACTER_ENCODING));
    }

    private TypeInformation<Row> createTypeInformation(DescriptorProperties descriptorProperties) {
        if (descriptorProperties.containsKey(RawStringValidator.FORMAT_SCHEMA)) {
            return (RowTypeInfo) descriptorProperties.getType(RawStringValidator.FORMAT_SCHEMA);
        }  else {
            return (TypeInformation<Row>) TypeConverters.createExternalTypeInfoFromDataType(
                    TableSchemaUtil.toRowType(deriveSchema(descriptorProperties.asMap())));
        }
    }

    private static DescriptorProperties getValidatedProperties(Map<String, String> propertiesMap) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putProperties(propertiesMap);

        // validate
        new RawStringValidator().validate(descriptorProperties);

        return descriptorProperties;
    }
}
