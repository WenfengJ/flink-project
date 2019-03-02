package com.longyun.calcite.json;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.time.FastDateFormat;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

/**
 * @author lynn
 * @ClassName com.longyun.calcite.json.JsonEnumerator
 * @Description TODO
 * @Date 19-2-27 下午1:58
 * @Version 1.0
 **/
public class JsonEnumerator<E> implements Enumerator<E>{

    private final JsonReader reader;
    private final RowConverter<E> rowConverter;
    private E current;

    private static final FastDateFormat TIME_FORMAT_DATE;
    private static final FastDateFormat TIME_FORMAT_TIME;
    private static final FastDateFormat TIME_FORMAT_TIMESTAMP;

    static {
        final TimeZone gmt = TimeZone.getTimeZone("GMT");
        TIME_FORMAT_DATE = FastDateFormat.getInstance("yyyy-MM-dd", gmt);
        TIME_FORMAT_TIME = FastDateFormat.getInstance("HH:mm:ss", gmt);
        TIME_FORMAT_TIMESTAMP =
                FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", gmt);
    }


    JsonEnumerator(Queue<String> source,
                   Map<String, JsonFieldType> fieldTypes, String[] fieldNames) {
        //noinspection unchecked
        this(source, (RowConverter<E>) converter(fieldTypes, fieldNames));
    }

    JsonEnumerator(Queue<String> source, RowConverter<E> rowConverter) {
        this.rowConverter = rowConverter;
        this.reader = new JsonReader(source);
    }


    @Override
    public E current() {
        return current;
    }

    @Override
    public boolean moveNext() {
        final JsonNode node = reader.readNext();
        if(node != null){
            current = rowConverter.convertRow(node);

            return true;
        }
        return false;
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException("Error closing Json reader", e);
        }
    }


    private static RowConverter<?> converter(Map<String, JsonFieldType> fieldTypes, String[] fieldNames) {
        if (fieldTypes.size() == 1) {
            return new SingleColumnRowConverter(fieldTypes.get(fieldNames[0]));
        } else {
            return new JsonRowConverter(fieldTypes, fieldNames);
        }
    }

    /** Deduces the names and types of a table's columns by reading the first line
     * of a JSON file. */
    static RelDataType deduceRowType(JavaTypeFactory typeFactory, String tplString,
                                     Map<String, JsonFieldType> fieldTypes, List<String> names) {
        final List<RelDataType> types = new ArrayList<>();

        try (JsonTplParser parser = new JsonTplParser(tplString)) {
            String[] strings = parser.readFieldTypes();
            if (strings == null) {
                strings = new String[]{"EmptyFileHasNoColumns:boolean"};
            }
            for (String string : strings) {
                final String name;
                final JsonFieldType fieldType;
                final int colon = string.indexOf(':');
                if (colon >= 0) {
                    name = string.substring(0, colon);
                    String typeString = string.substring(colon + 1);
                    fieldType = JsonFieldType.of(typeString);
                    if (fieldType == null) {
                        System.err.println("WARNING: Found unknown type: "
                                + typeString + " in Memory Queue "
                                + " for column: " + name
                                + ". Will assume the type of column is string");
                    }
                } else {
                    name = string;
                    fieldType = null;
                }
                final RelDataType type;
                if (fieldType == null) {
                    type = typeFactory.createSqlType(SqlTypeName.VARCHAR);
                } else {
                    type = fieldType.toType(typeFactory);
                }
                names.add(name);
                types.add(type);
                if (fieldTypes != null) {
                    fieldTypes.put(name, fieldType);
                }
            }
        } catch (IOException e) {
            // ignore
        }
        if (names.isEmpty()) {
            names.add("line");
            types.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));
        }

        return typeFactory.createStructType(Pair.zip(names, types));
    }

    /** Row converter.
     *
     * */
    abstract static class RowConverter<E> {
        abstract E convertRow(JsonNode node);

        protected Object convert(JsonFieldType fieldType, JsonNode node) {
            if (fieldType == null) {
                return null;
            }
            if(null == node || node.isNull() || node.isMissingNode()){
                return null;
            }
            String string = node.asText();
            switch (fieldType) {
                case BOOLEAN:
                    if (!node.isBoolean()) {
                        return null;
                    }
                    return node.booleanValue();
                case BYTE:
                    if (!node.isBinary()) {
                        return null;
                    }
                    try {
                        return node.binaryValue();
                    }catch (IOException e){
                        return null;
                    }
                case SHORT:
                    if (!node.isShort()) {
                        return null;
                    }
                    return node.shortValue();
                case INT:
                    if (!node.isInt()) {
                        return null;
                    }
                    return node.intValue();
                case LONG:
                    if (node.isLong() || node.isInt()) {
                        return node.longValue();
                    }
                    return null;
                case FLOAT:
                    if (!node.isFloat()) {
                        return null;
                    }
                    return node.floatValue();
                case DOUBLE:
                    if (!node.isDouble()) {
                        return null;
                    }
                    return node.doubleValue();
                case DATE:
                    if (string.length() == 0) {
                        return null;
                    }
                    try {
                        Date date = TIME_FORMAT_DATE.parse(string);
                        return (int) (date.getTime() / DateTimeUtils.MILLIS_PER_DAY);
                    } catch (ParseException e) {
                        return null;
                    }
                case TIME:
                    if (string.length() == 0) {
                        return null;
                    }
                    try {
                        Date date = TIME_FORMAT_TIME.parse(string);
                        return (int) date.getTime();
                    } catch (ParseException e) {
                        return null;
                    }
                case TIMESTAMP:
                    if (string.length() == 0) {
                        return null;
                    }
                    try {
                        Date date = TIME_FORMAT_TIMESTAMP.parse(string);
                        return date.getTime();
                    } catch (ParseException e) {
                        return null;
                    }
                case STRING:
                default:
                    return string;
            }
        }
    }

    /** Json row converter. */
    static class JsonRowConverter extends RowConverter<Object[]> {
        private final Map<String, JsonFieldType> fieldTypes;
        private final String[] fieldNames;
        // whether the row to convert is from a stream
        private final boolean stream;

        JsonRowConverter(Map<String, JsonFieldType> fieldTypes, String[] fieldNames) {
            this.fieldTypes = fieldTypes;
            this.fieldNames = fieldNames;
            this.stream = false;
        }

        JsonRowConverter(Map<String, JsonFieldType> fieldTypes, String[] fieldNames, boolean stream) {
            this.fieldTypes = fieldTypes;
            this.fieldNames = fieldNames;
            this.stream = stream;
        }

        public Object[] convertRow(JsonNode node) {
            if (stream) {
                return convertStreamRow(node);
            } else {
                return convertNormalRow(node);
            }
        }

        public Object[] convertNormalRow(final JsonNode node) {
            final Object[] objects = new Object[fieldNames.length];

            for (int i = 0; i < fieldNames.length; i++) {
                if(fieldNames[i].contains(JsonTplParser.DEFAULT_LAYER_SPEARATOR)){
                    String[] tokens = fieldNames[i].split(JsonTplParser.DEFAULT_LAYER_SPEARATOR);
                    JsonNode currentNode = node;
                    for (int j = 0; j < tokens.length; j++){
                        currentNode = currentNode.get(tokens[j]);
                    }
                    objects[i] = convert(fieldTypes.get(fieldNames[i]), currentNode);
                }else {
                    objects[i] = convert(fieldTypes.get(fieldNames[i]), node.get(fieldNames[i]));
                }
            }
            return objects;
        }

        public Object[] convertStreamRow(final JsonNode node) {
            final Object[] objects = new Object[fieldNames.length + 1];

            objects[0] = System.currentTimeMillis();
            for (int i = 0; i < fieldNames.length; i++) {
                if(fieldNames[i].contains(JsonTplParser.DEFAULT_LAYER_SPEARATOR)){
                    String[] tokens = fieldNames[i].split(JsonTplParser.DEFAULT_LAYER_SPEARATOR);
                    JsonNode currentNode = node;
                    for (int j = 0; j < tokens.length; j++){
                        currentNode = currentNode.get(tokens[j]);
                    }

                    objects[i + 1] = convert(fieldTypes.get(fieldNames[i]), currentNode);
                }else {
                    objects[i + 1] = convert(fieldTypes.get(fieldNames[i]), node.get(fieldNames[i]));
                }
            }
            return objects;
        }
    }

    /** Single column row converter. */
    private static class SingleColumnRowConverter extends RowConverter {
        private final JsonFieldType fieldType;

        private SingleColumnRowConverter(JsonFieldType fieldType) {
            this.fieldType = fieldType;
        }

        public Object convertRow(JsonNode node) {
            return convert(fieldType, node);
        }
    }

}

// End CsvEnumerator.java
