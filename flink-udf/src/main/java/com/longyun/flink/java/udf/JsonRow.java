package com.longyun.flink.java.udf;

import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.table.runtime.functions.utils.JsonUtils;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.sql.Date;
import java.sql.Time;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class JsonRow extends TableFunction<Row> {

    private Map<String, DataType> fieldTypeMap;


    public JsonRow(Map<String, DataType> fieldTypeMap){
        this.fieldTypeMap = fieldTypeMap;
    }

    public void eval(String jsonStr, String... paths) {
        Row row = new Row(paths.length);
        try {
            for (int i = 0; i < paths.length; i++) {
                String value = JsonUtils.getInstance().getJsonObject(jsonStr, "$."+paths[i]);
                DataType typeName = fieldTypeMap.getOrDefault(paths[i], DataTypes.STRING);
                if(typeName == DataTypes.FLOAT){
                    row.setField(i, Float.parseFloat(value));
                }else if(typeName == DataTypes.DOUBLE){
                    row.setField(i, Double.parseDouble(value));
                }else if(typeName == DataTypes.INT){
                    row.setField(i, Integer.parseInt(value));
                }else if(typeName == DataTypes.LONG){
                    row.setField(i, Long.parseLong(value));
                }else if(typeName == DataTypes.TIMESTAMP){
                    Long v = Long.parseLong(value);
                    row.setField(i, new Timestamp(v));
                }else if(typeName == DataTypes.TIME){
                    Long v = Long.parseLong(value);
                    row.setField(i, new Time(v));
                }else if(typeName == DataTypes.DATE){
                    Long v = Long.parseLong(value);
                    row.setField(i, new Date(v));
                }else {
                    row.setField(i, value);
                }
            }

            collect(row);
        }catch (RuntimeException e){
            e.printStackTrace();
        }
    }


    @Override
    public DataType getResultType(Object[] arguments, Class[] argTypes) {
        List<DataType> types = new ArrayList<>();
        for (int i = 0; i < arguments.length; i++) {
            if(null == arguments[i]) continue;
            types.add(fieldTypeMap.getOrDefault(arguments[i].toString(), DataTypes.STRING));
        }

        DataType[] typeArray = new DataType[types.size()];
        types.toArray(typeArray);
        return new RowType(typeArray);
    }
}
