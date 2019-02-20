package com.longyun.blink.java.udf;

import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DataTypes;

import java.io.UnsupportedEncodingException;

/**
 * @author yuanxiaolong
 * @ClassName com.longyun.blink.java.udf.Bytes2String
 * @Description TODO
 * @Date 2019/1/30 12:49
 * @Version 1.0
 **/
public class Bytes2String extends TableFunction<String> {

    public void eval(byte[] bytes){
        try {
            String msg = new String(bytes, "UTF-8");
            collect(msg);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    @Override
    public DataType getResultType(Object[] arguments, Class[] argTypes) {
        return DataTypes.STRING;
    }
}
