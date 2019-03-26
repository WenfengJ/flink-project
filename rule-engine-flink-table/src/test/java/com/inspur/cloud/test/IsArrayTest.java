package com.inspur.cloud.test;

import org.apache.flink.types.Row;

import java.lang.reflect.Array;

/**
 * @author lynn
 * @ClassName com.inspur.cloud.test.IsArrayTest
 * @Description TODO
 * @Date 19-3-26 上午9:35
 * @Version 1.0
 **/
public class IsArrayTest {

    public static void main (String args[]) {
//        printType(args);

        printType(Array.newInstance(Row.class, 10));
    }

    private static void printType (Object object) {
        Class type = object.getClass();
        if (type.isArray()) {
            Class elementType = type.getComponentType();
            System.out.println("Array of: " + elementType);
            System.out.println(" Length: " + Array.getLength(object));
        }
    }
}
