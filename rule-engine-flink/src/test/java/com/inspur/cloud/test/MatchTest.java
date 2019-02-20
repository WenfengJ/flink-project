package com.inspur.cloud.test;

import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author yuanxiaolong
 * @ClassName com.inspur.cloud.test.MatchTest
 * @Description TODO
 * @Date 2019/1/16 15:08
 * @Version 1.0
 **/
public class MatchTest {

    public static Map<String, String> getIndexedProperty(Properties properties, String key, String subKey) {
        String escapedKey = Pattern.quote(key);
        String escapedSubKey = Pattern.quote(subKey);
        System.out.println("regex: " + escapedKey + "\\.\\d+\\." + escapedSubKey);
        return (Map)properties.entrySet().stream().filter((entry) -> {
            System.out.println(entry.getKey());
            return ((String)entry.getKey()).matches(escapedKey + "\\.\\d+\\." + escapedSubKey);
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static Map<String, String> getIndexedProperty2(Properties properties, String key, String subKey) {
        System.out.println("regex: " + key + "\\.\\d+\\." + subKey);
        return (Map)properties.entrySet().stream().filter((entry) -> {
            System.out.println(entry.getKey());
            return ((String)entry.getKey()).matches(key + "\\.\\d+\\." + subKey);
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
    /**
     *
     * @param args
     */
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty("format.type", "json");
        properties.setProperty("format.schema", "ROW<clientToken VARCHAR,state VARCHAR,timestamp TIMESTAMP>");
        properties.setProperty("schema.0.name", "field1");
        properties.setProperty("schema.1.name", "field2");
        Map<String, String> result = getIndexedProperty(properties, "schema", "name");
        System.out.println(result.size());
    }
}
