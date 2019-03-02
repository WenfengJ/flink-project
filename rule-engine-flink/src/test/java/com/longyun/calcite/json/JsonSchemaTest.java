package com.longyun.calcite.json;

import com.google.common.collect.Maps;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author lynn
 * @ClassName com.longyun.calcite.json.JsonSchemaTest
 * @Description TODO
 * @Date 19-2-28 下午2:08
 * @Version 1.0
 **/
public class JsonSchemaTest {

    public static void executeQuery(CalciteConnection calciteConnection, String sql) throws Exception {
        System.out.println(sql);
        Statement statement = calciteConnection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);

        if(resultSet != null){
            final StringBuilder buf = new StringBuilder();
            while (resultSet.next()) {
                int n = resultSet.getMetaData().getColumnCount();
                for (int i = 1; i <= n; i++) {
                    buf.append(i > 1 ? "; " : "")
                            .append(resultSet.getMetaData().getColumnLabel(i))
                            .append("\t")
                            .append(resultSet.getObject(i));
                }
                System.out.println(buf.toString());
                buf.setLength(0);
            }
            resultSet.close();
        }else {
            System.err.println("resultSet is null!");
        }

        statement.close();
    }

    /**
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {

        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties info = new Properties();
        Connection connection =
                DriverManager.getConnection("jdbc:calcite:caseSensitive=false;lex=MYSQL", info);
        CalciteConnection calciteConnection =
                connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        Map<String, Object> operand = Maps.newHashMap();

        MemorySource<String> source = new MemorySource<>();
        source.offer("sensor", "{\"clienttoken\":\"token1\",\"state\":{\"status\":\"online\", \"value\":12},\"timestamp\":1551422889}");
        source.offer("sensor", "{\"clienttoken\":\"token1\",\"state\":{\"status\":\"online\", \"value\":2},\"timestamp\":1551422889}");
        source.offer("sensor", "{\"clienttoken\":\"token1\",\"state\":{\"status\":\"online\", \"value\":11},\"timestamp\":1551422889}");
        source.offer("sensor", "{\"clienttoken\":\"token1\",\"state\":{\"status\":\"online\", \"value\":17},\"timestamp\":1551422889}");

        source.offer("client", "{\"clientid\":\"client-1\", \"clienttoken\":\"token1\"}");


        Map<String, String> tblTplMap = Maps.newHashMap();
        tblTplMap.put("sensor", "{\"clienttoken\":\"token1\",\"state\":{\"status\":\"online\", \"value\":12},\"timestamp\":1551422889}");
        tblTplMap.put("client", "{\"clientid\":\"client-1\", \"clienttoken\":\"token1\"}");

        operand.putIfAbsent("source", source);
        operand.putIfAbsent("tbl-tpl", tblTplMap);

        Schema schema = JsonSchemaFactory.INSTANCE.create(rootSchema, "JSON", operand);
        rootSchema.add("JSON", schema);


        executeQuery(calciteConnection,"SELECT * FROM JSON.sensor where state__value > 10");

        executeQuery(calciteConnection,"SELECT * FROM JSON.client");

        TimeUnit.SECONDS.sleep(10);
        System.out.println("-------insert into table---------");
        source.offer("sensor", "{\"clienttoken\":\"token2\",\"state\":{\"status\":\"online\", \"value\":26},\"timestamp\":1551424889}");

        source.offer("client", "{\"clientid\":\"client-2\", \"clienttoken\":\"token2\"}");

        executeQuery(calciteConnection, "SELECT * FROM JSON.SENSOR where state__value > 10");

        executeQuery(calciteConnection,"SELECT * FROM JSON.client");

        connection.close();
    }
}
