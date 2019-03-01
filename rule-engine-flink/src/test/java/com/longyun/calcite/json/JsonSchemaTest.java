package com.longyun.calcite.json;

import com.google.common.collect.Maps;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;

/**
 * @author lynn
 * @ClassName com.longyun.calcite.json.JsonSchemaTest
 * @Description TODO
 * @Date 19-2-28 下午2:08
 * @Version 1.0
 **/
public class JsonSchemaTest {


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

        Queue<String> source = new LinkedList<>();
        source.offer("{\"clienttoken\":\"token1\",\"state\":{\"status\":\"online\", \"value\":12},\"timestamp\":1551422889}");
        source.offer("{\"clienttoken\":\"token1\",\"state\":{\"status\":\"online\", \"value\":12},\"timestamp\":1551422889}");
        source.offer("{\"clienttoken\":\"token1\",\"state\":{\"status\":\"online\", \"value\":12},\"timestamp\":1551422889}");
        source.offer("{\"clienttoken\":\"token1\",\"state\":{\"status\":\"online\", \"value\":12},\"timestamp\":1551422889}");

        operand.putIfAbsent("source", source);
        operand.putIfAbsent("name", "SENSOR");
        operand.putIfAbsent("flavor", null);

        Schema schema = JsonSchemaFactory.INSTANCE.create(rootSchema, "JSON", operand);
        rootSchema.add("JSON", schema);

        Statement statement = calciteConnection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM JSON.SENSOR");

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
        connection.close();
    }
}
