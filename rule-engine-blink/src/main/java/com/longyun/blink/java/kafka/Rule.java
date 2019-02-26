package com.longyun.blink.java.kafka;

import com.google.common.collect.Maps;
import org.apache.flink.table.api.types.DataType;

import java.io.Serializable;
import java.util.Map;

/**
 * @author yuanxiaolong
 * @ClassName com.longyun.blink.java.kafka.Rule
 * @Description TODO
 * @Date 2019/2/26 11:31
 * @Version 1.0
 **/
public class Rule implements Serializable {

    private String id;

    private String topicPattern;

    private String ruleSQL;

    private Map<String, DataType> fieldTypes;


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTopicPattern() {
        return topicPattern;
    }

    public void setTopicPattern(String topicPattern) {
        this.topicPattern = topicPattern;
    }

    public String getRuleSQL() {
        return ruleSQL;
    }

    public void setRuleSQL(String ruleSQL) {
        this.ruleSQL = ruleSQL;
    }

    public Map<String, DataType> getFieldTypes() {
        return fieldTypes;
    }

    public void setFieldTypes(Map<String, DataType> fieldTypes) {
        this.fieldTypes = fieldTypes;
    }

    public Rule withId(String id) {
        this.setId(id);
        return this;
    }

    public Rule withTopicPattern(String topicPattern) {
        this.setTopicPattern(topicPattern);
        return this;
    }

    public Rule withRuleSQL(String ruleSQL) {
        this.setRuleSQL(ruleSQL);
        return this;
    }

    public Rule withFieldTypes(Map<String, DataType> fieldTypes) {
        this.setFieldTypes(fieldTypes);
        return this;
    }

    public Rule addFieldTypes(String field, DataType type) {
        if(null == this.getFieldTypes()){
            this.fieldTypes = Maps.newTreeMap();
        }
        this.fieldTypes.put(field, type);
        return this;
    }

    @Override
    public String toString() {
        return "Rule{" +
                "id='" + id + '\'' +
                ", topicPattern='" + topicPattern + '\'' +
                ", ruleSQL='" + ruleSQL + '\'' +
                ", fieldTypes=" + fieldTypes +
                '}';
    }
}
