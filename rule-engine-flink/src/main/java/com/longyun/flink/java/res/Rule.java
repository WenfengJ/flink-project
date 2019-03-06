package com.longyun.flink.java.res;


import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author yuanxiaolong
 * @ClassName com.longyun.blink.java.Rule
 * @Description TODO
 * @Date 2019/2/26 11:31
 * @Version 1.0
 **/
public class Rule<Tag> implements Serializable {

    private String id;

    private String topicPattern;

    private String ruleSQL;

    private String templateRow;

    private OutputTag<Tag> outputTag;

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


    public String getTemplateRow() {
        return templateRow;
    }

    public void setTemplateRow(String templateRow) {
        this.templateRow = templateRow;
    }

    public OutputTag<Tag> getOutputTag() {
        return outputTag;
    }

    public void setOutputTag(OutputTag<Tag> outputTag) {
        this.outputTag = outputTag;
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

    public Rule withTemplateRow(String templateRow){
        this.setTemplateRow(templateRow);
        return this;
    }

    public Rule withOutputTag(OutputTag<Tag> outputTag){
        this.setOutputTag(outputTag);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Rule<?> rule = (Rule<?>) o;
        return Objects.equals(id, rule.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "Rule{" +
                "id='" + id + '\'' +
                ", topicPattern='" + topicPattern + '\'' +
                ", ruleSQL='" + ruleSQL + '\'' +
                ", templateRow='" + templateRow + '\'' +
                '}';
    }
}
