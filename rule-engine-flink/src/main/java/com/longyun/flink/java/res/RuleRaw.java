package com.longyun.flink.java.res;

import java.io.Serializable;

/**
 * @author yuanxiaolong
 * @ClassName com.longyun.blink.java.RuleRaw
 * @Description TODO
 * @Date 2019/2/26 13:14
 * @Version 1.0
 **/
public class RuleRaw implements Serializable {

    private String rule;

    private String raw;

    public String getRule() {
        return rule;
    }

    public void setRule(String rule) {
        this.rule = rule;
    }

    public String getRaw() {
        return raw;
    }

    public void setRaw(String raw) {
        this.raw = raw;
    }

    public RuleRaw withRule(String rule){
        this.setRule(rule);
        return this;
    }

    public RuleRaw withRaw(String raw){
        this.setRaw(raw);
        return this;
    }

    @Override
    public String toString() {
        return "RuleRaw{" +
                "rule=" + rule +
                ", raw=" + raw +
                '}';
    }
}
