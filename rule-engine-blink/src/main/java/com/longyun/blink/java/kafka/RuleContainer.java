package com.longyun.blink.java.kafka;

import org.apache.flink.api.common.accumulators.Accumulator;

import java.util.TreeSet;


public class RuleContainer implements Accumulator<String, TreeSet<String>> {

    private TreeSet<String> rules = new TreeSet<>();

    @Override
    public void add(String value) {
        rules.add(value);
    }

    @Override
    public TreeSet<String> getLocalValue() {
        return rules;
    }

    @Override
    public void resetLocal() {
        rules.clear();
    }

    @Override
    public void merge(Accumulator<String, TreeSet<String>> other) {
        if(null != other && other.getLocalValue().size() > 0){
            this.rules.addAll(other.getLocalValue());
        }
    }

    @Override
    public Accumulator<String, TreeSet<String>> clone() {
        RuleContainer container = new RuleContainer();
        container.rules = this.rules;
        return container;
    }
}
