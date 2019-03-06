package com.longyun.flink.java.res;

import com.google.common.collect.Sets;
import com.longyun.flink.java.res.event.AddEvent;
import com.longyun.flink.java.res.event.EventListener;
import com.longyun.flink.java.res.event.RemoveEvent;

import java.io.Serializable;
import java.util.Set;

/**
 * @author lynn
 * @ClassName com.longyun.flink.java.res.RuleContainer
 * @Description TODO
 * @Date 19-3-6 上午8:48
 * @Version 1.0
 **/
public class RuleContainer<Tag, R> implements Serializable {

    private final Set<Rule<Tag>> rules;

    private EventListener<R> listener;

    public RuleContainer() {
        this(Sets.newLinkedHashSet());
    }

    private RuleContainer(Set<Rule<Tag>> rules) {
        this.rules = rules;
    }


    public Set<Rule<Tag>> getRules() {
        return rules;
    }

    public boolean add(Rule rule){
        if(rules.add(rule)){
            this.listener.handle(new AddEvent<Tag>(rule));
            return true;
        }

        return false;
    }

    public boolean remove(Rule rule){
        if(rules.remove(rule)){
            this.listener.handle(new RemoveEvent(rule));
            return true;
        }

        return false;
    }

    public void addListener(EventListener<R> listener){
        System.out.println("---add EventListener---");
        this.listener = listener;
    }
}
