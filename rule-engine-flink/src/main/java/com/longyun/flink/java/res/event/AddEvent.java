package com.longyun.flink.java.res.event;

import com.longyun.flink.java.res.Rule;

import java.io.Serializable;

/**
 * @author lynn
 * @ClassName com.longyun.flink.java.res.event.AddEvent
 * @Description TODO
 * @Date 19-3-6 上午8:54
 * @Version 1.0
 **/
public class AddEvent<Tag> implements Event, Serializable {

    private Rule<Tag> elem;

    public AddEvent(Rule<Tag> elem) {
        this.elem = elem;
    }

    @Override
    public Object getElem() {
        return elem;
    }
}
