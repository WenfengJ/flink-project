package com.longyun.flink.java.res.event;

import com.longyun.flink.java.res.Rule;

import java.io.Serializable;

/**
 * @author lynn
 * @ClassName com.longyun.flink.java.res.event.RemoveEvent
 * @Description TODO
 * @Date 19-3-6 上午8:57
 * @Version 1.0
 **/
public class RemoveEvent<Tag> implements Event, Serializable {

    private Rule<Tag> elem;

    public RemoveEvent(Rule<Tag> elem) {
        this.elem = elem;
    }

    @Override
    public Object getElem() {
        return elem;
    }
}
