package com.longyun.flink.java.res.event;

/**
 * @author lynn
 * @InterfaceName com.longyun.flink.java.res.event.EventListener
 * @Description TODO
 * @Date 19-3-6 上午8:36
 * @Version 1.0
 **/
public interface EventListener<R> {

    R handle(Event event);
}
