package com.longyun.flink.java.res.event;

/**
 * @author lynn
 * @InterfaceName com.longyun.flink.java.res.event.Event
 * @Description TODO
 * @Date 19-3-6 上午8:35
 * @Version 1.0
 **/
public interface Event<E> {

    E getElem();
}
