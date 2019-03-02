package com.longyun.calcite.json;

import com.google.common.collect.Maps;

import java.io.*;
import java.util.*;


/**
 * @author lynn
 * @ClassName com.longyun.calcite.json.MemorySource
 * @Description TODO
 * @Date 19-3-2 上午11:30
 * @Version 1.0
 **/
public class MemorySource<E> implements Serializable {

    private final Map<String, Queue<E>> memoryMap;

    public MemorySource(){
        this(Maps.newHashMap());
    }

    public MemorySource(Map<String, Queue<E>> memoryMap) {
        this.memoryMap = memoryMap;
    }

    public void offer(String table, E elem){
        Queue<E> queue = getQueue(table);
        queue.offer(elem);
    }

    public Queue<E> getQueue(String table){
        if(memoryMap.containsKey(table)){
            return memoryMap.get(table);
        }else {
            Queue<E> queue = new LinkedList<>();
            memoryMap.putIfAbsent(table, queue);
            return queue;
        }
    }

}
