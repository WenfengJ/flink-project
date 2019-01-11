package com.longyun.flink;

import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Collections;
import java.util.List;

/**
 * @author yuanxiaolong
 * @ClassName com.longyun.flink.CounterSource
 * @Description TODO
 * @Date 2019/1/11 13:56
 * @Version 1.0
 **/
public class CounterSource extends RichParallelSourceFunction<Long> implements ListCheckpointed<Long> {

    /**  current offset for exactly once semantics */
    private Long offset;

    /** flag for job cancellation */
    private volatile boolean isRunning = true;

    @Override
    public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
        return Collections.singletonList(offset);
    }

    @Override
    public void restoreState(List<Long> state) throws Exception {
        for (Long s : state){
            offset = s;
        }
    }

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        final Object lock = ctx.getCheckpointLock();

        while (isRunning) {
            // output and state update are atomic
            synchronized (lock) {
                ctx.collect(offset);
                offset += 1;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
