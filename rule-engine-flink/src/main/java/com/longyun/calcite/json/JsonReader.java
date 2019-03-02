package com.longyun.calcite.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

/**
 * @author lynn
 * @ClassName com.longyun.calcite.json.JsonReader
 * @Description TODO
 * @Date 19-2-27 下午4:40
 * @Version 1.0
 **/
public class JsonReader implements Closeable {

    private Queue<String> queue;

    private ObjectMapper mapper;

    private boolean hasNext = true;

    public JsonReader(Queue<String> queue) {
        this(queue, new ObjectMapper(), true);
    }

    public JsonReader(Queue<String> queue, boolean hasNext) {
        this(queue, new ObjectMapper(), hasNext);
    }

    public JsonReader(Queue<String> queue, ObjectMapper mapper, boolean hasNext) {
        this.queue = queue;
        this.mapper = mapper;
        this.hasNext = hasNext;
    }


    /**
     * Reads the next line from the buffer and converts to a string array.
     *
     * @return a string array with each comma-separated element as a separate
     *         entry.
     *
     * @throws IOException
     *             if bad things happen during the read
     */
    public JsonNode readNext(){
        try {
            String nextLine = getNextLine();
            if (!hasNext) {
                return null; // should throw if still pending?
            }
            return mapper.readValue(nextLine, JsonNode.class);
        }catch (IOException e){
            e.printStackTrace();
        }

        return null;
    }

    /**
     * Reads the next line from the file.
     *
     * @return the next line from the file without trailing newline
     * @throws IOException
     *             if bad things happen during the read
     */
    private String getNextLine() throws IOException {
        String nextLine = queue.poll();
        if (nextLine == null) {
            hasNext = false;
        }
        return hasNext ? nextLine : null;
    }

    @Override
    public void close() throws IOException {

    }

}
