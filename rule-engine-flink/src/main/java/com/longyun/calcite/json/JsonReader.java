package com.longyun.calcite.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.util.*;

/**
 * @author lynn
 * @ClassName com.longyun.calcite.json.JsonReader
 * @Description TODO
 * @Date 19-2-27 下午4:40
 * @Version 1.0
 **/
public class JsonReader implements Closeable {

    private Queue<String> jsonLines;

    private ObjectMapper mapper;

    private boolean hasNext = true;

    public static final String DEFAULT_LAYER_SPEARATOR = "__";

    public JsonReader(Queue<String> jsonLines) {
        this(jsonLines, new ObjectMapper(), true);
    }

    public JsonReader(Queue<String> jsonLines, boolean hasNext) {
        this(jsonLines, new ObjectMapper(), hasNext);
    }

    public JsonReader(Queue<String> jsonLines, ObjectMapper mapper, boolean hasNext) {
        this.jsonLines = jsonLines;
        this.mapper = mapper;
        this.hasNext = hasNext;
    }

    /**
     * Reads the entire file into a List with each element being a String[] of
     * tokens.
     *
     * @return a List of String[], with each String[] representing a line of the
     *         file.
     *
     * @throws IOException
     *             if bad things happen during the read
     */
    public List<JsonNode> readAll() throws IOException {

        List<JsonNode> allElements = new ArrayList<>();
        while (hasNext) {
            JsonNode node = readNext();
            if (node != null)
                allElements.add(node);
        }
        return allElements;

    }

    /**
     *
     * @return
     */
    public String[] readFieldTypes(){
        try {
            String nextLine = jsonLines.peek();
            JsonNode jsonNode = mapper.readTree(nextLine);
            StringBuilder sb = new StringBuilder();
            visitTypes(jsonNode, new LinkedList<String>(), 0, sb);
            if(sb.charAt(sb.length() - 1) == ','){
                sb.deleteCharAt(sb.length() - 1);
            }

            return sb.toString().split(",");
        }catch (IOException e){
            e.printStackTrace();
        }

        return null;
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
        String nextLine = jsonLines.poll();
        if (nextLine == null) {
            hasNext = false;
        }
        return hasNext ? nextLine : null;
    }

    @Override
    public void close() throws IOException {

    }

    private void visitTypes(JsonNode node, Queue<String> ancestors, int depth, StringBuilder sb){
        if(null == node) {
            return;
        }
        switch (node.getNodeType()){
            case OBJECT:
                Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
                depth++;
                while (fields.hasNext()){
                    Map.Entry<String, JsonNode> field = fields.next();
                    ancestors.offer(field.getKey());
                    visitTypes(field.getValue(), ancestors, depth, sb);
                }
                break;
            case BINARY:
            case NUMBER:
                retriveAncestors(ancestors, sb, depth);
                if(node.isInt()){
                    sb.append("int").append(",");
                }else if(node.isShort()){
                    sb.append("short").append(",");
                }else if(node.isFloat()){
                    sb.append("float").append(",");
                }else if(node.isDouble()){
                    sb.append("double").append(",");
                }else if(node.isBigInteger()){
                    sb.append("biginteger").append(",");
                }else if(node.isBigDecimal()){
                    sb.append("decimal").append(",");
                }
                break;
            case STRING:
                retriveAncestors(ancestors, sb, depth);
                sb.append("string").append(",");
                break;
            case BOOLEAN:
                retriveAncestors(ancestors, sb, depth);
                sb.append("boolean").append(",");
                break;
            case POJO:
            case ARRAY:
                retriveAncestors(ancestors, sb, depth);
                sb.append("array").append(",");
                break;
            case NULL:
            case MISSING:
                break;
        }
    }

    private void retriveAncestors(Queue<String> ancestors, StringBuilder sb, int depth){
        if(depth == 1){
            String ancestor = "";
            while (!ancestors.isEmpty()){
                ancestor = ancestors.poll();
            }
            sb.append(ancestor);
        }else{
            String ancestor = ancestors.poll();
            sb.append(ancestor).append(DEFAULT_LAYER_SPEARATOR);
            ancestors.offer(ancestor);
            for (int i = 0; i < depth - 1; i++) {
                sb.append(ancestors.poll()).append(DEFAULT_LAYER_SPEARATOR);
            }
        }
        if(sb.toString().endsWith(DEFAULT_LAYER_SPEARATOR)){
            sb.delete(sb.length() - DEFAULT_LAYER_SPEARATOR.length(), sb.length());
        }
        sb.append(":");
    }

}
