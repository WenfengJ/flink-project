package com.longyun.calcite.json;

import com.esotericsoftware.kryo.util.ObjectMap;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * @author lynn
 * @ClassName com.longyun.calcite.json.JsonTplParser
 * @Description TODO
 * @Date 19-3-2 下午12:22
 * @Version 1.0
 **/
public class JsonTplParser implements Closeable, Serializable {

    public static final String DEFAULT_LAYER_SPEARATOR = "__";

    private final String tplString;

    private ObjectMapper mapper;

    public JsonTplParser(String tplString) {
        this(tplString, new ObjectMapper());
    }

    public JsonTplParser(String tplString, ObjectMapper mapper) {
        this.tplString = tplString;
        this.mapper = mapper;
    }

    @Override
    public void close() throws IOException {

    }

    /**
     *
     * @return
     */
    public String[] readFieldTypes(){
        try {
            JsonNode jsonNode = mapper.readTree(tplString);
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
