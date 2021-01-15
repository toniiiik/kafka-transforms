package sk.toniiiik.kafka.connect.transform.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class ValueToJson<R extends ConnectRecord<R>> implements Transformation<R> {

    ValueToJsonConfig config;

    private R applyWithSchema(R r) throws IOException {
        if(config.fields.isEmpty() && r.value() instanceof String){
            return newRecord(r, null, toMap((String)r.value()));
        }

        final Struct data = (Struct)getValue(r);
        final Schema schema = getSchema(r);

        if(config.fields.isEmpty()){
            return r;
        }

        for (String f : config.fields) {
            if(data.get(f) == null){
                continue;
            }
            //try parse string to json
            //TODO: is mutable OK?
            Object oldValue = data.get(f);
            if (oldValue instanceof String){
                data.put(f, toMap((String)oldValue));
//                TODO: update schema for the field with infermethod
            }
        }
        return newRecord(r, null, r.value());
    }

    private R applySchemaless(R r) throws IOException {
        if(config.fields.isEmpty() && r.value() instanceof String){
            return newRecord(r, null, toMap((String)r.value()));
        }

        final Map<String,Object> data = (Map<String, Object>)getValue(r);

        for (String f : config.fields) {
            if(!data.containsKey(f)){
                continue;
            }
            //try parse string to json
            //TODO: is mutable OK?
            Object oldValue = data.get(f);
            if (oldValue instanceof String){
                data.put(f, toMap((String)oldValue));
            }
        }
        return newRecord(r, null, r.value());
    }

    @Override
    public R apply(R r) {
        // no schema, value is Map just change
        try {
            if (getSchema(r) == null) {
                return applySchemaless(r);

            } else {
                return applyWithSchema(r);
            }
        }
        catch(IOException ex){
            ex.printStackTrace();
            return r;
        }
    }

    @Override
    public ConfigDef config() {
        return ExtractTopicNameConfig.config();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> settings) {

        this.config = new ValueToJsonConfig(settings);
    }

    protected Schema getSchema(R record){
        return record.valueSchema();
    }
    protected  Object getValue(R record) {
        return record.value();
    }

    protected R newRecord(R r, Schema newSchema, Object newValue){
        return r.newRecord(r.topic(), r.kafkaPartition(), r.keySchema(), r.key(), newSchema, newValue, r.timestamp());
    }

    private HashMap<String, Object> toMap(String jsonData) throws JsonProcessingException {
        final ObjectMapper mapper = new ObjectMapper();
        final MapType type = mapper.getTypeFactory().constructMapType(
                Map.class, String.class, Object.class);
        HashMap<String,Object> newValue = mapper.readValue(jsonData, type);
        return newValue;
    }

    private Schema inferSchema(JsonNode jsonValue) {
        switch (jsonValue.getNodeType()) {
            case NULL:
                return Schema.OPTIONAL_STRING_SCHEMA;
            case BOOLEAN:
                return Schema.BOOLEAN_SCHEMA;
            case NUMBER:
                if (jsonValue.isIntegralNumber()) {
                    return Schema.INT64_SCHEMA;
                }
                else {
                    return Schema.FLOAT64_SCHEMA;
                }
            case ARRAY:
                SchemaBuilder arrayBuilder = SchemaBuilder.array(jsonValue.elements().hasNext() ? inferSchema(jsonValue.elements().next()) : Schema.OPTIONAL_STRING_SCHEMA);
                return arrayBuilder.build();
            case OBJECT:
                SchemaBuilder structBuilder = SchemaBuilder.struct();
                Iterator<Map.Entry<String, JsonNode>> it = jsonValue.fields();
                while (it.hasNext()) {
                    Map.Entry<String, JsonNode> entry = it.next();
                    structBuilder.field(entry.getKey(), inferSchema(entry.getValue()));
                }
                return structBuilder.build();
            case STRING:
                return Schema.STRING_SCHEMA;
            case BINARY:
            case MISSING:
            case POJO:
            default:
                return null;
        }
    }
}
