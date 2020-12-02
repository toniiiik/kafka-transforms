package sk.prosoft.kafka.connect.transform.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.io.IOException;
import java.util.Map;


public abstract class ValueToJson<R extends ConnectRecord<R>> implements Transformation<R> {

    public static class Key<R extends ConnectRecord<R>> extends ValueToJson<R> {
        @Override
        protected R newRecord(R record, Object newValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), newValue, record.valueSchema(), record.value(), record.timestamp());
        }

        @Override
        protected Schema getSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object getValue(R record) {
            return record.key();
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends ValueToJson<R> {

        @Override
        protected R newRecord(R record, Object newValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), newValue, record.timestamp());

        }

        @Override
        protected Schema getSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object getValue(R record) {
            return record.value();
        }
    }

    ValueToJsonConfig config;

    private R applyWithSchema(R r) throws IOException {
        final Struct data = (Struct)getValue(r);
        final Schema schema = getSchema(r);

        for (String f : config.fields) {
            if(data.get(f) == null){
                continue;
            }
            //try parse string to json
            //TODO: is mutable OK?
            Object oldValue = data.get(f);
            if (oldValue instanceof String){
                JsonNode newValue = new ObjectMapper().readValue((String)oldValue,JsonNode.class);
                data.put(f, newValue);
//                TODO: update schema for the field
//                schema.field(f).
            }
        }
        return r;
    }

    private R applySchemaless(R r) throws IOException {
        final Map<String,Object> data = (Map<String, Object>)getValue(r);

        for (String f : config.fields) {
            if(!data.containsKey(f)){
                continue;
            }
            //try parse string to json
            //TODO: is mutable OK?
            Object oldValue = data.get(f);
            if (oldValue instanceof String){
                JsonNode newValue = new ObjectMapper().readValue((String)oldValue,JsonNode.class);
                data.put(f, newValue);
            }
        }

        return r;
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

    protected abstract R newRecord(R record, Object newValue);

    protected abstract Schema getSchema(R record);
    protected abstract Object getValue(R record);

}
