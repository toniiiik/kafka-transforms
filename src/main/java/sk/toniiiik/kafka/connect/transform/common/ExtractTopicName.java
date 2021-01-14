package sk.toniiiik.kafka.connect.transform.common;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;


public abstract class ExtractTopicName<R extends ConnectRecord<R>> implements Transformation<R> {

    public static class Key<R extends ConnectRecord<R>> extends ExtractTopicName<R> {
        @Override
        protected R newRecord(R record, String topic) {
            return record.newRecord(topic, record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), record.value(), record.timestamp());
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

    public static class Value<R extends ConnectRecord<R>> extends ExtractTopicName<R> {

        @Override
        protected R newRecord(R record, String topic) {
            return record.newRecord(topic, record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), record.value(), record.timestamp());

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

    ExtractTopicNameConfig config;

    @Override
    public R apply(R r) {

        String newTopic = r.topic();

        Schema schema = getSchema(r);
        Object value = getValue(r);

        //if no value handle as si
        if (value == null){
            return newRecord(r, r.topic());
        }
        // no schema value is Map
        if(schema == null){
           final Map<String,Object> data = (Map<String, Object>)getValue(r);
           newTopic = (String)data.get(config.field);
        }else {
            final Struct data = (Struct) getValue(r);
            newTopic = (String)data.get(config.field);
        }

        if(newTopic == null){
            return newRecord(r, r.topic());
        }

        if (config.prefix == null){
            config.prefix = "";
        }

        newTopic = config.prefix + newTopic;

        return newRecord(r, newTopic);
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

        this.config = new ExtractTopicNameConfig(settings);
    }

    protected abstract R newRecord(R record, String updatedTopic);

    protected abstract Schema getSchema(R record);
    protected abstract Object getValue(R record);

}
