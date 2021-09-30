package sk.toniiiik.kafka.connect.transform.common;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class CloudEventHeaders<R extends ConnectRecord<R>> implements Transformation<R> {

	public static final String CONTENT_TYPE_HEADER_NAME = "content-type";
	public static final String CE_ID_HEADER_NAME = "ce_id";
	public static final String CE_SPECVERSION_HEADER_NAME = "ce_specversion";
	public static final String CE_SOURCE_HEADER_NAME = "ce_source";
	public static final String CE_TYPE_HEADER_NAME = "ce_type";

	private SchemaAndValue contentType;
	private SchemaAndValue cloudEventSpecVersion;
	private SchemaAndValue cloudEventSource;
	private SchemaAndValue cloudEvenType;

	private List additionalPlacements = null;

	@Override
	public void configure(Map<String, ?> configs) {
		CloudEventHeadersConfig config = new CloudEventHeadersConfig(configs);
		contentType = Values.parseString(config.contentType);
		cloudEventSpecVersion = new SchemaAndValue(Schema.STRING_SCHEMA,config.cloudEventSpecVersion);
		if (config.cloudEventSource != null) {
			cloudEventSource = Values.parseString(config.cloudEventSource);
		}
		if(config.cloudEventsType != null) {
			cloudEvenType = Values.parseString(config.cloudEventsType);
		}
		this.additionalPlacements = config.additionalPlacements;
	}

	@Override
	public R apply(R record) {
		if (record == null) {
			return record;
		}
		// Copy the existing headers
		Headers newHeaders = new ConnectHeaders(record.headers());
		// Add the new header only if there is NOT a header with the same name
		addHeaderIfNotPresent(newHeaders, CONTENT_TYPE_HEADER_NAME, contentType);
		addHeaderIfNotPresent(newHeaders, CE_ID_HEADER_NAME, Values.parseString(UUID.randomUUID().toString()));
		addHeaderIfNotPresent(newHeaders, CE_SPECVERSION_HEADER_NAME, cloudEventSpecVersion);
		addHeaderIfNotPresent(newHeaders, CE_SOURCE_HEADER_NAME, cloudEventSource);
		if(cloudEvenType != null && !cloudEvenType.value().equals(StringUtils.EMPTY)){
			addHeaderIfNotPresent(newHeaders, CE_TYPE_HEADER_NAME, cloudEvenType);
		}

		processAdditionalPlacements(record, newHeaders);

		return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), record.value(),
				record.timestamp(), newHeaders);
	}

	private void processAdditionalPlacements(R record, Headers newHeaders) {
		if( this.additionalPlacements == null){
			return;
		}

		for (Object p :
				this.additionalPlacements) {
			String[] parts = ((String)p).split(":");
			CloudEventHeadersConfig.Placement placement = CloudEventHeadersConfig.Placement.fromParts(parts);
			addHeaderIfNotPresent(newHeaders, placement.alias, placement.asSchemaAndValue());
		}

	}

	private void addHeaderIfNotPresent(Headers newHeaders, String key, SchemaAndValue value) {
		if (value != null && newHeaders.lastWithName(key) == null) {
			newHeaders.add(key, value);
		}
	}

	@Override
	public ConfigDef config() {
		return CloudEventHeadersConfig.config();
	}

	@Override
	public void close() {
		// nothing to do here
	}

}
