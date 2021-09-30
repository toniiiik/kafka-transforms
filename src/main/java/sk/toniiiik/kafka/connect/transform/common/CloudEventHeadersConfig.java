package sk.toniiiik.kafka.connect.transform.common;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;

public class CloudEventHeadersConfig extends AbstractConfig {

	public static final String CONTENT_TYPE_CONFIG = "contenttype";
	public static final String CE_SPECVERSION_CONFIG = "cespecversion";
	public static final String CE_SOURCE_CONFIG = "cesource";
	public static final String CE_TYPE_CONFIG = "cetype";
	public static final String CE_ADDITIONAL_PLACEMENT = "ce.additional.placement";

	String contentType;
	String cloudEventSpecVersion;
	String cloudEventSource;
	String cloudEventsType;
	List additionalPlacements;

	public CloudEventHeadersConfig(Map<?, ?> originals) {
		super(config(), originals);

		this.contentType = getString(CONTENT_TYPE_CONFIG);
		this.cloudEventSpecVersion = getString(CE_SPECVERSION_CONFIG);
		this.cloudEventSource = getString(CE_SOURCE_CONFIG);
		this.additionalPlacements = getList(CE_ADDITIONAL_PLACEMENT);
	}
	public static enum PlacementType {
		STRING,
		INT,
		NUMBER,
		DATE,
		TIME,
		DATETIME,
	};

	static class Placement{

		String value;
		PlacementType type;
		String placement;
		String alias;

		Placement(){

		}

		static void validate(String[] parts){
			if (parts.length != 4){
				throw new ConfigException(CE_ADDITIONAL_PLACEMENT, parts, "Placement must consists of <value>:<type>:[header|data]:<alias>");
			}

			StringBuilder errors = new StringBuilder();

			try {
				PlacementType.valueOf(parts[1]);
			}
			catch (IllegalArgumentException ex) {
				errors.append(String.format("E: type of the placement %s is not valid. Got %s", parts[3], parts[1]));
			}
			if( !parts[2].equals("header") && !parts[2].equals("data")){
				errors.append(String.format("E: placement must be one of 'header|data'. Got %s", parts[2]));
			}
			if(errors.length() > 0){
				throw new ConfigException(CE_ADDITIONAL_PLACEMENT, parts, errors.toString());
			}
		}

		static Placement fromParts(String[] parts){
			Placement result = new Placement();
			result.alias = parts[3];
			result.value = parts[0];
			result.placement = parts[2];
			result.type = PlacementType.valueOf(parts[1]);
			return result;
		}

		public SchemaAndValue asSchemaAndValue(){
			switch (this.type){
				case DATE:
					 if(value.equals("now")){
					 	DateFormat formatter = new SimpleDateFormat("YYYY-MM-dd");
					 	value = formatter.format(Date.from(Instant.now()));
					 }
				case DATETIME:
					if(value.equals("now")){
//						DateFormat formatter = new SimpleDateFormat("YYYY-MM-ddTHH:mm:ss.SSS");
						value = Instant.now().toString();
					}
				case TIME:
					if(value.equals("now")){
						DateFormat formatter = new SimpleDateFormat("HH:mm:ss.SSS");
						value = formatter.format(Date.from(Instant.now()));
					}
				case STRING:
					return new SchemaAndValue(Schema.STRING_SCHEMA, this.value);
				case INT:
					return new SchemaAndValue(Schema.INT32_SCHEMA, Integer.parseInt(this.value));
				case NUMBER:
					return new SchemaAndValue(Schema.FLOAT32_SCHEMA, Float.parseFloat(this.value));
				default:
					throw new ConfigException(CE_ADDITIONAL_PLACEMENT, this, "Type is not specified: " + this.type.toString());
			}
		}
	}

	private static class ListValidator implements ConfigDef.Validator {
		public ListValidator() {
		}
		@Override
		public void ensureValid(String s, Object o) {
			if ( null == o){
				return;
			}
			for (Object placement :
					(Collection)o) {
				String[] parts = ((String)placement).split(":");
				Placement.validate(parts);
			}
		}
	}

	public static ConfigDef config() {
		return new ConfigDef()//
				.define(CONTENT_TYPE_CONFIG, ConfigDef.Type.STRING, "application/cloudevents+json", ConfigDef.Importance.HIGH,
						"Content type of the cloud event value (default 'application/cloudevents+json')")
				.define(CE_SPECVERSION_CONFIG, ConfigDef.Type.STRING, "1.0", ConfigDef.Importance.HIGH,
						"Specification version of the cloud event value (default '1.0')")
				.define(CE_SOURCE_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, "Source of the cloud event")
				.define(CE_TYPE_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
						"Type of the message, if null or empty string is presented header will be omitted.")
				.define(CE_ADDITIONAL_PLACEMENT,
						ConfigDef.Type.LIST,
						null,
						new ListValidator(),
						ConfigDef.Importance.HIGH,
						"Extra fields can be added as part of the event as data or custom ce_* attribute as message header. <value>:<type>:[header|data]:<alias>",
						(String)null,
						-1,
						ConfigDef.Width.MEDIUM,
						"Setting for additional fields in header or payload of cloud event"
						);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((cloudEventSource == null) ? 0 : cloudEventSource.hashCode());
		result = prime * result + ((cloudEventSpecVersion == null) ? 0 : cloudEventSpecVersion.hashCode());
		result = prime * result + ((contentType == null) ? 0 : contentType.hashCode());
		result = prime * result + ((cloudEventsType == null) ? 0 : cloudEventsType.hashCode());
 		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		CloudEventHeadersConfig other = (CloudEventHeadersConfig) obj;
		if (cloudEventSource == null) {
			if (other.cloudEventSource != null) {
				return false;
			}
		} else if (!cloudEventSource.equals(other.cloudEventSource)) {
			return false;
		}
		if (cloudEventSpecVersion == null) {
			if (other.cloudEventSpecVersion != null) {
				return false;
			}
		} else if (!cloudEventSpecVersion.equals(other.cloudEventSpecVersion)) {
			return false;
		}
		if (contentType == null) {
			if (other.contentType != null) {
				return false;
			}
		} else if (!contentType.equals(other.contentType)) {
			return false;
		}
		if(cloudEventsType == null) {
			if(other.cloudEventsType != null) {
				return false;
			}
		} else if (!cloudEventsType.equals(other.cloudEventsType)){
			return false;
		}
		return true;
	}

}
