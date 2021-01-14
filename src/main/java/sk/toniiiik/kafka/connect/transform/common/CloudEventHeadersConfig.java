package sk.toniiiik.kafka.connect.transform.common;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class CloudEventHeadersConfig extends AbstractConfig {

	public static final String CONTENT_TYPE_CONFIG = "contenttype";
	public static final String CE_SPECVERSION_CONFIG = "cespecversion";
	public static final String CE_SOURCE_CONFIG = "cesource";

	String contentType;
	String cloudEventSpecVersion;
	String cloudEventSource;

	public CloudEventHeadersConfig(Map<?, ?> originals) {
		super(config(), originals);

		this.contentType = getString(CONTENT_TYPE_CONFIG);
		this.cloudEventSpecVersion = getString(CE_SPECVERSION_CONFIG);
		this.cloudEventSource = getString(CE_SOURCE_CONFIG);
	}

	public static ConfigDef config() {
		return new ConfigDef()//
				.define(CONTENT_TYPE_CONFIG, ConfigDef.Type.STRING, "application/cloudevents+json", ConfigDef.Importance.HIGH,
						"Content type of the cloud event value (default 'application/cloudevents+json')")
				.define(CE_SPECVERSION_CONFIG, ConfigDef.Type.STRING, "1.0", ConfigDef.Importance.HIGH,
						"Specification version of the cloud event value (default '1.0')")
				.define(CE_SOURCE_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, "Source of the cloud event");
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((cloudEventSource == null) ? 0 : cloudEventSource.hashCode());
		result = prime * result + ((cloudEventSpecVersion == null) ? 0 : cloudEventSpecVersion.hashCode());
		result = prime * result + ((contentType == null) ? 0 : contentType.hashCode());
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
		return true;
	}

}
