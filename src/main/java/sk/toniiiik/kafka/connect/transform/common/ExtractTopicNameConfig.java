package sk.toniiiik.kafka.connect.transform.common;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class ExtractTopicNameConfig extends AbstractConfig {

  String field;
  String prefix;

  public static final String FIELD_CONFIG = "field";
  static final String FIELD_DOC = "The Message field to be used as topic name";
  public static final String PREFIX_CONFIG = "prefix";
  static final String PREFIX_DOC = "Topic prefix";

  public ExtractTopicNameConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.field = this.getString(FIELD_CONFIG);
    this.prefix = this.getString(PREFIX_CONFIG);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, FIELD_CONFIG)
        .define(PREFIX_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, PREFIX_CONFIG);
  }
}
