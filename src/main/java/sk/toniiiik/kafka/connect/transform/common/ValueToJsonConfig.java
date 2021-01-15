package sk.toniiiik.kafka.connect.transform.common;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ValueToJsonConfig extends AbstractConfig {

  List<String> fields;

  public static final String FIELDS_CONFIG = "fields";
  static final String FIELD_DOC = "The Message fields to be converted to json object from string";

  public ValueToJsonConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.fields = this.getList(FIELDS_CONFIG);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(FIELDS_CONFIG, ConfigDef.Type.LIST, "", ConfigDef.Importance.HIGH, FIELD_DOC);
  }
}
