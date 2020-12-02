package sk.prosoft.kafka.connect.transform.common;


import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ValueToJsonTest {
    private final ValueToJson<SinkRecord> xform = new ValueToJson.Value<>();

    @After
    public void teardown() {
        xform.close();
    }

    @Test
    public void schemaless() {
        xform.configure(Collections.singletonMap("fields", "json, headers"));

        final HashMap<String, Object> value = new HashMap<>();
        value.put("a", 1);
        value.put("json", "{\n  \"before\" : null,\n  \"after\" : {\n    \"insuranceCategory\" : \"MANDATORY\",\n    \"lastUpdatedBy\" : \"lekar\",\n    \"providerCode\" : \"24\",\n    \"uuid\" : \"f66e02e1-d857-4a84-87d9-4b1d6fbe52c3\",\n    \"version\" : 0,\n    \"insuranceDocumentType\" : \"EPZP_SR\",\n    \"lastUpdated\" : \"2020-06-04T07:47:16.648076974Z\",\n    \"createdDate\" : \"2020-06-04T07:47:16.648076974Z\",\n    \"deleted\" : false,\n    \"identification\" : \"456789132\",\n    \"createdBy\" : \"lekar\",\n    \"tenantId\" : 9999,\n    \"id\" : 12244,\n    \"validity\" : {\n      \"start\" : \"2020-06-04T07:47:00Z\",\n      \"end\" : null\n    },\n    \"providerName\" : null\n  }\n}");
        value.put("headers",  "{\n  \"hostname\" : \"nghis-patient-api-5795c69c45-8j4mm\",\n  \"change-type\" : \"CREATE\",\n  \"message-object-id\" : 12244,\n  \"message-event-type\" : \"Insurance\",\n  \"user-login\" : \"lekar\",\n  \"request-id\" : \"bbaa6021-0a27-48fc-935e-2b88977ae222\",\n  \"tenant-id\" : 9999,\n  \"application-id\" : \"patient-management-service\",\n  \"message-source-id\" : 12244,\n  \"user-source\" : \"{\\n  \\\"lastName\\\" : \\\"Lekar\\\",\\n  \\\"firstName\\\" : \\\"Jan\\\",\\n  \\\"enlistmentCodes\\\" : null,\\n  \\\"prefix\\\" : \\\"Phd\\\",\\n  \\\"postfix\\\" : \\\"Ing\\\",\\n  \\\"professionalCode\\\" : null,\\n  \\\"personalNumber\\\" : \\\"1111\\\",\\n  \\\"login\\\" : \\\"lekar\\\",\\n  \\\"department\\\" : null,\\n  \\\"employeeNumber\\\" : \\\"1\\\",\\n  \\\"healthcareProfessional\\\" : \\\"1\\\"\\n}\"\n}");
        value.put("c", 3);

        final SinkRecord record = new SinkRecord("", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final HashMap<String, Integer> expectedKey = new HashMap<>();
        expectedKey.put("a", 1);
        expectedKey.put("b", 2);

        assertNull(transformedRecord.valueSchema());
        assertTrue(((Map<String, Object>)transformedRecord.value()).get("json") instanceof JsonNode);
    }

    @Test
    public void withSchema() {
        xform.configure(Collections.singletonMap("fields", "a,b"));

        final Schema valueSchema = SchemaBuilder.struct()
                .field("a", Schema.INT32_SCHEMA)
                .field("json", Schema.STRING_SCHEMA)
                .field("c", Schema.INT32_SCHEMA)
                .build();

        final Struct value = new Struct(valueSchema);
        value.put("a", 1);
        value.put("json", "{\n  \"before\" : null,\n  \"after\" : {\n    \"insuranceCategory\" : \"MANDATORY\",\n    \"lastUpdatedBy\" : \"lekar\",\n    \"providerCode\" : \"24\",\n    \"uuid\" : \"f66e02e1-d857-4a84-87d9-4b1d6fbe52c3\",\n    \"version\" : 0,\n    \"insuranceDocumentType\" : \"EPZP_SR\",\n    \"lastUpdated\" : \"2020-06-04T07:47:16.648076974Z\",\n    \"createdDate\" : \"2020-06-04T07:47:16.648076974Z\",\n    \"deleted\" : false,\n    \"identification\" : \"456789132\",\n    \"createdBy\" : \"lekar\",\n    \"tenantId\" : 9999,\n    \"id\" : 12244,\n    \"validity\" : {\n      \"start\" : \"2020-06-04T07:47:00Z\",\n      \"end\" : null\n    },\n    \"providerName\" : null\n  }\n}");
        value.put("c", 3);

        final SinkRecord record = new SinkRecord("", 0, null, null, valueSchema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        //TODO: check schema is changed for json
//        final Schema expectedKeySchema = SchemaBuilder.struct()
//                .field("a", Schema.INT32_SCHEMA)
//                .field("b", Schema.INT32_SCHEMA)
//                .build();
//
//        final Struct expectedKey = new Struct(expectedKeySchema)
//                .put("a", 1)
//                .put("b", 2);
//
//        assertEquals(expectedKeySchema, transformedRecord.keySchema());
//        assertEquals(expectedKey, transformedRecord.key());

        assertTrue(((Map<String, Object>)transformedRecord.value()).get("json") instanceof JsonNode);
    }

}