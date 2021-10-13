package sk.toniiiik.kafka.connect.transform.common;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.naming.ConfigurationException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.stream.StreamSupport;

import static org.junit.Assert.*;

public class CloudEventHeadersTest {

    private final CloudEventHeaders<SinkRecord> xform = new CloudEventHeaders<>();
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();


    private String getData(){
        String stringData = "{\n  \"before\" : null,\n  \"after\" : {\n    \"insuranceCategory\" : \"MANDATORY\",\n    \"lastUpdatedBy\" : \"lekar\",\n    \"providerCode\" : \"24\",\n    \"uuid\" : \"f66e02e1-d857-4a84-87d9-4b1d6fbe52c3\",\n    \"version\" : 0,\n    \"insuranceDocumentType\" : \"EPZP_SR\",\n    \"lastUpdated\" : \"2020-06-04T07:47:16.648076974Z\",\n    \"createdDate\" : \"2020-06-04T07:47:16.648076974Z\",\n    \"deleted\" : false,\n    \"identification\" : \"456789132\",\n    \"createdBy\" : \"lekar\",\n    \"tenantId\" : 9999,\n    \"id\" : 12244,\n    \"validity\" : {\n      \"start\" : \"2020-06-04T07:47:00Z\",\n      \"end\" : null\n    },\n    \"providerName\" : null\n  }\n}";
        return stringData;
    }

    @After
    public void doAfter(){
        xform.close();
    }

    @Test
    public void additionalPlacement_OK(){
        xform.configure(Collections.singletonMap(CloudEventHeadersConfig.CE_ADDITIONAL_PLACEMENT, "1:INT:header:ce_tenantid"));

        final SinkRecord record = new SinkRecord("", 0, null, null, null, getData(), 0);
        final SinkRecord cloudEvent = xform.apply(record);

        assertTrue(StreamSupport.stream(cloudEvent.headers().spliterator(), true).anyMatch(h->h.key().equals("ce_tenantid")));
        assertTrue(StreamSupport.stream(cloudEvent.headers().spliterator(), true).anyMatch(h->h.key().equals("ce_type")));
    }

    @Test
    public void ceTypeIsDefined_OK(){
        xform.configure(Collections.singletonMap(CloudEventHeadersConfig.CE_TYPE_CONFIG, "ObservationCreated"));

        final SinkRecord record = new SinkRecord("", 0, null, null, null, getData(), 0);
        final SinkRecord cloudEvent = xform.apply(record);

        assertTrue(StreamSupport.stream(cloudEvent.headers().spliterator(), true).anyMatch(h->h.key().equals("ce_type")));
    }

    @Test
    public void configureAdditionalPlacement_NOK() throws ConfigException {
        expectedEx.expect(ConfigException.class);
        expectedEx.expectMessage("E: type of the placement ce_tenantid is not valid. Got String");

        xform.configure(Collections.singletonMap(CloudEventHeadersConfig.CE_ADDITIONAL_PLACEMENT, "1:String:header:ce_tenantid"));
    }

    @Test
    public void shouldBeDateAdditionalPlacement_OK() {

        xform.configure(Collections.singletonMap(CloudEventHeadersConfig.CE_ADDITIONAL_PLACEMENT, "now:DATE:header:ce_date"));
        final SinkRecord record = new SinkRecord("", 0, null, null, null, getData(), 0);
        final SinkRecord cloudEvent = xform.apply(record);

        String value = (String)StreamSupport.stream(cloudEvent.headers().spliterator(), true).filter(h->h.key().equals("ce_date")).findFirst().get().value();
        DateFormat formatter = new SimpleDateFormat("YYYY-MM-dd");
        Date expect =Date.from(Instant.now());
        assertEquals(formatter.format(expect), value);
    }

    @Test
    public void shouldBeDateTimeAdditionalPlacement_OK() {
        xform.configure(Collections.singletonMap(CloudEventHeadersConfig.CE_ADDITIONAL_PLACEMENT, "now:DATETIME:header:ce_datetime"));
        final SinkRecord record = new SinkRecord("", 0, null, null, null, getData(), 0);
        final SinkRecord cloudEvent = xform.apply(record);

        String value = (String)StreamSupport.stream(cloudEvent.headers().spliterator(), true).filter(h->h.key().equals("ce_datetime")).findFirst().get().value();
        assertTrue(Instant.now().isAfter(Instant.parse(value)));
    }
}
