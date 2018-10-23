package com.tm.kafka.connect.rest.converter.source;

import com.tm.kafka.connect.rest.RestSourceConnectorConfig;
import com.tm.kafka.connect.rest.http.payload.JSONPayload;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SourceJSONPayloadConverterTest {

	SourceJSONPayloadConverter subject;

  @Before
  public void setUp() {
    subject = new SourceJSONPayloadConverter();
    Map<String,String> configs = new HashMap<>();
    configs.put("rest.source.properties", "Content-Type:application/json,Accept:application/json");
    configs.put("rest.source.url", "https://blockchain.info/ticker");
    configs.put("rest.source.destination.topics", "test");
    
    subject.start(new RestSourceConnectorConfig(configs));
  }

  @Test
  public void shouldConvertJsonMessage() throws Exception {
    String json = "{\"key1\":\"val1\",\"key2\":\"val2\"}";
  

    List<SourceRecord> records = subject.convert(json.getBytes());
    assertNotNull(records);
    assertEquals(1,records.size());
   
  }



 /**
  public void shouldReturnEmptyJsonObjectIfStringIsUnparsable() throws Exception {
    String json = "{ \"corrupted json\" ";
    SinkRecord record = mock(SinkRecord.class);
    when(record.value()).thenReturn(json);

    JSONPayload converted = subject.convert(record);

    assertEquals("{}", converted.asString());
  }
*/
}
