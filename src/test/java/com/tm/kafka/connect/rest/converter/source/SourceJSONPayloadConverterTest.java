package com.tm.kafka.connect.rest.converter.source;

import com.tm.kafka.connect.rest.RestSourceConnectorConfig;
import com.tm.kafka.connect.rest.http.payload.JSONPayload;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

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
    
    //coverage
    json = "{\"key1\":\"val1\",\"key2\":\"val2\",\"key3\":true,\"key4\":12345,\"key5\":1.2,\"key6\":[],\"key7\":null}";
    

    records = subject.convert(json.getBytes());
    assertNotNull(records);
    assertEquals(1,records.size());
   
  }
  
  @Test
  public void convertWithSchema() throws Exception {
	  
	  SourceJSONPayloadConverter convertor = new SourceJSONPayloadConverter();
	    Map<String,String> configs = new HashMap<>();
	    configs.put("rest.source.properties", "Content-Type:application/json,Accept:application/json");
	    configs.put("rest.source.url", "https://blockchain.info/ticker");
	    configs.put("rest.source.destination.topics", "test");
	    configs.put("rest.source.payload.json.schema", "{\"type\":\"struct\",\"name\":\"toto\",\"fields\":[{\"field\":\"key1\",\"type\":\"string\",\"optional\":true},{\"field\":\"key2\",\"type\":\"string\",\"optional\":false},{\"field\":\"key3\",\"type\":\"string\",\"optional\":true}]}" 
	    		);
	   // {"type":"struct","name":"toto","fields":[{"field":"key1","type":"string","optional":true},{"field":"key2","type":"string","optional":false},{"field":"key3","type":"int32","optional":true}]}
	    convertor.start(new RestSourceConnectorConfig(configs));
	   
	    //optional value key3 not set
	    String json = "{\"key1\":\"val1\",\"key2\":\"val2\"}";
	    
	    List<SourceRecord> records = convertor.convert(json.getBytes());
	    assertNotNull(records);
	    assertEquals(1,records.size());
	    
	    SourceRecord rec = records.get(0);
	    assertNotNull(rec.valueSchema());
	    
	    JsonConverter jConv = new JsonConverter();
	    jConv.configure(new HashMap<>(),false);
	    jConv.fromConnectData("dummy", rec.valueSchema(), rec.value());
	    
	    //non optional key2 not set
	    json = "{\"key3\":12345}";
	    try {
	    	records = convertor.convert(json.getBytes());
	    	assertTrue(false);
	    }catch (DataException e) {
	    	assertTrue(e.getMessage(),Pattern.matches(".*required STRING.*", e.getMessage()));
	    }
	    //test int16
	    json = "{\"key3\":12345,\"key2\":\"val2\"}";
	    records = convertor.convert(json.getBytes());
	     rec = records.get(0);
	    //too much fields
	     json = "{\"key4\":\"val1\",\"key2\":\"val2\"}";
	     records = convertor.convert(json.getBytes());
	     rec = records.get(0);
	     byte[] data = jConv.fromConnectData("dummy", rec.valueSchema(), rec.value());
	     String finalJson = new String(data);
	     //the field is ignored
	     assertFalse(Pattern.matches(".*key4.*", finalJson));
	  
	  
  }
  
  @Test
  public void convertWithBitcoinRateSchema() throws IOException {
	  SourceJSONPayloadConverter convertor = new SourceJSONPayloadConverter();
	    Map<String,String> configs = new HashMap<>();
	    configs.put("rest.source.properties", "Content-Type:application/json,Accept:application/json");
	    configs.put("rest.source.url", "https://blockchain.info/ticker");
	    configs.put("rest.source.destination.topics", "test");
	  
	   
	    configs.put("rest.source.payload.json.schema", getData("bitcoinRateSchema.json")); 
	    		
	    convertor.start(new RestSourceConnectorConfig(configs));
	    
	    String json = getData("bitcoinRate.json");
	    
	    List<SourceRecord> records = convertor.convert(json.getBytes());
	    assertNotNull(records);
	    assertEquals(1,records.size());
	    
	    SourceRecord rec = records.get(0);
	    assertNotNull(rec.valueSchema());
	    
	    JsonConverter jConv = new JsonConverter();
	    jConv.configure(new HashMap<>(),false);
	    jConv.fromConnectData("dummy", rec.valueSchema(), rec.value());
	    

	    
  }
  
  @Test
  public void convertMapIntKeySchema() throws IOException {
	  SourceJSONPayloadConverter convertor = new SourceJSONPayloadConverter();
	    Map<String,String> configs = new HashMap<>();
	    configs.put("rest.source.properties", "Content-Type:application/json,Accept:application/json");
	    configs.put("rest.source.url", "https://blockchain.info/ticker");
	    configs.put("rest.source.destination.topics", "test");
	  
	   
	    configs.put("rest.source.payload.json.schema", getData("MapIntKeySchema.json")); 
	    		
	    convertor.start(new RestSourceConnectorConfig(configs));
	    
	    String json = getData("MapIntKey.json");
	    
	    List<SourceRecord> records = convertor.convert(json.getBytes());
	    assertNotNull(records);
	    assertEquals(1,records.size());
	    
	    SourceRecord rec = records.get(0);
	    assertNotNull(rec.valueSchema());
	    
	    JsonConverter jConv = new JsonConverter();
	    jConv.configure(new HashMap<>(),false);
	    jConv.fromConnectData("dummy", rec.valueSchema(), rec.value());
	    
	    //coverage bad bytes
	    try {
	    json="[ [ 123, {\"key5\":[\"34\"]}]]";
	    convertor.convert(json.getBytes());
	    }catch(DataException e) {};
	    
	  //coverage bad intKey
	    try {
	    json="{}";
	    convertor.convert(json.getBytes());
	    }catch(DataException e) {};
	    try {
		    json="[{}]";
		    convertor.convert(json.getBytes());
		}catch(DataException e) {};
	    try {
		    json="[[]]";
		    convertor.convert(json.getBytes());
		}catch(DataException e) {};
		
		
  }

  @Test
  public void badSchema() throws Exception {
	  Map<String,String> configs = new HashMap<>();
	    configs.put("rest.source.properties", "Content-Type:application/json,Accept:application/json");
	    configs.put("rest.source.url", "https://blockchain.info/ticker");
	    configs.put("rest.source.destination.topics", "test");
	    configs.put("rest.source.payload.json.schema", "{\"type\":\"struct\",\"name\":\"toto\",\"fields\":[{\"name\":\"key1\",\"type\":\"string\",\"optional\":true},{\"field\":\"key2\",\"type\":\"string\",\"optional\":false},{\"field\":\"key3\",\"type\":\"string\",\"optional\":true}]}" 
	    		);
	   try {
	    new RestSourceConnectorConfig(configs);
	    assertTrue(false);
	   }catch (ConfigException e) {
		   
	   }
  }

  // get schema in the ressource file
  private String getData(String fileName) throws IOException {
	  InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
	  return new String(IOUtils.toByteArray(is));
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
