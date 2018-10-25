package com.tm.kafka.connect.rest.converter.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.tm.kafka.connect.rest.RestSourceConnectorConfig;
import com.tm.kafka.connect.rest.converter.PayloadToSourceRecordConverter;
import com.tm.kafka.connect.rest.selector.TopicSelector;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;


import org.apache.kafka.connect.source.SourceRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.lang.System.currentTimeMillis;


import java.io.IOException;
import java.math.BigDecimal;

public class SourceJSONPayloadConverter implements PayloadToSourceRecordConverter {

  private Logger log = LoggerFactory.getLogger(SourceJSONPayloadConverter.class);

  private String url;
  private TopicSelector topicSelector;

  
private JsonConverter converterWithoutSchema;

private ObjectMapper om;
private Schema jsonSchema;

  @Override
  public List<SourceRecord> convert(byte[] bytes) {
    ArrayList<SourceRecord> records = new ArrayList<>();
    Map<String, String> sourcePartition = Collections.singletonMap("URL", url);
    Map<String, Long> sourceOffset = Collections.singletonMap("timestamp", currentTimeMillis());
    String topic = topicSelector.getTopic(bytes);
    
    
    SchemaAndValue sv ;
    try {
    	JsonNode data =  om.readTree(bytes);
    	//work also if jsonSchema is null
	    Object value = JsonSchemaConverter.convertToConnect(jsonSchema,data);
        
        sv = new SchemaAndValue(jsonSchema,value);
    	
	    
    } catch (IOException e) {
		//when requested data was not json
		throw new RuntimeException(e);
	}
    
    SourceRecord sourceRecord = new SourceRecord(sourcePartition, sourceOffset, topic, sv.schema(), sv.value());
    records.add(sourceRecord);
    return records;
  }

  @Override
  public void start(RestSourceConnectorConfig config) {
    url = config.getUrl();
    om = new ObjectMapper();
    
     jsonSchema = config.getPayloadJsonSchema();
    
    
    topicSelector = config.getTopicSelector();
    topicSelector.start(config);
    converterWithoutSchema = new JsonConverter();
    Map<String,Object> confs = new HashMap<>();
    confs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
    converterWithoutSchema.configure(confs, false);

     
  }
  
 
  

}
