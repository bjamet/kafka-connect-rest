package com.tm.kafka.connect.rest.converter.source;

import com.tm.kafka.connect.rest.RestSourceConnectorConfig;
import com.tm.kafka.connect.rest.converter.PayloadToSourceRecordConverter;
import com.tm.kafka.connect.rest.selector.TopicSelector;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.System.currentTimeMillis;

public class SourceJSONPayloadConverter implements PayloadToSourceRecordConverter {

  private Logger log = LoggerFactory.getLogger(SourceJSONPayloadConverter.class);

  private String url;
  private TopicSelector topicSelector;

private JsonConverter converter;

  @Override
  public List<SourceRecord> convert(byte[] bytes) {
    ArrayList<SourceRecord> records = new ArrayList<>();
    Map<String, String> sourcePartition = Collections.singletonMap("URL", url);
    Map<String, Long> sourceOffset = Collections.singletonMap("timestamp", currentTimeMillis());
    String topic = topicSelector.getTopic(bytes);
    
    SchemaAndValue sv  = converter.toConnectData(topic,  bytes);
    
    SourceRecord sourceRecord = new SourceRecord(sourcePartition, sourceOffset, topic, sv.schema(), sv.value());
    records.add(sourceRecord);
    return records;
  }

  @Override
  public void start(RestSourceConnectorConfig config) {
    url = config.getUrl();
    topicSelector = config.getTopicSelector();
    topicSelector.start(config);
    converter = new JsonConverter();
    Map<String,Object> confs = new HashMap<>();
    confs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
    converter.configure(confs, false);
     
  }

}
