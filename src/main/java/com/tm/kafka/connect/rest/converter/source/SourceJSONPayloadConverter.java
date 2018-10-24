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
    
    if (jsonSchema!=null) {
    	
    	try {
	    	JsonNode data =  om.readTree(bytes);
	    	
	    	
	        Object value = convertToConnect(jsonSchema,data);
	        
	        sv = new SchemaAndValue(jsonSchema,value);
    	} catch (IOException e) {
    		//when requested data was not json
    		throw new RuntimeException(e);
    	}
    }else {
    	sv = converterWithoutSchema.toConnectData(topic,  bytes);
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
  
  //copy and adapted from JsonConverter
  private static Object convertToConnect(Schema schema, JsonNode jsonValue) {
      final Schema.Type schemaType;
      if (schema != null) {
          schemaType = schema.type();
          if (jsonValue == null || jsonValue.isNull()) {
              if (schema.defaultValue() != null)
                  return schema.defaultValue(); // any logical type conversions should already have been applied
              if (schema.isOptional())
                  return null;
              throw new DataException("Invalid null value for required " + schemaType +  " field");
          }
      } else {
          switch (jsonValue.getNodeType()) {
              case NULL:
                  // Special case. With no schema
                  return null;
              case BOOLEAN:
                  schemaType = Schema.Type.BOOLEAN;
                  break;
              case NUMBER:
                  if (jsonValue.isIntegralNumber())
                      schemaType = Schema.Type.INT64;
                  else
                      schemaType = Schema.Type.FLOAT64;
                  break;
              case ARRAY:
                  schemaType = Schema.Type.ARRAY;
                  break;
              case OBJECT:
                  schemaType = Schema.Type.MAP;
                  break;
              case STRING:
                  schemaType = Schema.Type.STRING;
                  break;

              case BINARY:
              case MISSING:
              case POJO:
              default:
                  schemaType = null;
                  break;
          }
      }

      final JsonToConnectTypeConverter typeConverter = TO_CONNECT_CONVERTERS.get(schemaType);
      if (typeConverter == null)
          throw new DataException("Unknown schema type: " + String.valueOf(schemaType));

      Object converted = typeConverter.convert(schema, jsonValue);
      if (schema != null && schema.name() != null) {
          LogicalTypeConverter logicalConverter = TO_CONNECT_LOGICAL_CONVERTERS.get(schema.name());
          if (logicalConverter != null)
              converted = logicalConverter.convert(schema, converted);
      }
      return converted;
  }
  
  private static final Map<Schema.Type, JsonToConnectTypeConverter> TO_CONNECT_CONVERTERS = new EnumMap<>(Schema.Type.class);

  static {
      TO_CONNECT_CONVERTERS.put(Schema.Type.BOOLEAN, new JsonToConnectTypeConverter() {
          @Override
          public Object convert(Schema schema, JsonNode value) {
              return value.booleanValue();
          }
      });
      TO_CONNECT_CONVERTERS.put(Schema.Type.INT8, new JsonToConnectTypeConverter() {
          @Override
          public Object convert(Schema schema, JsonNode value) {
              return (byte) value.intValue();
          }
      });
      TO_CONNECT_CONVERTERS.put(Schema.Type.INT16, new JsonToConnectTypeConverter() {
          @Override
          public Object convert(Schema schema, JsonNode value) {
              return (short) value.intValue();
          }
      });
      TO_CONNECT_CONVERTERS.put(Schema.Type.INT32, new JsonToConnectTypeConverter() {
          @Override
          public Object convert(Schema schema, JsonNode value) {
              return value.intValue();
          }
      });
      TO_CONNECT_CONVERTERS.put(Schema.Type.INT64, new JsonToConnectTypeConverter() {
          @Override
          public Object convert(Schema schema, JsonNode value) {
              return value.longValue();
          }
      });
      TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT32, new JsonToConnectTypeConverter() {
          @Override
          public Object convert(Schema schema, JsonNode value) {
              return value.floatValue();
          }
      });
      TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT64, new JsonToConnectTypeConverter() {
          @Override
          public Object convert(Schema schema, JsonNode value) {
              return value.doubleValue();
          }
      });
      TO_CONNECT_CONVERTERS.put(Schema.Type.BYTES, new JsonToConnectTypeConverter() {
          @Override
          public Object convert(Schema schema, JsonNode value) {
              try {
                  return value.binaryValue();
              } catch (IOException e) {
                  throw new DataException("Invalid bytes field", e);
              }
          }
      });
      TO_CONNECT_CONVERTERS.put(Schema.Type.STRING, new JsonToConnectTypeConverter() {
          @Override
          public Object convert(Schema schema, JsonNode value) {
              return value.textValue();
          }
      });
      TO_CONNECT_CONVERTERS.put(Schema.Type.ARRAY, new JsonToConnectTypeConverter() {
          @Override
          public Object convert(Schema schema, JsonNode value) {
              Schema elemSchema = schema == null ? null : schema.valueSchema();
              ArrayList<Object> result = new ArrayList<>();
              for (JsonNode elem : value) {
                  result.add(convertToConnect(elemSchema, elem));
              }
              return result;
          }
      });
      TO_CONNECT_CONVERTERS.put(Schema.Type.MAP, new JsonToConnectTypeConverter() {
          @Override
          public Object convert(Schema schema, JsonNode value) {
              Schema keySchema = schema == null ? null : schema.keySchema();
              Schema valueSchema = schema == null ? null : schema.valueSchema();

              // If the map uses strings for keys, it should be encoded in the natural JSON format. If it uses other
              // primitive types or a complex type as a key, it will be encoded as a list of pairs. If we don't have a
              // schema, we default to encoding in a Map.
              Map<Object, Object> result = new HashMap<>();
              if (schema == null || keySchema.type() == Schema.Type.STRING) {
                  if (!value.isObject())
                      throw new DataException("Maps with string fields should be encoded as JSON objects, but found " + value.getNodeType());
                  Iterator<Map.Entry<String, JsonNode>> fieldIt = value.fields();
                  while (fieldIt.hasNext()) {
                      Map.Entry<String, JsonNode> entry = fieldIt.next();
                      result.put(entry.getKey(), convertToConnect(valueSchema, entry.getValue()));
                  }
              } else {
                  if (!value.isArray())
                      throw new DataException("Maps with non-string fields should be encoded as JSON array of tuples, but found " + value.getNodeType());
                  for (JsonNode entry : value) {
                      if (!entry.isArray())
                          throw new DataException("Found invalid map entry instead of array tuple: " + entry.getNodeType());
                      if (entry.size() != 2)
                          throw new DataException("Found invalid map entry, expected length 2 but found :" + entry.size());
                      result.put(convertToConnect(keySchema, entry.get(0)),
                              convertToConnect(valueSchema, entry.get(1)));
                  }
              }
              return result;
          }
      });
      TO_CONNECT_CONVERTERS.put(Schema.Type.STRUCT, new JsonToConnectTypeConverter() {
          @Override
          public Object convert(Schema schema, JsonNode value) {
              if (!value.isObject())
                  throw new DataException("Structs should be encoded as JSON objects, but found " + value.getNodeType());

              // We only have ISchema here but need Schema, so we need to materialize the actual schema. Using ISchema
              // avoids having to materialize the schema for non-Struct types but it cannot be avoided for Structs since
              // they require a schema to be provided at construction. However, the schema is only a SchemaBuilder during
              // translation of schemas to JSON; during the more common translation of data to JSON, the call to schema.schema()
              // just returns the schema Object and has no overhead.
              Struct result = new Struct(schema.schema());
              for (Field field : schema.fields())
                  result.put(field, convertToConnect(field.schema(), value.get(field.name())));

              return result;
          }
      });
  }

  // Convert values in Kafka Connect form into their logical types. These logical converters are discovered by logical type
  // names specified in the field
  private static final HashMap<String, LogicalTypeConverter> TO_CONNECT_LOGICAL_CONVERTERS = new HashMap<>();
  static {
      TO_CONNECT_LOGICAL_CONVERTERS.put(Decimal.LOGICAL_NAME, new LogicalTypeConverter() {
          @Override
          public Object convert(Schema schema, Object value) {
              if (!(value instanceof byte[]))
                  throw new DataException("Invalid type for Decimal, underlying representation should be bytes but was " + value.getClass());
              return Decimal.toLogical(schema, (byte[]) value);
          }
      });

      TO_CONNECT_LOGICAL_CONVERTERS.put(Date.LOGICAL_NAME, new LogicalTypeConverter() {
          @Override
          public Object convert(Schema schema, Object value) {
              if (!(value instanceof Integer))
                  throw new DataException("Invalid type for Date, underlying representation should be int32 but was " + value.getClass());
              return Date.toLogical(schema, (int) value);
          }
      });

      TO_CONNECT_LOGICAL_CONVERTERS.put(Time.LOGICAL_NAME, new LogicalTypeConverter() {
          @Override
          public Object convert(Schema schema, Object value) {
              if (!(value instanceof Integer))
                  throw new DataException("Invalid type for Time, underlying representation should be int32 but was " + value.getClass());
              return Time.toLogical(schema, (int) value);
          }
      });

      TO_CONNECT_LOGICAL_CONVERTERS.put(Timestamp.LOGICAL_NAME, new LogicalTypeConverter() {
          @Override
          public Object convert(Schema schema, Object value) {
              if (!(value instanceof Long))
                  throw new DataException("Invalid type for Timestamp, underlying representation should be int64 but was " + value.getClass());
              return Timestamp.toLogical(schema, (long) value);
          }
      });
  }

  
  
  private interface JsonToConnectTypeConverter {
      Object convert(Schema schema, JsonNode value);
  }

  private interface LogicalTypeConverter {
      Object convert(Schema schema, Object value);
  }
  

}
