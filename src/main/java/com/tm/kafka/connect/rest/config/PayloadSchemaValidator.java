package com.tm.kafka.connect.rest.config;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;

public class PayloadSchemaValidator implements ConfigDef.Validator {
	
	private Schema schema = null;
	private JsonConverter converter;
	private ObjectMapper om;
  
  
  public PayloadSchemaValidator() {
	  converter = new JsonConverter();
	  converter.configure(new HashMap<>(), false);
	  om = new ObjectMapper();
  }
  
  public Schema getSchema(String schema) {
	  if (schema == null)
		  return null;
	  if (schema.trim().length()==0)
		  return null;
	  
	  return _testSchema(schema,null);
 }
  
  private Schema _testSchema(String schema,String name) {
	  try {
		   //provider should be string
			JsonNode schemaJson = om.readTree(schema);
			return converter.asConnectSchema(schemaJson);
		} catch (IOException e) {
			throw new ConfigException("Property " + name + " is not valid json",e);
		}catch (DataException e) {
			throw new ConfigException("Property " + name + " is not a valid schema",e);
		}
	  
  }
  @Override
  public void ensureValid(String name, Object provider) {
	  if (provider==null)
		  return;
	  
	  String object = provider.toString();
	  if (object.trim().length()==0)
		  return;
	 
	   _testSchema(provider.toString(),name);
	   
  }

  @Override
  public String toString() {
    return  "Ensure the schema is valid Json";
  }
}
