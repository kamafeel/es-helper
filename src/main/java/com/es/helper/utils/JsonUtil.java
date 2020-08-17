package com.es.helper.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.lang.reflect.Type;

/**
 * jackson json的工具
 * @author zhangqi73
 */
public final class JsonUtil {

  private static ObjectMapper mapper = new ObjectMapper();
  static{
    //ObjectMapper忽略多余字段
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    //NULL不参与序列化
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }
  /**
   * Serialize any Java value as a String.
   */
  public static <T> String toStr(T obj) throws JsonProcessingException {
    return obj instanceof String ? (String) obj : mapper.writeValueAsString(obj);
  }

  public static <T> String toPrettyStr(T obj) throws JsonProcessingException {
    return obj instanceof String ? (String) obj : mapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
  }

  /**
   * Deserialize JSON content from given JSON content String.
   */
  public static <T> T toT(String content, Class<T> valueType) throws IOException {
    return mapper.readValue(content, valueType);
  }

  /**
   * Deserialize JSON content from given JSON content String.
   */
  public static JsonNode toJsonNode(String content) throws IOException {
    return mapper.readTree(content);
  }


  public static ObjectNode createObjectNode() {
    return mapper.createObjectNode();
  }

  public static ArrayNode createArrayNode() {
    return mapper.createArrayNode();
  }

  /**
   * Deserialize JSON content from given JSON content String.
   */
  public static <T> T toT(String content, Type type) throws IOException {
    return mapper.readValue(content, mapper.getTypeFactory().constructType(type));
  }
}