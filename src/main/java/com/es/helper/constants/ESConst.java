package com.es.helper.constants;

/**
 * @author zhangqi
 * 常量
 */
public final class ESConst {

  //ES SCROLL查询默认条数
  public static int ES_DEFAULT_SCROLL_SIZE = 100;

  //ES 搜索建议默认条数
  public static int ES_COMPLETION_SUGGESTION_SIZE = 10;
  //ES 复合聚合默认条数
  public static int ES_COMPOSITE_SIZE = 10000;

  //类型淡化处理
  public final static String ES_INDEX_TYPE = "_doc";

  public final static String ES_MAPPING_FIELD = "field";
  public final static String ES_MAPPING_TYPE = "type";
  public final static String ES_MAPPING_FORMAT = "format";
}
