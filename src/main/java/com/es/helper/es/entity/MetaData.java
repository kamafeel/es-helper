package com.es.helper.es.entity;

import lombok.Data;

/**
 * @author zhangqi
 * 元数据载体类
 */
@Data
public class MetaData {
    private String indexName;
    private String indexMappingPath;
    private boolean printLog = false;
    private int number_of_shards;
    private int number_of_replicas;
}