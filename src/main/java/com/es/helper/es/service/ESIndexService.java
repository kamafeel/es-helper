package com.es.helper.es.service;


import com.es.helper.es.entity.MetaData;
import com.es.helper.es.utils.MetaTools;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author zhangqi
 * ES 索引服务类
 * ES 6.5.4-6.7.1 通用
 * @param <T>
 */
public interface ESIndexService<T> {
    /**
     * 创建索引
     * @param clazz
     * @throws Exception
     */
    default void createIndex(Class<T> clazz) throws Exception{
        MetaData md = MetaTools.getIndexMetaData(clazz);
        this.createIndex(md.getIndexMappingPath(), md.getIndexName());
    }
    /**
     * 删除索引
     * @param clazz
     * @throws Exception
     */
    default void dropIndex(Class<T> clazz) throws Exception{
        this.dropIndex(MetaTools.getIndexMetaData(clazz).getIndexName());
    }
    /**
     * 索引是否存在
     * @param clazz
     * @throws Exception
     */
    default boolean exists(Class<T> clazz) throws Exception{
        return this.exists(MetaTools.getIndexMetaData(clazz).getIndexName());
    }

    /**
     * 索引是否存在
     * @param indexName
     * @return
     * @throws Exception
     */
    boolean exists(String indexName) throws Exception;

    /**
     * 创建索引
     * @param indexNamePath
     * @param indexName
     * @throws Exception
     */
    void createIndex(String indexNamePath, String indexName) throws Exception;

    void dropIndex(String indexName) throws Exception;

    /**
     * 获取索引基本信息 自行处理GetIndexResponse
     * just like (LinkedHashMap) GetIndexResponse.getMappings().get(indexName).getSourceAsMap().get("properties")
     * @param clazz
     * @return
     * @throws Exception
     */
    default GetIndexResponse getIndex(Class clazz) throws Exception{
        return this.getIndex(MetaTools.getIndexMetaData(clazz).getIndexName());
    }

    /**
     * 获取索引基本信息 自行处理GetIndexResponse
     * just like (LinkedHashMap) GetIndexResponse.getMappings().get(indexName).getSourceAsMap().get("properties")
     * @param indexName
     * @return
     * @throws Exception
     */
    GetIndexResponse getIndex(String indexName) throws IOException;

    /**
     * 获取索引属性信息
     * @param indexName
     * @return
     * @throws IOException
     */
    LinkedHashMap getIndexProperties(String indexName) throws IOException;

    /**
     * 增加索引Mapping配置
     * @param indexName
     * @param mappingSource 自行组织json字符串
     * @throws IOException
     */
    void putMapping(String indexName, String mappingSource) throws IOException;

    /**
     * 增加索引Mapping配置
     * @param indexName
     * @param property
     *         Map<String,Object> map = Maps.newHashMap();
     *         map.put(KDomConst.ES_MAPPING_FIELD,"addfield");
     *         map.put(KDomConst.ES_MAPPING_TYPE,"keyword");
     * @throws IOException
     */
    void putMapping(String indexName, Map<String,Object>... property) throws IOException;

    /**
     * 索引迁移
     * @param destIndexName 目标索引预先需要自行建立
     * @param queryBuilder
     * @param script
     * @param sourceIndex
     * @return
     * @throws IOException
     */
    long reIndex(String destIndexName, QueryBuilder queryBuilder, Script script, String... sourceIndex) throws IOException;

    /**
     * 别名模式,迁移索引 原子操作
     * @param destIndexName
     * @param sourceIndexName
     * @return 索引节点确认返回,将返回true
     * @throws IOException
     */
    boolean reIndexByAliases(String destIndexName, String sourceIndexName) throws IOException;
}
