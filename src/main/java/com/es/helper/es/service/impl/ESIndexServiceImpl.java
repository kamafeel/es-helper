package com.es.helper.es.service.impl;

import com.es.helper.constants.ESConst;
import com.es.helper.es.service.ESIndexService;
import com.google.common.collect.Maps;
import com.es.helper.enums.ErrorCodeEnum;
import com.es.helper.utils.AssertUtil;
import com.es.helper.utils.FileUtil;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.script.Script;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author zhangqi
 * ES 6.5.4-6.7.1 通用
 * 索引服务类
 * @param <T>
 */
@Service
@Slf4j
@ConditionalOnExpression("${es.enabled:true}")
public class ESIndexServiceImpl<T> implements ESIndexService<T> {

    @Resource
    private RestHighLevelClient client;

    @Override
    public boolean exists(String indexName) throws Exception {
        GetIndexRequest get = new GetIndexRequest();
        get.indices(indexName);
        return this.exists(get);
    }

    private boolean exists(GetIndexRequest getIndexRequest) throws IOException {
        AssertUtil.isNotNull(getIndexRequest, ErrorCodeEnum.ILLEGAL_PARAMETER);
        return client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
    }

    @Override
    public void createIndex(String indexNamePath, String indexName) throws Exception {
        GetIndexRequest getIndexRequest = new GetIndexRequest();
        getIndexRequest.indices(indexName);
        if (this.exists(getIndexRequest)) {
            log.info(indexName + "已存在!");
        } else {
            String conentstr = FileUtil.jsonFileRead(indexNamePath);
            CreateIndexRequest request = new CreateIndexRequest(indexName);
            request.source(conentstr, XContentType.JSON);
            this.createIndex(request);
            log.info(indexName + "生成完成");
        }
    }

    private void createIndex(CreateIndexRequest createIndexRequest) throws IOException {
        AssertUtil.isNotNull(createIndexRequest, ErrorCodeEnum.ILLEGAL_PARAMETER);
        String index = createIndexRequest.index();
        GetIndexRequest getIndexRequest = new GetIndexRequest();
        getIndexRequest.indices(index);
        if (!exists(getIndexRequest)) {
            client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        }
    }

    @Override
    public void dropIndex(String indexName) throws Exception {
        GetIndexRequest getIndexRequest = new GetIndexRequest();
        getIndexRequest.indices(indexName);
        if (this.exists(getIndexRequest)) {
            client.indices().delete(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT);
            log.info(indexName + "删除成功");
        }
    }

    @Override
    public GetIndexResponse getIndex(String indexName) throws IOException {
        GetIndexRequest get = new GetIndexRequest();
        get.indices(indexName);
        return client.indices().get(get, RequestOptions.DEFAULT);
    }

    @Override
    public LinkedHashMap getIndexProperties(String indexName) throws IOException {
        return (LinkedHashMap) this.getIndex(indexName).getMappings().get(indexName).get(ESConst.ES_INDEX_TYPE).getSourceAsMap().get("properties");
    }

    @Override
    public void putMapping(String indexName, String mappingSource) throws IOException {
        PutMappingRequest putMappingRequest = new PutMappingRequest(indexName).type(ESConst.ES_INDEX_TYPE).source(mappingSource,XContentType.JSON);
        client.indices().putMapping(putMappingRequest,RequestOptions.DEFAULT);
    }

    @Override
    public void putMapping(String indexName, Map<String,Object>... property) throws IOException {
        if(property.length==0){
            return;
        }
        Map<String,Object> properties = Maps.newLinkedHashMap();
        Map<String,Object> fields = Maps.newHashMap();
        for(int i=0; i<property.length;i++){
            Map<String,Object> propertyMap = Maps.newHashMap();
            fields.put(property[i].get(ESConst.ES_MAPPING_FIELD).toString(),propertyMap);
            propertyMap.put(ESConst.ES_MAPPING_TYPE,property[i].get(ESConst.ES_MAPPING_TYPE));
            if(property[i].containsKey(ESConst.ES_MAPPING_FORMAT)){
                propertyMap.put(ESConst.ES_MAPPING_FORMAT,property[i].get(ESConst.ES_MAPPING_FORMAT));
            }
        }
        properties.put("properties",fields);

        PutMappingRequest putMappingRequest = new PutMappingRequest(indexName).type(ESConst.ES_INDEX_TYPE).source(properties);
        client.indices().putMapping(putMappingRequest,RequestOptions.DEFAULT);
    }

    @Override
    public long reIndex(String destIndexName, QueryBuilder queryBuilder, Script script, String... sourceIndexName) throws IOException {
        ReindexRequest reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices(sourceIndexName);
        reindexRequest.setDestIndex(destIndexName);
        reindexRequest.setSourceDocTypes(ESConst.ES_INDEX_TYPE);
        reindexRequest.setDestDocType(ESConst.ES_INDEX_TYPE);
        reindexRequest.setConflicts("proceed");//冲突，采用计数，而不中断。
        if(queryBuilder!=null){
            reindexRequest.setSourceQuery(queryBuilder);
        }
        if(script!=null){
            reindexRequest.setScript(script);
        }
        return client.reindex(reindexRequest,RequestOptions.DEFAULT).getTotal();
    }

    @Override
    public boolean reIndexByAliases(String destIndexName, String sourceIndexName) throws IOException {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        IndicesAliasesRequest.AliasActions add = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                .index(destIndexName)
                .alias(sourceIndexName);
        IndicesAliasesRequest.AliasActions removeIndexAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE_INDEX)
                .index(sourceIndexName);

        request.addAliasAction(add);
        request.addAliasAction(removeIndexAction);
       return client.indices().updateAliases(request,RequestOptions.DEFAULT).isAcknowledged();
    }
}
