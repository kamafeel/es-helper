package com.es.helper.es.service.impl;

import com.es.helper.annotation.Fn;
import com.es.helper.annotation.reflection.Reflections;
import com.es.helper.constants.ESConst;
import com.es.helper.enums.ErrorCodeEnum;
import com.es.helper.es.annotation.ESID;
import com.es.helper.es.entity.CompositeAggResponse;
import com.es.helper.es.entity.MetaData;
import com.es.helper.es.entity.PageList;
import com.es.helper.es.entity.ScrollResponse;
import com.es.helper.es.service.ESTemplateService;
import com.es.helper.es.utils.MetaTools;
import com.es.helper.help.ESCommonException;
import com.es.helper.utils.JsonUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.script.mustache.SearchTemplateResponse;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.percentiles.*;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @param <T>
 * @param <M>
 * @author zhangqi
 * ES 6.5.4
 * ES 文档操作服务
 */
@Service
@Slf4j
@ConditionalOnExpression("${es.enabled:true}")
public class ESTemplateServiceImpl<T, M> implements ESTemplateService<T, M> {
    //ID
    private final static String ID = "_id";

    @Resource
    private RestHighLevelClient client;


    @Override
    public Response request(Request request) throws Exception {
        Response response = client.getLowLevelClient().performRequest(request);
        return response;
    }

    @Override
    public SearchResponse request(SearchRequest request) throws IOException {
        return client.search(request, RequestOptions.DEFAULT);
    }

    @Override
    public boolean save(T t) throws Exception {
        return save(t, null);
    }

    @Override
    public boolean save(T t, String routing) throws Exception {
        MetaData metaData = MetaTools.getIndexMetaData(t.getClass());
        String id = MetaTools.getESId(t);
        IndexRequest indexRequest = null;
        if (StringUtils.isEmpty(id)) {
            indexRequest = new IndexRequest(metaData.getIndexName());
        } else {
            //淡化type概念，后续版本将取消
            indexRequest = new IndexRequest(metaData.getIndexName(), ESConst.ES_INDEX_TYPE, id);
            MetaTools.ignoreEsId(t);
        }
        String source = JsonUtil.toStr(t);
        indexRequest.source(source, XContentType.JSON);
        if (!StringUtils.isEmpty(routing)) {
            indexRequest.routing(routing);
        }
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
            log.info("INDEX DOC CREATE SUCCESS");
        } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            log.info("INDEX DOC UPDATE SUCCESS");
        } else {
            return false;
        }
        return true;
    }

    @Override
    public boolean save(Map m, String indexName) throws Exception {
        IndexRequest indexRequest = null;
        if (m.containsKey(ID)) {
            indexRequest = new IndexRequest(indexName, ESConst.ES_INDEX_TYPE, m.get(ID).toString());
            m.remove(ID);//不再保存ID
        } else {
            indexRequest = new IndexRequest(indexName, ESConst.ES_INDEX_TYPE);
        }
        indexRequest.source(m, XContentType.JSON);
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
            log.info("INDEX DOC CREATE SUCCESS");
        } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            log.info("INDEX DOC UPDATE SUCCESS");
        } else {
            return false;
        }
        return true;
    }

    @Override
    public List<BulkResponse> saveBatch(List<Map<String,Object>> list, String indexName) throws Exception {
        if (list == null || list.isEmpty()) {
            return null;
        }
        List<BulkResponse> brs = Lists.newArrayList();
        List<List<Map<String,Object>>> partition = Lists.partition(list, 3000);
        for (int i = 0; i < partition.size(); i++) {
            brs.add(saveBulk2Map(partition.get(i), indexName));
        }
        return brs;
    }

    @Override
    public List<BulkResponse> saveBatch(List<T> list) throws Exception {
        if (list == null || list.isEmpty()) {
            return null;
        }
        MetaData metaData = MetaTools.getIndexMetaData(list.get(0).getClass());
        List<BulkResponse> brs = Lists.newArrayList();
        List<List<T>> lists = Lists.partition(list, 3000);
        for (int i = 0; i < lists.size(); i++) {
            brs.add(saveBulk(lists.get(i), metaData.getIndexName()));
        }
        return brs;
    }

    @Override
    public List<BulkResponse> updateBatch(List<T> list) throws Exception {
        if (list == null || list.isEmpty()) {
            return null;
        }
        MetaData metaData = MetaTools.getIndexMetaData(list.get(0).getClass());
        List<BulkResponse> brs = Lists.newArrayList();
        List<List<T>> lists = Lists.partition(list, 3000);
        for (int i = 0; i < lists.size(); i++) {
            brs.add(updateBulk(lists.get(i), metaData.getIndexName(), null));
        }
        return brs;
    }

    @Override
    public List<BulkResponse> updateBatch(List<Map<String,Object>> list, String indexName) throws Exception {
        if (list == null || list.isEmpty()) {
            return null;
        }
        List<BulkResponse> brs = Lists.newArrayList();
        List<List<Map<String,Object>>> partition = Lists.partition(list, 3000);
        for (int i = 0; i < partition.size(); i++) {
            brs.add(updateBulk2Map(partition.get(i), indexName,null));
        }
        return brs;
    }

    @Override
    public List<BulkResponse> updateBatchByScript(List<T> list, String script, Fn<T, ?>... fn) throws Exception {
        if (list == null || list.isEmpty()) {
            return null;
        }
        MetaData metaData = MetaTools.getIndexMetaData(list.get(0).getClass());
        List<BulkResponse> brs = Lists.newArrayList();
        List<List<T>> lists = Lists.partition(list, 3000);
        for (int i = 0; i < lists.size(); i++) {
            brs.add(updateBulk(lists.get(i), metaData.getIndexName(), script, fn));
        }
        return brs;
    }


    @Override
    public List<BulkResponse> updateBatchByScript(List<Map<String,Object>> list, String script, String indexName) throws Exception {
        if (list == null || list.isEmpty()) {
            return null;
        }
        List<BulkResponse> brs = Lists.newArrayList();
        List<List<Map<String,Object>>> partition = Lists.partition(list, 3000);
        for (int i = 0; i < partition.size(); i++) {
            brs.add(updateBulk2Map(partition.get(i), indexName,script));
        }
        return brs;
    }

    private BulkResponse saveBulk(List<T> list, String indexName) throws Exception {
        BulkRequest br = new BulkRequest();
        for (int i = 0; i < list.size(); i++) {
            T t = list.get(i);
            String id = MetaTools.getESId(t);
            if(StringUtils.isEmpty(id)){
                br.add(new IndexRequest(indexName, ESConst.ES_INDEX_TYPE, id)
                        .opType(DocWriteRequest.OpType.INDEX) //自动生成ID
                        .source(JsonUtil.toStr(t), XContentType.JSON));
            }else{
                MetaTools.ignoreEsId(t);
                br.add(new IndexRequest(indexName, ESConst.ES_INDEX_TYPE, id)
                        .opType(DocWriteRequest.OpType.CREATE) //只创建
                        .source(JsonUtil.toStr(t), XContentType.JSON));
            }


        }
        BulkResponse bulkResponse = client.bulk(br, RequestOptions.DEFAULT);
        return bulkResponse;
    }

    private BulkResponse saveBulk2Map(List<Map<String,Object>> list, String indexName) throws Exception {
        BulkRequest br = new BulkRequest();
        for (int i = 0; i < list.size(); i++) {
            Map<String,Object> m = list.get(i);
            if (m.containsKey(ID)) {
                String id = m.get(ID).toString();
                m.remove(ID);//不再保存ID
                br.add(new IndexRequest(indexName, ESConst.ES_INDEX_TYPE, m.get(ID).toString())
                        .opType(DocWriteRequest.OpType.CREATE) //只创建
                        .source(m, XContentType.JSON));
            }else{
                br.add(new IndexRequest(indexName, ESConst.ES_INDEX_TYPE)
                        .opType(DocWriteRequest.OpType.INDEX) //自动生成ID
                        .source(m, XContentType.JSON));
            }
        }
        BulkResponse bulkResponse = client.bulk(br, RequestOptions.DEFAULT);
        return bulkResponse;
    }

    private BulkResponse updateBulk(List<T> list, String indexName, String script, Fn<T, ?>... fn) throws Exception {
        BulkRequest br = new BulkRequest();
        for (int i = 0; i < list.size(); i++) {
            T t = list.get(i);
            UpdateRequest ur = new UpdateRequest(indexName, ESConst.ES_INDEX_TYPE, MetaTools.getESId(t));
            if (!StringUtils.isEmpty(script)) {
                ur.scriptedUpsert(true);//脚本必须执行，如果文档不存在,创建文档
                ur.script(new Script(ScriptType.INLINE, "painless", script, this.getScriptMap(t, fn)));
            } else {
                ur.doc(MetaTools.getFieldValue(t));//忽略ID和空值信息
                ur.docAsUpsert(true); //如果文档不存在,使用upsert模式创建文档
            }
            br.add(ur);
        }
        BulkResponse bulkResponse = client.bulk(br, RequestOptions.DEFAULT);
        return bulkResponse;
    }

    private BulkResponse updateBulk2Map(List<Map<String,Object>> list, String indexName, String script) throws Exception {
        BulkRequest br = new BulkRequest();
        for (int i = 0; i < list.size(); i++) {
            Map<String,Object> m = list.get(i);
            UpdateRequest ur = new UpdateRequest(indexName, ESConst.ES_INDEX_TYPE, m.get(ID).toString());
            if (!StringUtils.isEmpty(script)) {
                ur.scriptedUpsert(true);//脚本必须执行，如果文档不存在,创建文档
                ur.script(new Script(ScriptType.INLINE, "painless", script, m));
            } else {
                m.remove(ID);//不再保存ID
                ur.doc(m);
                ur.docAsUpsert(true); //如果文档不存在,使用upsert模式创建文档
            }
            br.add(ur);
        }
        BulkResponse bulkResponse = client.bulk(br, RequestOptions.DEFAULT);
        return bulkResponse;
    }

    private Map<String, Object> getScriptMap(T t, Fn<T, ?>... fn) throws NoSuchFieldException, IllegalAccessException {
        Map<String, Object> map = Maps.newHashMap();
        for (Fn<T, ?> f : fn) {
            String fieldName = Reflections.fnToFieldName(f);
            map.put(fieldName, MetaTools.getFieldValue(t, fieldName));
        }
        return map;
    }

    @Override
    public boolean update(T t) throws Exception {
        MetaData metaData = MetaTools.getIndexMetaData(t.getClass());
        String id = MetaTools.getESId(t);
        if (StringUtils.isEmpty(id)) {
            throw new Exception("ID cannot be empty");
        }
        UpdateRequest updateRequest = new UpdateRequest(metaData.getIndexName(), ESConst.ES_INDEX_TYPE, id);
        updateRequest.doc(MetaTools.getFieldValue(t)); //MAP模式 忽略ID和空值信息
        //updateRequest.doc(JsonUtil.toStr(t), XContentType.JSON); //JSON模式
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
            log.info("INDEX DOC CREATE SUCCESS");
        } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            log.info("INDEX DOC UPDATE SUCCESS");
        } else {
            return false;
        }
        return true;
    }

    @Override
    public boolean update(Map m, String indexName) throws Exception {
        if (!m.containsKey(ID)) {
            throw new Exception("map key _id cannot be empty");
        }
        UpdateRequest updateRequest = new UpdateRequest(indexName, ESConst.ES_INDEX_TYPE, m.get(ID).toString());
        m.remove(ID);//不再保存ID
        updateRequest.doc(m); //使用MAP模式
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
            log.info("INDEX DOC CREATE SUCCESS");
        } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            log.info("INDEX DOC UPDATE SUCCESS");
        } else {
            return false;
        }
        return true;
    }

    @Override
    public boolean delete(T t) throws Exception {
        return delete(t, null);
    }

    @Override
    public boolean delete(T t, String routing) throws Exception {
        MetaData metaData = MetaTools.getIndexMetaData(t.getClass());
        String id = MetaTools.getESId(t);
        if (StringUtils.isEmpty(id)) {
            throw new Exception("ID cannot be empty");
        }
        DeleteRequest deleteRequest = new DeleteRequest(metaData.getIndexName(), ESConst.ES_INDEX_TYPE, id);
        if (!StringUtils.isEmpty(routing)) {
            deleteRequest.routing(routing);
        }
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        if (deleteResponse.getResult() == DocWriteResponse.Result.DELETED) {
            log.info("INDEX DOC DELETE SUCCESS");
        } else {
            return false;
        }
        return true;
    }

    @Override
    public BulkByScrollResponse deleteByCondition(QueryBuilder queryBuilder, String indexName) throws Exception {
        if (queryBuilder == null) {
            queryBuilder = new MatchAllQueryBuilder();
        }
        DeleteByQueryRequest request = new DeleteByQueryRequest(indexName);
        request.setQuery(queryBuilder);
        BulkByScrollResponse bulkResponse = client.deleteByQuery(request, RequestOptions.DEFAULT);
        return bulkResponse;
    }

    @Override
    public boolean deleteById(M id, Class<T> clazz) throws Exception {
        MetaData metaData = MetaTools.getIndexMetaData(clazz);
        if (org.springframework.util.StringUtils.isEmpty(id)) {
            throw new Exception("ID cannot be empty");
        }
        DeleteRequest deleteRequest = new DeleteRequest(metaData.getIndexName(), ESConst.ES_INDEX_TYPE, id.toString());
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        if (deleteResponse.getResult() == DocWriteResponse.Result.DELETED) {
            log.info("INDEX DOC DELETE SUCCESS");
        } else {
            return false;
        }
        return true;
    }

    @Override
    public SearchResponse search(SearchRequest searchRequest) throws IOException {
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return searchResponse;
    }

    @Override
    public PageList<T> search(QueryBuilder queryBuilder, SortBuilder<?> sort, Class<T> clazz, int pageSize, int pageNum, String[] includes, String[] excludes) throws Exception {
        MetaData metaData = MetaTools.getIndexMetaData(clazz);
        PageList<T> pl = new PageList<T>();
        List<T> list = Lists.newArrayList();
        SearchHits searchHits = this.search2SearchHits(queryBuilder, sort, metaData.getIndexName(), metaData.isPrintLog(), pageSize, pageNum,includes,excludes);
        pl.setTotal(searchHits.getTotalHits());//ES版本6
        //pl.setTotal(searchHits.getTotalHits().value);//ES版本7
        for (SearchHit hit : searchHits) {
            T t = JsonUtil.toT(hit.getSourceAsString(), clazz);
            correctID(clazz, t, (M) hit.getId());
            list.add(t);
        }
        pl.setRecords(list);
        return pl;
    }

    @Override
    public PageList<Map<String, Object>> searchToMap(QueryBuilder queryBuilder, SortBuilder<?> sort, String indexName,boolean isPrintLog, int pageSize, int pageNum,String[] includes, String[] excludes) throws Exception {
        PageList<Map<String, Object>> pl = new PageList<Map<String, Object>>();
        List<Map<String, Object>> list = Lists.newArrayList();
        SearchHits searchHits = this.search2SearchHits(queryBuilder, sort, indexName,isPrintLog, pageSize, pageNum,includes,excludes);
        pl.setTotal(searchHits.getTotalHits());//ES版本6
        //pl.setTotal(searchHits.getTotalHits().value);//ES版本7
        for (SearchHit hit : searchHits) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            sourceAsMap.put(ID, hit.getId());
            list.add(sourceAsMap);
        }
        pl.setRecords(list);
        return pl;
    }

    private SearchHits search2SearchHits(QueryBuilder queryBuilder, SortBuilder<?> sort, String indexName,boolean isPrintLog, int pageSize, int pageNum,String[] includes, String[] excludes) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder);
        if (sort != null) {
            searchSourceBuilder.sort(sort);
        }
        if(includes!=null && excludes!=null){
            searchSourceBuilder.fetchSource(includes,excludes);
        }
        searchSourceBuilder.size(pageSize);
        searchSourceBuilder.from((pageNum - 1) * pageSize);
        searchRequest.source(searchSourceBuilder);
        if (isPrintLog) {
            log.info(searchSourceBuilder.toString());
        }
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return searchResponse.getHits();
    }

    @Override
    public long count(QueryBuilder queryBuilder, Class<T> clazz) throws Exception {
        throw new ESCommonException(ErrorCodeEnum.UNEXCEPTED, "this es version is not support");
    }

    @Override
    public Collection<String> collapse(QueryBuilder queryBuilder, Class<T> clazz, Fn<T, ?> fn) throws IOException {
        MetaData metaData = MetaTools.getIndexMetaData(clazz);
        return this.collapse(queryBuilder, metaData.getIndexName(), Reflections.fnToFieldName(fn));
    }

    @Override
    public Collection<String> collapse(QueryBuilder queryBuilder, String indexName, String fieldName) throws IOException {
        Set<String> set = Sets.newHashSet();
        this.collapse(queryBuilder, indexName, fieldName, null).getHits().forEach(s -> {
            s.getFields().get(fieldName).getValues().forEach(o -> {
                set.add(o.toString());
            });
        });
        return set;
    }

    @Override
    public SearchResponse collapse(QueryBuilder queryBuilder, Class<T> clazz, Fn<T, ?> fn, List<InnerHitBuilder> innerHits) throws IOException {
        MetaData metaData = MetaTools.getIndexMetaData(clazz);
        return this.collapse(queryBuilder, metaData.getIndexName(), Reflections.fnToFieldName(fn), innerHits);
    }

    @Override
    public SearchResponse collapse(QueryBuilder queryBuilder, String indexName, String fieldName, List<InnerHitBuilder> innerHits) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder);
        CollapseBuilder cb = new CollapseBuilder(fieldName);
        if (innerHits != null && !innerHits.isEmpty()){
            cb.setInnerHits(innerHits);
            cb.setMaxConcurrentGroupRequests(3);//每组检索 inner_hits 并发请求数量
        }

        searchSourceBuilder.collapse(cb);
        log.info(searchSourceBuilder.toString());
        searchRequest.source(searchSourceBuilder);
        return client.search(searchRequest, RequestOptions.DEFAULT);
    }

    @Override
    public ScrollResponse<T> createScroll(QueryBuilder queryBuilder, Class<T> clazz, TimeValue tv, Integer size) throws Exception {
        MetaData metaData = MetaTools.getIndexMetaData(clazz);
        if (queryBuilder == null) {
            queryBuilder = new MatchAllQueryBuilder();
        }
        List<T> list = new ArrayList<>();
        Scroll scroll = new Scroll(tv);
        SearchRequest searchRequest = new SearchRequest(metaData.getIndexName());
        searchRequest.scroll(scroll);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(size == null || size == 0 ? ESConst.ES_DEFAULT_SCROLL_SIZE : 0);
        searchSourceBuilder.query(queryBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        //第一把查询出的结果
        for (SearchHit hit : searchHits) {
            T t = JsonUtil.toT(hit.getSourceAsString(), clazz);
            //将_id字段重新赋值给@ESID注解的字段
            //correctID(clazz, t, (M)hit.getId());
            list.add(t);
        }
        return new ScrollResponse(list, scrollId);
    }

    @Override
    public ScrollResponse<T> queryScroll(Class<T> clazz, TimeValue tv, String scrollId) throws Exception {
        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
        Scroll scroll = new Scroll(tv);
        scrollRequest.scroll(scroll);
        SearchResponse searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
        scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        List<T> list = new ArrayList<>();
        for (SearchHit hit : searchHits) {
            T t = JsonUtil.toT(hit.getSourceAsString(), clazz);
            //将_id字段重新赋值给@ESID注解的字段
            //correctID(clazz, t, (M)hit.getId());
            list.add(t);
        }
        return new ScrollResponse(list, scrollId);
    }

    @Override
    public List<String> completionSuggest(String fieldName, String fieldValue, Class<T> clazz) throws Exception {
        MetaData metaData = MetaTools.getIndexMetaData(clazz);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        CompletionSuggestionBuilder completionSuggestionBuilder = new
                CompletionSuggestionBuilder(fieldName + ".suggest");
        completionSuggestionBuilder.text(fieldValue);
        completionSuggestionBuilder.skipDuplicates(true);
        completionSuggestionBuilder.size(ESConst.ES_COMPLETION_SUGGESTION_SIZE);
        suggestBuilder.addSuggestion("suggest_" + fieldName, completionSuggestionBuilder);
        searchSourceBuilder.suggest(suggestBuilder);
        SearchRequest searchRequest = new SearchRequest(metaData.getIndexName());
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        Suggest suggest = searchResponse.getSuggest();
        if (suggest == null) {
            return null;
        }
        CompletionSuggestion completionSuggestion = suggest.getSuggestion("suggest_" + fieldName);
        List<String> list = new ArrayList<>();
        for (CompletionSuggestion.Entry entry : completionSuggestion.getEntries()) {
            for (CompletionSuggestion.Entry.Option option : entry) {
                String suggestText = option.getText().string();
                list.add(suggestText);
            }
        }
        return list;
    }

    @Override
    public long cardinalityAgg(QueryBuilder queryBuilder, Class<T> clazz, Fn<T, ?> fn, Script script, Long precisionThreshold) throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        CardinalityAggregationBuilder aggregation = AggregationBuilders
                .cardinality("cardinality_" + Reflections.fnToFieldName(fn));
        if (script != null) {
            aggregation.script(script);
        } else {
            aggregation.field(Reflections.fnToFieldName(fn));
        }
        if (precisionThreshold != null) {
            aggregation.precisionThreshold(precisionThreshold);
        }
        if (queryBuilder != null) {
            searchSourceBuilder.query(queryBuilder);
        }
        searchSourceBuilder.size(0);
        searchSourceBuilder.aggregation(aggregation);
        SearchRequest searchRequest = new SearchRequest(MetaTools.getIndexMetaData(clazz).getIndexName());
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        Cardinality agg = searchResponse.getAggregations().get("cardinality_" + Reflections.fnToFieldName(fn));
        return agg.getValue();
    }

    @Override
    public Map<Double, Double> percentileAgg(QueryBuilder queryBuilder, Class<T> clazz, Fn<T, ?> fn, double... customSegment) throws Exception {
        MetaData md = MetaTools.getIndexMetaData(clazz);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        PercentilesAggregationBuilder aggregation = AggregationBuilders
                .percentiles("percentiles_" + Reflections.fnToFieldName(fn))
                .field(Reflections.fnToFieldName(fn)).percentiles(customSegment);
        if (queryBuilder != null) {
            searchSourceBuilder.query(queryBuilder);
        }
        searchSourceBuilder.size(0);
        searchSourceBuilder.aggregation(aggregation);
        SearchRequest searchRequest = new SearchRequest(md.getIndexName());
        searchRequest.source(searchSourceBuilder);
        if (md.isPrintLog()) {
            log.info(searchSourceBuilder.toString());
        }
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        Map<Double, Double> map = new LinkedHashMap<>();
        Percentiles agg = searchResponse.getAggregations().get("percentiles_" + Reflections.fnToFieldName(fn));
        for (Percentile entry : agg) {
            double percent = entry.getPercent();
            double value = entry.getValue();
            map.put(percent, value);
        }
        return map;
    }

    @Override
    public Map<Double, Double> percentileAgg(QueryBuilder queryBuilder, String indexName, boolean isPrintLog, String fieldName, double... customSegment) throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        PercentilesAggregationBuilder aggregation = AggregationBuilders
                .percentiles("percentiles_" + fieldName)
                .field(fieldName).percentiles(customSegment);
        if (queryBuilder != null) {
            searchSourceBuilder.query(queryBuilder);
        }
        searchSourceBuilder.size(0);
        searchSourceBuilder.aggregation(aggregation);
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);
        if (isPrintLog) {
            log.info(searchSourceBuilder.toString());
        }
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        Map<Double, Double> map = new LinkedHashMap<>();
        Percentiles agg = searchResponse.getAggregations().get("percentiles_" + fieldName);
        for (Percentile entry : agg) {
            double percent = entry.getPercent();
            double value = entry.getValue();
            map.put(percent, value);
        }
        return map;
    }

    @Override
    public Map<Double, Double> percentileRanksAgg(QueryBuilder queryBuilder, Class<T> clazz, Fn<T, ?> fn, double... customSegment) throws Exception {
        MetaData md = MetaTools.getIndexMetaData(clazz);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        PercentileRanksAggregationBuilder aggregation = AggregationBuilders
                .percentileRanks("percentiles_" + Reflections.fnToFieldName(fn), customSegment)
                .field(Reflections.fnToFieldName(fn));
        if (queryBuilder != null) {
            searchSourceBuilder.query(queryBuilder);
        }
        searchSourceBuilder.size(0);
        searchSourceBuilder.aggregation(aggregation);
        SearchRequest searchRequest = new SearchRequest(md.getIndexName());
        searchRequest.source(searchSourceBuilder);
        if (md.isPrintLog()) {
            log.info(searchSourceBuilder.toString());
        }
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        Map<Double, Double> map = new LinkedHashMap<>();
        PercentileRanks agg = searchResponse.getAggregations().get("percentiles_" + Reflections.fnToFieldName(fn));
        for (Percentile entry : agg) {
            double percent = entry.getPercent();
            double value = entry.getValue();
            map.put(percent, value);
        }
        return map;
    }

    @Override
    public Aggregations agg(AggregationBuilder aggregationBuilder, QueryBuilder queryBuilder, Class<T> clazz) throws IOException {
        MetaData md = MetaTools.getIndexMetaData(clazz);
        return this.agg(aggregationBuilder, queryBuilder, md.getIndexName(), md.isPrintLog());
    }

    @Override
    public Aggregations agg(AggregationBuilder aggregationBuilder, QueryBuilder queryBuilder, String indexName, boolean isPrintLog) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        if (queryBuilder != null) {
            searchSourceBuilder.query(queryBuilder);
        }
        searchSourceBuilder.size(0);
        searchSourceBuilder.trackTotalHits(false);//性能开销大,关闭
        searchSourceBuilder.aggregation(aggregationBuilder);
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);
        if (isPrintLog) {
            log.info(searchSourceBuilder.toString());
        }
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return searchResponse.getAggregations();
    }

    @Override
    public List<HashMap<String, Object>> agg2Map(AggregationBuilder aggregationBuilder, QueryBuilder queryBuilder, String indexName, boolean isPrintLog) throws IOException {
        List<HashMap<String, Object>> result = Lists.newArrayList();
        HashMap<String, Object> temp_ = Maps.newHashMapWithExpectedSize(16);
        this.aggHandle(this.agg(aggregationBuilder,queryBuilder,indexName,isPrintLog),result,temp_);
        return result;
    }

    @Override
    public Aggregations compositeAgg(QueryBuilder queryBuilder, String indexName, boolean isPrintLog, Integer size, AggregationBuilder subAgg, CompositeValuesSourceBuilder... cvs) throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        if (queryBuilder != null) {
            searchSourceBuilder.query(queryBuilder);
        }
        searchSourceBuilder.size(0);
        searchSourceBuilder.trackTotalHits(false);//性能开销大,关闭
        CompositeAggregationBuilder compositeAggregationBuilder = new CompositeAggregationBuilder("composite_agg",Lists.newArrayList(cvs));
        if(size !=null && size>0){
            compositeAggregationBuilder.size(size);
        }else{
            compositeAggregationBuilder.size(ESConst.ES_COMPOSITE_SIZE);
        }
        if(subAgg!=null){
            compositeAggregationBuilder.subAggregation(subAgg);
        }
        searchSourceBuilder.aggregation(compositeAggregationBuilder);
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);
        if (isPrintLog) {
            log.info(searchSourceBuilder.toString());
        }
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return searchResponse.getAggregations();
    }
    @Override
    public CompositeAggResponse compositeAgg2Map(QueryBuilder queryBuilder, Class<T> clazz, Integer size, AggregationBuilder subAgg, CompositeValuesSourceBuilder... cvs) throws Exception {
        MetaData md = MetaTools.getIndexMetaData(clazz);
        return this.compositeAgg2Map(queryBuilder, md.getIndexName(), md.isPrintLog(),size,subAgg,cvs);
    }

    @Override
    public CompositeAggResponse compositeAgg2Map(QueryBuilder queryBuilder, Class<T> clazz, Integer size, AggregationBuilder subAgg, Fn<T, ?>... fns) throws Exception {
        MetaData md = MetaTools.getIndexMetaData(clazz);
        CompositeValuesSourceBuilder[] cvs = new CompositeValuesSourceBuilder[fns.length];
        for (int i = 0; i < fns.length; i++) {
            cvs[i] = new TermsValuesSourceBuilder(Reflections.fnToFieldName(fns[i]))
                    .field(Reflections.fnToFieldName(fns[i]))
                    .missingBucket(true);
        }
        return this.compositeAgg2Map(queryBuilder, md.getIndexName(), md.isPrintLog(),size,subAgg,cvs);
    }

    @Override
    public CompositeAggResponse compositeAgg2Map(QueryBuilder queryBuilder, String indexName, boolean isPrintLog, Integer size, AggregationBuilder subAgg,String... fields) throws Exception {
        CompositeValuesSourceBuilder[] cvs = new CompositeValuesSourceBuilder[fields.length];
        for (int i = 0; i < fields.length; i++) {
            cvs[i] = new TermsValuesSourceBuilder(fields[i])
                    .field(fields[i])
                    .missingBucket(true);
        }
        return this.compositeAgg2Map(queryBuilder, indexName, isPrintLog,size,subAgg,cvs);
    }

    @Override
    public CompositeAggResponse compositeAgg2Map(QueryBuilder queryBuilder, String indexName, boolean isPrintLog, Integer size, AggregationBuilder subAgg, CompositeValuesSourceBuilder... cvs) throws Exception {
        List<HashMap<String, Object>> result = Lists.newArrayList();
        HashMap<String, Object> temp_ = Maps.newHashMapWithExpectedSize(16);
        CompositeAggResponse compositeAggResponse = new CompositeAggResponse();
        Aggregations agg = this.compositeAgg(queryBuilder,indexName,isPrintLog,size,subAgg,cvs);
        //获取具体分桶数据
        this.aggHandle(agg,result,temp_);
        ParsedComposite parsedComposite = agg.get("composite_agg");
        //获取分页查询下一页参数 7.6版本改为非全量查询,性能提升
        compositeAggResponse.setAfterKey(parsedComposite.afterKey());
        compositeAggResponse.setBuckets(result);

        return compositeAggResponse;
    }

    /**
     * 递归处理聚合
     * @param agg
     * @param result
     * @param temp_
     */
    public void aggHandle(Aggregations agg, List<HashMap<String, Object>> result, HashMap<String, Object> temp_) {
        String name = "";
        for (Aggregation data : agg) {
            name = data.getName();
            Object obj = agg.get(name);
            if (obj instanceof Terms) {
                Terms terms = (Terms) obj;
                name = terms.getName();
                if (terms.getBuckets().isEmpty()) {
                    result.add((HashMap<String, Object>) temp_.clone());
                }
                for (Terms.Bucket bucket : terms.getBuckets()) {
                    temp_.put(name, bucket.getKeyAsString());
                    temp_.put("count", bucket.getDocCount());
                    if (bucket.getAggregations().asList() == null ||
                            bucket.getAggregations().asList().isEmpty()) {
                        result.add((HashMap<String, Object>) temp_.clone());
                    } else {
                        aggHandle(bucket.getAggregations(), result, temp_);
                    }
                }
            } else if (obj instanceof Histogram) {
                Histogram histogram = (Histogram) obj;
                name = histogram.getName();
                if (histogram.getBuckets().isEmpty()) {
                    result.add((HashMap<String, Object>) temp_.clone());
                }
                for (Histogram.Bucket bucket : histogram.getBuckets()) {
                    temp_.put(name, bucket.getKeyAsString());
                    temp_.put("count", bucket.getDocCount());
                    if (bucket.getAggregations().asList() == null ||
                            bucket.getAggregations().asList().isEmpty()) {
                        result.add((HashMap<String, Object>) temp_.clone());
                    } else {
                        aggHandle(bucket.getAggregations(), result, temp_);
                    }
                }
            } else if(obj instanceof ParsedComposite){
                ParsedComposite parsedComposite = (ParsedComposite) obj;
                name = parsedComposite.getName();
                if (parsedComposite.getBuckets().isEmpty()) {
                    result.add((HashMap<String, Object>) temp_.clone());
                }
                for (ParsedComposite.ParsedBucket bucket : parsedComposite.getBuckets()) {
                    for(Map.Entry<String,Object> m : bucket.getKey().entrySet()){
                        temp_.put(m.getKey(), m.getValue());
                    }
                    temp_.put("count", bucket.getDocCount());
                    if (bucket.getAggregations().asList() == null ||
                            bucket.getAggregations().asList().isEmpty()) {
                        result.add((HashMap<String, Object>) temp_.clone());
                    } else {
                        aggHandle(bucket.getAggregations(), result, temp_);
                    }
                }
            } else if (obj instanceof Max) {
                Max max = (Max)obj;
                temp_.put(max.getName(),max.getValue());
                result.add((HashMap<String, Object>) temp_.clone());
            } else if (obj instanceof Min) {
                Min min = (Min)obj;
                temp_.put(min.getName(),min.getValue());
                result.add((HashMap<String, Object>) temp_.clone());
            } else if (obj instanceof Avg) {
                Avg avg = (Avg)obj;
                temp_.put(avg.getName(),avg.getValue());
                result.add((HashMap<String, Object>) temp_.clone());
            } else if (obj instanceof Sum) {
                Sum sum = (Sum)obj;
                temp_.put(sum.getName(),sum.getValue());
                result.add((HashMap<String, Object>) temp_.clone());
            } else if (obj instanceof ValueCount) {
                ValueCount vc = (ValueCount)obj;
                temp_.put(vc.getName(),vc.getValue());
                result.add((HashMap<String, Object>) temp_.clone());
            } else if (obj instanceof Stats) {
                Stats stats = (Stats)obj;
                temp_.put(stats.getName(),stats);
                result.add((HashMap<String, Object>) temp_.clone());
            }
        }
    }

    @Override
    public Map getById(M id, String indexName) throws Exception {
        if (org.springframework.util.StringUtils.isEmpty(id)) {
            throw new Exception("ID cannot be empty");
        }
        GetRequest getRequest = new GetRequest(indexName, ESConst.ES_INDEX_TYPE, id.toString());
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        if (getResponse.isExists()) {
            Map<String, Object> map = getResponse.getSourceAsMap();
            //手动映射ID
            map.put(ID, getResponse.getId());
            return map;
        }
        return null;
    }

    @Override
    public T getById(M id, Class<T> clazz) throws Exception {
        MetaData metaData = MetaTools.getIndexMetaData(clazz);
        if (org.springframework.util.StringUtils.isEmpty(id)) {
            throw new Exception("ID cannot be empty");
        }
        GetRequest getRequest = new GetRequest(metaData.getIndexName(), ESConst.ES_INDEX_TYPE, id.toString());
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        if (getResponse.isExists()) {
            T t = JsonUtil.toT(getResponse.getSourceAsString(), clazz);
            //将_id字段重新赋值给@ESID注解的字段,但忽略ignoreCreate为true字段
            correctID(clazz, t, (M) getResponse.getId());
            return t;
        }
        return null;
    }

    @Override
    public List<T> getByIds(M[] ids, Class<T> clazz) throws Exception {
        MetaData metaData = MetaTools.getIndexMetaData(clazz);
        MultiGetRequest request = new MultiGetRequest();
        for (int i = 0; i < ids.length; i++) {
            request.add(new MultiGetRequest.Item(metaData.getIndexName(), ESConst.ES_INDEX_TYPE, ids[i].toString()));
        }
        MultiGetResponse response = client.mget(request, RequestOptions.DEFAULT);
        List<T> list = new ArrayList<>();
        for (int i = 0; i < response.getResponses().length; i++) {
            MultiGetItemResponse item = response.getResponses()[i];
            GetResponse getResponse = item.getResponse();
            if (getResponse.isExists()) {
                T t = JsonUtil.toT(getResponse.getSourceAsString(), clazz);
                //将_id字段重新赋值给@ESID注解的字段,但忽略ignoreCreate为true字段
                correctID(clazz, t, (M) getResponse.getId());
                list.add(t);
            }
        }
        return list;
    }

    @Override
    public boolean exists(M id, Class<T> clazz) throws Exception {
        MetaData metaData = MetaTools.getIndexMetaData(clazz);
        if (org.springframework.util.StringUtils.isEmpty(id)) {
            throw new Exception("ID cannot be empty");
        }
        GetRequest getRequest = new GetRequest(metaData.getIndexName(), ESConst.ES_INDEX_TYPE, id.toString());
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        if (getResponse.isExists()) {
            return true;
        }
        return false;
    }

    @Override
    public Response saveTemplate(String templateName, String templateContent) throws Exception {
        Request scriptRequest = new Request("POST", "_scripts/" + templateName);
        scriptRequest.setJsonEntity(templateContent);
        Response scriptResponse = request(scriptRequest);
        return scriptResponse;
    }

    @Override
    public SearchResponse searchByTemplate(Map<String, Object> templateParams, ScriptType st, String template, String indexName, boolean isPrintLog) throws Exception {
        SearchTemplateRequest request = new SearchTemplateRequest();
        SearchRequest searchRequest = new SearchRequest(indexName);
        request.setRequest(searchRequest);
        request.setScriptType(st);
        request.setScript(template);
        request.setScriptParams(templateParams);
        if(isPrintLog){
            //request.setSimulate(true);
            //request.setExplain(true);
            //request.setProfile(true);
            log.info(request.getScript());
            log.info("Params:{}",request.getScriptParams());
        }
        SearchTemplateResponse response = client.searchTemplate(request, RequestOptions.DEFAULT);
        return response.getResponse();
    }

    private static Map<Class, String> classIDMap = new ConcurrentHashMap();

    /**
     * 将_id字段重新赋值给@ESID注解的字段（如果ignore 为true则赋值）
     *
     * @param clazz
     * @param t
     * @param _id
     */
    private void correctID(Class clazz, T t, M _id) {
        try {
            if (org.springframework.util.StringUtils.isEmpty(_id)) {
                return;
            }
            if (classIDMap.containsKey(clazz)) {
                Field field = clazz.getDeclaredField(classIDMap.get(clazz));
                field.setAccessible(true);
                ESID esid = field.getAnnotation(ESID.class);
                //这里不支持非String类型的赋值，如果用默认的id，则id的类型一定是String类型的
                if (esid.ignoreCreate() && field.get(t) == null) {
                    field.set(t, _id);
                }
                return;
            }
            for (int i = 0; i < clazz.getDeclaredFields().length; i++) {
                Field field = clazz.getDeclaredFields()[i];
                field.setAccessible(true);
                ESID esid = field.getAnnotation(ESID.class);
                if (esid != null) {
                    classIDMap.put(clazz, field.getName());
                    //这里不支持非String类型的赋值，如果用默认的id，则id的类型一定是String类型的
                    if (esid.ignoreCreate() && field.get(t) == null) {
                        field.set(t, _id);
                    }
                    return;
                }
            }
        } catch (Exception e) {
            log.error("correctID error!", e);
        }
    }
}
