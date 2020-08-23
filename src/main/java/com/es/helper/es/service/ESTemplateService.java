package com.es.helper.es.service;

import com.es.helper.annotation.Fn;
import com.es.helper.es.entity.CompositeAggResponse;
import com.es.helper.es.entity.MetaData;
import com.es.helper.es.entity.PageList;
import com.es.helper.es.entity.ScrollResponse;
import com.es.helper.es.utils.MetaTools;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author zhangqi
 * ES实体操作类,ES字段需要使用驼峰
 * T 类型
 * M id
 * 基于ES 6.7.1
 **/
public interface ESTemplateService<T,M> {

    /**
     * LowLevelClient查询模式
     * @param request
     * @return
     * @throws Exception
     */
    Response request(Request request) throws Exception;

    /**
     * 原生查询
     * @param request
     * @return
     */
    SearchResponse request(SearchRequest request) throws IOException;

    /**
     * 新增文档
     * 带ID速度将变慢,无特殊情况,不指定ID
     * @param t 文档pojo
     */
    boolean save(T t) throws Exception;

    /**
     * 保存Map对象到 文档
     * 不保存ID  _id
     * @param m
     * @param indexName
     * @return
     * @throws Exception
     */
    boolean save(Map m, String indexName) throws Exception;

    /**
     * 新增文档（路由方式）
     * @param t 文档pojo
     * @param routing 路由信息（默认路由为文档数据_id）
     * @return
     * @throws Exception
     */
    boolean save(T t, String routing) throws Exception;

    /**
     * 新增文档集合
     * （分批方式，提升性能，防止es服务内存溢出，每批默认3000条数据）
     * 如果ID存在,不做任何操作。
     * @param list 文档pojo集合
     */
     List<BulkResponse> saveBatch(List<T> list) throws Exception;

    /**
     * 新增文档集合
     *（分批方式，提升性能，防止es服务内存溢出，每批默认3000条数据）
     * _id Key作为ID字段,不会保存
     * @param list
     * @param indexName
     * @return
     * @throws Exception
     */
    List<BulkResponse> saveBatch(List<Map<String,Object>> list, String indexName) throws Exception;

    /**
     * 更新文档集合
     * （分批方式，提升性能，防止es服务内存溢出，每批默认3000条数据）
     * 如果ID存在,将更新,否则放弃
     * @param list
     * @param indexName
     * @return
     * @throws Exception
     */
    List<BulkResponse> updateBatch(List<Map<String,Object>> list, String indexName) throws Exception;

    /**
     * 更新文档集合
     * （分批方式，提升性能，防止es服务内存溢出，每批默认3000条数据）
     * 如果ID存在,将更新,否则放弃
     * @param list 文档pojo集合
     * @return
     * @throws Exception
     */
    List<BulkResponse> updateBatch(List<T> list) throws Exception;

    /**
     *  更新文档集合(使用脚本)
     * （分批方式，提升性能，防止es服务内存溢出，每批默认3000条数据）
     *  如果ID存在,将更新,否则放弃
     *
     * @param list
     * @param script painless脚本  ctx._source.count += params.count
     * @param fn
     * @return
     * @throws Exception
     */
    List<BulkResponse> updateBatchByScript(List<T> list, String script, Fn<T, ?>... fn) throws Exception;

    List<BulkResponse> updateBatchByScript(List<Map<String, Object>> list, String script, String indexName) throws Exception;

    /**
     * 按照有值字段更新文档
     * @param t 文档pojo
     */
     boolean update(T t) throws Exception;

    /**
     * 按MAP更新文档
     * @param t
     * @param indexName
     * @return
     * @throws Exception
     */
    boolean update(Map t,String indexName) throws Exception;
    
    /**
     * 删除文档
     * ID必须存在
     * @param t 文档pojo
     */
     boolean delete(T t) throws Exception;


    /**
     * 删除文档（路由方式）
     * @param t 文档pojo
     * @param routing 路由信息（默认路由为文档数据_id）
     * @return
     * @throws Exception
     */
     boolean delete(T t, String routing) throws Exception;


    /**
     * 根据条件删除文档
     * @param queryBuilder 查询条件（官方） 为NULL,将删除所有数据
     * @param clazz 文档pojo类类型
     * @return
     * @throws Exception
     */
     BulkByScrollResponse deleteByCondition(QueryBuilder queryBuilder, String indexName) throws Exception;
     default BulkByScrollResponse deleteByCondition(QueryBuilder queryBuilder, Class<T> clazz) throws Exception{
         return this.deleteByCondition(queryBuilder,MetaTools.getIndexMetaData(clazz).getIndexName());
     }

    /**
     * 删除文档
     * @param id 文档主键
     * @param clazz 文档pojo类类型
     * @return
     * @throws Exception
     */
     boolean deleteById(M id, Class<T> clazz) throws Exception;

    /**
     * 根据ID查询
     * @param id 文档数据id值
     * @param clazz 文档pojo类类型
     * @return
     * @throws Exception
     */
    T getById(M id, Class<T> clazz) throws Exception;

    Map getById(M id, String indexName) throws Exception;

    /**
     * 根据ID列表批量查询
     * @param ids 文档数据id值数组
     * @param clazz 文档pojo类类型
     * @return
     * @throws Exception
     */
    List<T> getByIds(M[] ids, Class<T> clazz) throws Exception;

    /**
     * id数据是否存在
     * @param id 文档数据id值
     * @param clazz 文档pojo类类型
     * @return
     */
    boolean exists(M id, Class<T> clazz) throws Exception;


    /**
     * 原生查询
     * @param searchRequest 原生查询请求对象
     * @return
     * @throws Exception
     */
     SearchResponse search(SearchRequest searchRequest) throws Exception;

    /**
     * 分页查询
     * @param queryBuilder 查询条件
     * @param clazz 文档pojo类类型
     * @param pageSize 每页数量
     * @param pageNum 第几页
     * @return
     * @throws Exception
     */
    PageList<T> search(QueryBuilder queryBuilder, SortBuilder<?> sort, Class<T> clazz, int pageSize, int pageNum, String[] includes, String[] excludes) throws Exception;

    default PageList<T> search(QueryBuilder queryBuilder, SortBuilder<?> sort, Class<T> clazz, int pageSize, int pageNum) throws Exception{
        return search(queryBuilder,sort,clazz,pageSize,pageNum,null,null);
    }
    /**
     * 分页查询 返回为MAP
     * id值在 key为id里面
     * @param queryBuilder 查询条件(过滤，排序自行编写)
     * @param pageSize 每页数量
     * @param pageNum 第几页
     * @return
     * @throws Exception
     */
    PageList<Map<String, Object>> searchToMap(QueryBuilder queryBuilder, SortBuilder<?> sort, String indexName,boolean isPrintLog, int pageSize, int pageNum,String[] includes, String[] excludes) throws Exception;

    default PageList<Map<String, Object>> searchToMap(QueryBuilder queryBuilder, SortBuilder<?> sort, String indexName,boolean isPrintLog, int pageSize, int pageNum) throws Exception{
        return searchToMap(queryBuilder,sort,indexName,isPrintLog,pageSize,pageNum,null,null);
    }
    /**
     * 查询数量
     * @param queryBuilder 查询条件
     * @param clazz 文档pojo类类型
     * @return
     * @throws Exception
     */
     long count(QueryBuilder queryBuilder, Class<T> clazz) throws Exception;

    /**
     *  折叠 此方法可以理解为去重合并
     * @param queryBuilder
     * @param clazz
     * @param fn
     * @return
     * @throws IOException
     */
    Collection<String> collapse(QueryBuilder queryBuilder, Class<T> clazz, Fn<T, ?> fn) throws IOException;

    /**
     * 同上
     * @param queryBuilder
     * @param indexName
     * @param fieldName
     * @return
     * @throws IOException
     */
    Collection<String> collapse(QueryBuilder queryBuilder, String indexName, String fieldName) throws IOException;

    /**
     *
     * @param queryBuilder
     * @param clazz
     * @param fn
     * @param innerHits
     * @return
     * @throws IOException
     */
    SearchResponse collapse(QueryBuilder queryBuilder, Class<T> clazz, Fn<T, ?> fn, List<InnerHitBuilder> innerHits) throws IOException;

    SearchResponse collapse(QueryBuilder queryBuilder, String indexName, String fieldName, List<InnerHitBuilder> innerHits) throws IOException;

    /**
     * scroll方式查询，创建scroll
     * 游标查询 先查询初始化,再批量拉取结果，初始化后文档任何变化将忽略
     * @param queryBuilder 查询条件
     * @param clazz 文档pojo类类型
     * @param tv scroll窗口时间，快速数据
     * @param size scroll查询每次查询条数
     * @return
     * @throws Exception
     */
    ScrollResponse<T> createScroll(QueryBuilder queryBuilder, Class<T> clazz, TimeValue tv, Integer size) throws Exception;

    /**
     * scroll方式查询
     * 用于快速大量的数据查询，或者高频分页,滚动下拉
     * @param clazz 文档pojo类类型
     * @param tv scroll窗口时间，单位：小时
     * @param scrollId scroll查询id，from ScrollResponse
     * @return
     * @throws Exception
     */
    ScrollResponse<T> queryScroll(Class<T> clazz, TimeValue tv, String scrollId) throws Exception;

    /**
     * 搜索建议Completion Suggester
     * @param fieldName 搜索建议对应查询字段
     * @param fieldValue 搜索建议查询条件
     * @param clazz 文档pojo类类型
     * @return
     * @throws Exception
     */
     List<String> completionSuggest(String fieldName, String fieldValue, Class<T> clazz) throws Exception;

    /**
     * 基数聚合 类似 count(distinct Fn) 返回一个近似值，数据量较小是基本是完全正确的
     * HyperLogLog++ Hash计数法
     * @param queryBuilder
     * @param clazz
     * @param fn
     * @param precisionThreshold 最大不超过40000,超过此数值以上的计数更加模糊，非精确
     * @param script 如果不为空,采用script模式进行cardinality
     * @return
     * @throws Exception
     */
    long cardinalityAgg(QueryBuilder queryBuilder, Class<T> clazz, Fn<T, ?> fn, Script script, Long precisionThreshold) throws Exception;

    /**
     *  百分比聚合
     *  找出平均值无法体现的异常值分布情况,找出数据是否存在偏斜,双峰等
     * @param queryBuilder
     * @param clazz
     * @param customSegment 自定义分析百分比 比如 1.0 10.0 30.0 95.0 99.0
     * @param fn
     * @return 百分比出现的观察值  1.0 15  10.0 23  ....
     * @throws Exception
     */
    Map<Double, Double> percentileAgg(QueryBuilder queryBuilder, Class<T> clazz, Fn<T, ?> fn,double... customSegment) throws Exception;

    /**
     * 不绑定Class
     * @param queryBuilder
     * @param indexName
     * @param fieldName
     * @param customSegment
     * @return
     * @throws Exception
     */
    Map<Double, Double> percentileAgg(QueryBuilder queryBuilder, String indexName, boolean isPrintLog,String fieldName, double... customSegment) throws Exception;

    /**
     *  百分比范围聚合
     *  找出数据分布状态
     * @param queryBuilder
     * @param clazz
     * @param customSegment 自定义分析的值数组
     * @param fn
     * @return 分析值数组，对应的百分比
     * @throws Exception
     */
    Map<Double, Double> percentileRanksAgg(QueryBuilder queryBuilder, Class<T> clazz, Fn<T, ?> fn, double... customSegment) throws Exception;

    /**
     * Aggregation 业务实现灵活，层级多;order排序,size,filter,Script等
     *
     * terms聚合(Script missing空缺默认值等)
     * Metrics聚合 (Min,Max,Sum,Avg stats extended_stats,count,)
     * range聚合
     * dateRange 时间范围聚合
     * IpRange IP范围聚合
     * histogram 直方图
     * dataHistogram 日期直方图
     * geohashGrid 地理哈希网格聚合
     * geoDistance 地理位置聚合
     * TOP hits聚合
     * TODO 不再进行封装
     * 不返回query部分的Hits
     * @param aggregationBuilder
     * @param queryBuilder
     * @param clazz
     * @return
     */
    Aggregations agg(AggregationBuilder aggregationBuilder, QueryBuilder queryBuilder, Class<T> clazz) throws IOException;

    /**
     * 同上,不强制绑定class
     * @param aggregationBuilder
     * @param queryBuilder
     * @param indexName
     * @param isPrintLog
     * @return
     * @throws IOException
     */
    Aggregations agg(AggregationBuilder aggregationBuilder, QueryBuilder queryBuilder, String indexName, boolean isPrintLog) throws IOException;

    /**
     * 分桶结果递归处理为Map
     * @param aggregationBuilder
     * @param queryBuilder
     * @param indexName
     * @param isPrintLog
     * @return
     * @throws IOException
     */
    List<HashMap<String, Object>> agg2Map(AggregationBuilder aggregationBuilder, QueryBuilder queryBuilder, String indexName, boolean isPrintLog) throws IOException;

    /**
     * 多字段复合聚合，建议使用此方法
     * 复合聚合 类似 group by column,column...
     * @param queryBuilder
     * @param indexName
     * @param isPrintLog
     * @param size 分页 索引最好建立 index sorting
     * @param subAgg 复合分类 子聚合 SUM,AVG,MIN等
     * @param cvs
     * @return
     * @throws Exception
     */
    Aggregations compositeAgg(QueryBuilder queryBuilder, String indexName, boolean isPrintLog, Integer size, AggregationBuilder subAgg, CompositeValuesSourceBuilder... cvs)  throws Exception;

    /**
     * 同上
     * @param queryBuilder
     * @param indexName
     * @param isPrintLog
     * @param size
     * @param cvs
     * @return
     * @throws Exception
     */
    CompositeAggResponse compositeAgg2Map(QueryBuilder queryBuilder, String indexName, boolean isPrintLog, Integer size, AggregationBuilder subAgg, CompositeValuesSourceBuilder... cvs) throws Exception;
    CompositeAggResponse compositeAgg2Map(QueryBuilder queryBuilder, Class<T> clazz, Integer size, AggregationBuilder subAgg,CompositeValuesSourceBuilder... cvs) throws Exception;

    /**
     * 对对象属性数组 进行terms方式复合聚合
     * @param queryBuilder
     * @param clazz
     * @param size
     * @param fn 进行terms 复合聚合
     * @return
     * @throws Exception
     */
    CompositeAggResponse compositeAgg2Map(QueryBuilder queryBuilder, Class<T> clazz, Integer size, AggregationBuilder subAgg, Fn<T, ?>... fn) throws Exception;
    CompositeAggResponse compositeAgg2Map(QueryBuilder queryBuilder, String indexName, boolean isPrintLog, Integer size, AggregationBuilder subAgg,String... field) throws Exception;

    /**
     * 保存模板(mustache language ES沙箱语言)
     * @param templateName
     * @param templateContent
     * @return
     * @throws Exception
     */
    Response saveTemplate(String templateName, String templateContent) throws Exception;

    /**
     * ignore
     * @param templateParams
     * @param st
     * @param template
     * @param clazz
     * @return
     * @throws Exception
     */
    default SearchResponse searchByTemplate(Map<String, Object> templateParams, ScriptType st, String template, Class<T> clazz) throws Exception{
        MetaData metaData = MetaTools.getIndexMetaData(clazz);
        return this.searchByTemplate(templateParams,st,template,metaData.getIndexName(),metaData.isPrintLog());
    }

    /**
     *
     * @param templateParams
     * @param st ScriptType.INLINE 或者 ScriptType.STORED
     * @param template 如果是ScriptType.STORED 为模板ID
     *                 如果是ScriptType.INLINE 为模板内容
     * @param indexName
     * @param isPrintLog
     * @return 自行处理模板返回信息
     * @throws Exception
     */
    SearchResponse searchByTemplate(Map<String, Object> templateParams, ScriptType st, String template, String indexName, boolean isPrintLog) throws Exception;

    /**
     * 递归处理聚合
     * @param agg
     * @param result
     * @param temp_
     */
    void aggHandle(Aggregations agg, List<HashMap<String, Object>> result, HashMap<String, Object> temp_);
}
