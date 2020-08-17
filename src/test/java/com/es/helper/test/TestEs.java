package com.es.helper.test;

import com.es.helper.es.service.ESIndexService;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.Map;


/**
 * @author zhangqi
 * 测试ES操作类
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Slf4j
public class TestEs {

//    @Resource
//    private ESTemplateService<?,String> esTemplateService;

    @Resource
    private ESIndexService esIndexService;

    @Test
    public void testES() throws Exception {
        //Collection<String> sr = esTemplateService.distinct(null,"fcm_cycle_regularity_dim","busType");
        //System.out.println(sr);

//        Map m = esTemplateService.getById("[KWS-B]ZN-1-3-E-R-S64F-allPro-DAY-mail-20","fcm_cycle_regularity_dim");
//        System.out.println(m);
//        String indexName = "fcm_pro_g_line";
//        BoolQueryBuilder sb = QueryBuilders.boolQuery();
//        AggregationBuilder ab = AggregationBuilders.terms("linename".concat("_Agg_terms"))//线路分组
//                .field("linename").size(Integer.MAX_VALUE)
//                .subAggregation(AggregationBuilders.terms("prokey".concat("_Agg_terms"))//协议,流量分组
//                                .field("prokey").size(Integer.MAX_VALUE)
//                                        .subAggregation(AggregationBuilders.extendedStats("provalue_extendedStats").field("provalue"))).size(Integer.MAX_VALUE)//按value计算数学公式
//                        //.subAggregation(bspab)//按方差临界值过滤桶
//                ;
//        List<HashMap<String, Object>> l = esTemplateService.agg2Map(ab,sb,indexName,true);
//        System.out.println(l);

//        String indexName = "fcm_comm_dim";
//
//        new DateHistogramValuesSourceBuilder("begin").field("begin")
//                .dateHistogramInterval(new DateHistogramInterval("1d")).format("yyyy-MM-dd");
//
//        new TermsValuesSourceBuilder("loc_part").field("loc_part");
//
//        CompositeAggResponse ll = esTemplateService.compositeAgg2Map(null,indexName,true,null,AggregationBuilders.extendedStats("count_stats").field("avgValue"),
//                "lineName","busType");
//
//        System.out.println(ll);


//        String t = "{\n" +
//                "  \"size\": 1,\n" +
//                "  \"query\": {\n" +
//                "    \"match\": {\n" +
//                "      \"prokey\": \"{{param1}}\"\n" +
//                "    }\n" +
//                "  }\n" +
//                "}";
//
//        String t1 = "{\n" +
//                "  \"size\": 0,\n" +
//                "  \"track_total_hits\": false,\n" +
//                "  \"aggregations\": {\n" +
//                "    \"prokey\": {\n" +
//                "      \"terms\": {\n" +
//                "        \"field\": \"{{param1}}\",\n" +
//                "        \"size\": 10,\n" +
//                "        \"min_doc_count\": 1,\n" +
//                "        \"shard_min_doc_count\": 0,\n" +
//                "        \"show_term_doc_count_error\": false,\n" +
//                "        \"order\": [\n" +
//                "          {\n" +
//                "            \"_count\": \"desc\"\n" +
//                "          },\n" +
//                "          {\n" +
//                "            \"_key\": \"asc\"\n" +
//                "          }\n" +
//                "        ]\n" +
//                "      }\n" +
//                "    }\n" +
//                "  }\n" +
//                "}";
//
//        //BoolQueryBuilder bool = QueryBuilders.boolQuery();
//        //bool.filter(QueryBuilders.termQuery("22",11));
//        Map<String,Object> map = Maps.newHashMap();
//        map.put("param1","prokey");
//        SearchResponse sr = esTemplateService.searchByTemplate(map, ScriptType.INLINE,
//                t, "fcm_pro_g_line", true);


        //esTemplateService.agg(AggregationBuilders.terms("prokey").field("prokey"),null,"fcm_pro_g_line", true);


        //esIndexService.createIndex("indexMapping/index_test.json","test_999");

//        Map<String,Object> map = Maps.newHashMap();
//        map.put("linename","test1111");
//        map.put("lnzd","2222");
//        map.put("add1",111);
//        map.put("add_xnhuixhx","223121");
//        esTemplateService.save(map,"test_999");

//        Map<String,Object> map = Maps.newHashMap();
//        map.put(KDomConst.ES_MAPPING_FIELD,"ipPackageNum");
//        map.put(KDomConst.ES_MAPPING_TYPE,"keyword");
//
//        Map<String,Object> map1 = Maps.newHashMap();
//        map1.put(KDomConst.ES_MAPPING_FIELD,"ip_part");
//        map1.put(KDomConst.ES_MAPPING_TYPE,"keyword");

//        Map<String,Object> map2 = Maps.newHashMap();
//        map2.put(KDomConst.ES_MAPPING_FIELD,"company_part");
//        map2.put(KDomConst.ES_MAPPING_TYPE,"keyword");
//
//        esIndexService.putMapping("fcm_comm_dim",map2);
//
//
//        Map<String, Object>  m = esIndexService.getIndexProperties("test_999");
//        System.out.println(m);

        esIndexService.createIndex("indexMapping/test_index.json","test_index");

//        PageList<Map<String, Object>> map =  esTemplateService.searchToMap(null,null,"fcm_comm_dim",true,10,1);
//        System.out.println(map);
    }
}
