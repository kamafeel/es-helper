package com.es.helper.es.utils;

import com.google.common.collect.Maps;
import com.es.helper.es.annotation.ESID;
import com.es.helper.es.annotation.ESMetaData;
import com.es.helper.es.entity.MetaData;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zhangqi
 * 获取Index实体注释信息
 */
public class MetaTools {

    private static final Map<Class, MetaData> META_DATA_MAP = Maps.newConcurrentMap();
    private static final Map<Class, Field> ESID_FIELD_DATA_MAP = Maps.newConcurrentMap();

    /**
     * 获取索引元数据
     * @param clazz
     * @return
     */
    public static MetaData getIndexMetaData(Class<?> clazz){
        //Map类型不做处理
        if(Map.class.isAssignableFrom(clazz)){
            return null;
        }
        if (META_DATA_MAP.containsKey(clazz)) {
            return META_DATA_MAP.get(clazz);
        }
        if(clazz.getAnnotation(ESMetaData.class) != null){
            MetaData metaData = new MetaData();
            metaData.setIndexName(clazz.getAnnotation(ESMetaData.class).indexName());
            metaData.setPrintLog(clazz.getAnnotation(ESMetaData.class).printLog());
            metaData.setNumber_of_replicas(clazz.getAnnotation(ESMetaData.class).number_of_replicas());
            metaData.setNumber_of_shards(clazz.getAnnotation(ESMetaData.class).number_of_shards());
            metaData.setIndexMappingPath(clazz.getAnnotation(ESMetaData.class).indexMappingPath());
            META_DATA_MAP.put(clazz,metaData);
            return metaData;
        }
        return null;
    }

    /**
     * 根据对象中的注解获取ID的字段值
     * @param obj
     * @return
     */
    public static String getESId(Object obj) throws Exception {
        if(ESID_FIELD_DATA_MAP.containsKey(obj.getClass())){
            Field f = ESID_FIELD_DATA_MAP.get(obj.getClass());
            f.setAccessible(true);
            Object value = f.get(obj);
            if(value == null){
                return null;
            }else{
                return value.toString();
            }
        }
        Field[] fields = obj.getClass().getDeclaredFields();
        for(Field f : fields){
            f.setAccessible(true);
            ESID esid = f.getAnnotation(ESID.class);
            if(esid != null){
                ESID_FIELD_DATA_MAP.put(obj.getClass(),f);
                Object value = f.get(obj);
                if(value == null){
                    return null;
                }else{
                    return value.toString();
                }
            }
        }
        return null;
    }

    public static void ignoreEsId(Object obj) throws IllegalAccessException {
        if(ESID_FIELD_DATA_MAP.containsKey(obj.getClass())){
            Field f = ESID_FIELD_DATA_MAP.get(obj.getClass());
            f.setAccessible(true);
            ESID esid = f.getAnnotation(ESID.class);
            if(esid.ignoreCreate()){
                f.set(obj,null);
            }
        }
        Field[] fields = obj.getClass().getDeclaredFields();
        for(Field f : fields){
            f.setAccessible(true);
            ESID esid = f.getAnnotation(ESID.class);
            if(esid != null){
                ESID_FIELD_DATA_MAP.put(obj.getClass(),f);
                if(esid.ignoreCreate()){
                    f.set(obj,null);
                }
            }
        }
    }

    /**
     * 获取o中所有的字段有值的map组合
     * @return
     */
    public static Map getFieldValue(Object o) throws IllegalAccessException {
        Map retMap = new HashMap();
        Field[] fs = o.getClass().getDeclaredFields();
        for(int i = 0;i < fs.length;i++){
            Field f = fs[i];
            f.setAccessible(true);
            ESID esid = f.getAnnotation(ESID.class);
            if(esid == null && f.get(o) != null){//忽略ID和空值信息
                retMap.put(f.getName(),f.get(o) );
            }
        }
        return retMap;
    }

    /**
     * 获取o中指定的字段的值
     * @return
     */
    public static Object getFieldValue(Object o,String fieldName) throws NoSuchFieldException, IllegalAccessException {
        Field f = o.getClass().getDeclaredField(fieldName);
        f.setAccessible(true);
        return f.get(o);
    }

    /**
     * 设置o中指定的字段的值
     * @return
     */
    public static void setFieldValue(Object o,String fieldName,String value) throws NoSuchFieldException, IllegalAccessException {
        Field f = o.getClass().getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(o,value);
    }
}
