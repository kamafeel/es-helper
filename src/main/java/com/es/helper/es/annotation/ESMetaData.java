package com.es.helper.es.annotation;

import java.lang.annotation.*;

/**
 * @author zhangqi
 * es索引元数据的注解，在es entity class上添加
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Documented
public @interface ESMetaData {

    /**
     * 索引名称，必须配置
     */
    String indexName();
    /**
     * 主分片数量
     * @return
     */
    int number_of_shards() default 1;
    /**
     * 备份分片数量
     * @return
     */
    int number_of_replicas() default 1;

    /**
     * 是否打印日志
     * @return
     */
    boolean printLog() default false;

    /**
     * 索引mappings文件相对位置
     * @return
     */
    String indexMappingPath() default "";
}
