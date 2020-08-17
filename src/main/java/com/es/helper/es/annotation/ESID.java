package com.es.helper.es.annotation;

import java.lang.annotation.*;

/**
 * @author zhangqi
 * ES entity 标识ID的注解,在es entity field上添加
 * ignoreCreate 为 true 不对此属性进行ES保存
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
@Documented
public @interface ESID {
    boolean ignoreCreate() default false;
}
