package com.es.helper.annotation;

import java.io.Serializable;
import java.util.function.Function;

/**
 * @author zhangqi
 * 通过对象方法取属性名
 */
public interface Fn<T, R> extends Function<T, R>, Serializable {
}
