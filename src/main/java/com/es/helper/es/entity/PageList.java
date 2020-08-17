package com.es.helper.es.entity;

import lombok.Data;

import java.util.List;

/**
 * @author zhangqi
 * 分页对象封装
 * @param <T>
 */
@Data
public class PageList<T> {
    List<T> records;
    private long total;
}
