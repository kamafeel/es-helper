package com.es.helper.es.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * @author zhangqi
 * Scroll封装对象
 * @param <T>
 */
@AllArgsConstructor
@Data
public class ScrollResponse<T> {
    private List<T> list;
    private String scrollId;
}
