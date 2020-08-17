package com.es.helper.es.entity;


import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhangqi
 * Composite封装对象
 */
@Data
public class CompositeAggResponse {
    private Map<String, Object> afterKey;
    private List<HashMap<String, Object>> buckets;
}
