package com.es.helper.enums;

/**
 * @author zhangqi
 * redis KEY
 *
 */
public enum RedisKey {

  /**
   * 短信验证码
   */
  SMS_CODE(String.class);

  private Class<?> clazz;

  private <T> RedisKey(Class<T> clazz) {
    this.clazz = clazz;
  }

  @SuppressWarnings("unchecked")
  public <T> Class<T> getClazz() {
    return (Class<T>) clazz;
  }
}
