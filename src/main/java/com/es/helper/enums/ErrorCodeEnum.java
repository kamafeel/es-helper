package com.es.helper.enums;

/**
 * 错误码规则为四位纯数字 
 * 1.公共模块 
 * 2.登录模块
 * 
 * @author zhangqi
 *
 */
public enum ErrorCodeEnum {

  /**
   * (保留码) 表示未知异常
   */
  UNEXCEPTED(9999),


  /* 公共模块错误码 */
  /**
   * 成功
   */
  SUCCESS(1),
  /**
   * 用户未登录
   */
  USER_NOT_LOGIN(1001),
  /**
   * 认证令牌过期
   */
  AUTH_TOKEN_EXPIRE(1002),
  /**
   * 非法参数
   */
  ILLEGAL_PARAMETER(1003),
  /**
   * 权限不足
   */
  ACCESS_DENIED(1004),
  /**
   * 操作数据库失败
   */
  FAIL_DATABASE(1006),

  /**
   * 操作数据库失败数据主键冲突
   */
  FAIL_DATABASE_DUPLICATE_KEY(1007);

  private int value;

  private ErrorCodeEnum(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }
}
