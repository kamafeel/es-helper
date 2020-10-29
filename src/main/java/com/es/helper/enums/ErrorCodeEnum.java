package com.es.helper.enums;

/**
 * 错误码规则为四位纯数字
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
   * 非法参数
   */
  ILLEGAL_PARAMETER(1003),
  /**
   * 权限不足
   */
  ACCESS_DENIED(1004);

  private int value;

  private ErrorCodeEnum(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }
}
