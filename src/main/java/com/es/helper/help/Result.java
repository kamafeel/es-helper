package com.es.helper.help;


import com.es.helper.enums.ErrorCodeEnum;
import com.es.helper.utils.SpringContextUtil;

import java.nio.file.AccessDeniedException;

/**
 * @author zhangqi
 * Controller层返回对象封装
 * @param <T>
 */
@SuppressWarnings({"rawtypes", "unchecked", "hiding"})
public class Result<T> {

  private int code;
  private T data;
  private String message;

  public Result() {

  }

  private Result(T data) {
    this.code = ErrorCodeEnum.SUCCESS.getValue();
    this.data = data;
  }

  private Result(ErrorCodeEnum code, String message) {
    ValidateMessageParser vm = SpringContextUtil.getBean(ValidateMessageParser.class);
    if (vm != null) {
      message = vm.parseMessage(code, message);
    }
    this.code = code.getValue();
    this.message = message;
  }

  private Result(ErrorCodeEnum code, String message, T data) {
    ValidateMessageParser vm = SpringContextUtil.getBean(ValidateMessageParser.class);
    if (vm != null) {
      message = vm.parseMessage(code, message);
    }
    this.code = code.getValue();
    this.message = message;
    this.data = data;
  }

  public int getCode() {
    return code;
  }

  public void setCode(int code) {
    this.code = code;
  }

  public T getData() {
    return data;
  }

  public void setData(T data) {
    this.data = data;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }


  public static Result success() {
    return new Result(null);
  }

  /**
   * 解决范型问题，无法展示data基础类型
   */
  public static <U> Result<U> success(U data) {
    return new Result(data);
  }

  public static Result failed(Exception e) {
    if (e instanceof ESCommonException) {
      return fail((ESCommonException) e);
    }else if (e instanceof AccessDeniedException){
      return new Result(ErrorCodeEnum.ACCESS_DENIED, "权限不足");
    } else {
      return new Result(ErrorCodeEnum.UNEXCEPTED, e.getMessage());
    }
  }

  private static Result fail(ESCommonException e) {
    return new Result(e.getCode(), e.getMessage());
  }

  public static Result failed(ErrorCodeEnum ErrorCodeEmum, String message) {
    return new Result(ErrorCodeEmum, message);
  }

  public static Result failed(ErrorCodeEnum ErrorCodeEmum) {
    return new Result(ErrorCodeEmum, null);
  }

  public static <U> Result<U> failed(ErrorCodeEnum ErrorCodeEmum, String message, U data) {
    return new Result(ErrorCodeEmum, message, data);

  }

}
