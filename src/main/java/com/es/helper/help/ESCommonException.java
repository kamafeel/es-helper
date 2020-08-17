package com.es.helper.help;

import com.es.helper.enums.ErrorCodeEnum;
import org.apache.commons.lang3.StringUtils;

/**
 * @author zhangqi
 * 公共异常
 */
public class ESCommonException extends RuntimeException {
  private static final long serialVersionUID = 8449738842423044010L;

  private ErrorCodeEnum code;

  public ESCommonException(ErrorCodeEnum code) {
    this.code = code;
  }

  public ESCommonException(ErrorCodeEnum code, String message) {
    super(message);
    this.code = code;
  }

  public ErrorCodeEnum getCode() {
    return code;
  }

  @Override
  public String getMessage() {
    if (StringUtils.isBlank(super.getMessage())) {
      return code.toString();
    }
    return super.getMessage();
  }
}
