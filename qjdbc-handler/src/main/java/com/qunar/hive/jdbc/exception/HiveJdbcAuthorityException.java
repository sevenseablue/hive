package com.qunar.hive.jdbc.exception;

/**
 * Created with Lee. Date: 2019/9/10 Time: 11:50 To change this template use File | Settings | File
 * Templates. Description:
 *
 * @author : hongweis.li
 */

public class HiveJdbcAuthorityException extends HiveJdbcStorageException {

  public HiveJdbcAuthorityException() {
    super();
  }

  public HiveJdbcAuthorityException(String message) {
    super(message);
  }

  public HiveJdbcAuthorityException(Throwable cause) {
    super(cause);
  }

  public HiveJdbcAuthorityException(String message, Throwable cause) {
    super(message, cause);
  }
}
