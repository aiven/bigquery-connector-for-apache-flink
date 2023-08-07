package io.aiven.flink.connectors.bigquery.sink;

public class BigQueryConnectorRuntimeException extends RuntimeException {
  public BigQueryConnectorRuntimeException() {}

  public BigQueryConnectorRuntimeException(String message) {
    super(message);
  }

  public BigQueryConnectorRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }

  public BigQueryConnectorRuntimeException(Throwable cause) {
    super(cause);
  }

  public BigQueryConnectorRuntimeException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
