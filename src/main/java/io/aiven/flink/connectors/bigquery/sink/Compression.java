package io.aiven.flink.connectors.bigquery.sink;

public enum Compression {
  GZIP("gzip"),
  NO_COMPRESSION("no compression");

  private final String value;

  Compression(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
