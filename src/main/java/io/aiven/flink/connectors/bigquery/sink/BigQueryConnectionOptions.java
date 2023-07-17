package io.aiven.flink.connectors.bigquery.sink;

import com.google.auth.Credentials;
import com.google.cloud.bigquery.storage.v1.TableName;
import java.io.Serializable;

public class BigQueryConnectionOptions implements Serializable {

  private static final long serialVersionUID = 1L;
  private final Credentials credentials;

  private final String project;
  private final String dataset;
  private final String table;
  private final int batchSize;
  private final int batchIntervalMs;
  private final int maxRetries;

  private final boolean createIfNotExists;

  public BigQueryConnectionOptions(
      String project,
      String dataset,
      String table,
      boolean createIfNotExists,
      int batchSize,
      int batchIntervalMs,
      int maxRetries,
      Credentials credentials) {
    this.project = project;
    this.dataset = dataset;
    this.table = table;
    this.batchSize = batchSize;
    this.batchIntervalMs = batchIntervalMs;
    this.maxRetries = maxRetries;
    this.createIfNotExists = createIfNotExists;
    this.credentials = credentials;
  }

  public TableName getTableName() {
    return TableName.of(project, dataset, table);
  }

  public Credentials getCredentials() {
    return credentials;
  }

  public boolean isCreateIfNotExists() {
    return createIfNotExists;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public int getBatchIntervalMs() {
    return batchIntervalMs;
  }

  public int getMaxRetries() {
    return maxRetries;
  }
}
