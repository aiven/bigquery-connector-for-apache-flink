package io.aiven.flink.connectors.bigquery.sink;

import com.google.auth.Credentials;
import com.google.cloud.bigquery.TableId;
import java.io.Serializable;

public class BigQueryConnectionOptions implements Serializable {

  private static final long serialVersionUID = 1L;
  private final Credentials credentials;

  private final TableId tableId;

  public BigQueryConnectionOptions(
      String projectId, String dataset, String tableName, Credentials credentials) {
    this.tableId = TableId.of(projectId, dataset, tableName);
    this.credentials = credentials;
  }

  public TableId getTableId() {
    return tableId;
  }

  public Credentials getCredentials() {
    return credentials;
  }
}
