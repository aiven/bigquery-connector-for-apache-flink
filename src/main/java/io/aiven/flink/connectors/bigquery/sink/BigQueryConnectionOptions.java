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

  public BigQueryConnectionOptions(
      String project, String dataset, String table, Credentials credentials) {
    this.project = project;
    this.dataset = dataset;
    this.table = table;
    this.credentials = credentials;
  }

  public TableName getTableName() {
    return TableName.of(project, dataset, table);
  }

  public Credentials getCredentials() {
    return credentials;
  }
}
