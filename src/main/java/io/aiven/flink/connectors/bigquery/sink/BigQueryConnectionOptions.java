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

  private final boolean createIfNotExists;
  private final DeliveryGuarantee deliveryGuarantee;

  public BigQueryConnectionOptions(
      String project,
      String dataset,
      String table,
      boolean createIfNotExists,
      DeliveryGuarantee deliveryGuarantee,
      Credentials credentials) {
    this.project = project;
    this.dataset = dataset;
    this.table = table;
    this.createIfNotExists = createIfNotExists;
    this.deliveryGuarantee = deliveryGuarantee;
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

  public DeliveryGuarantee getDeliveryGuarantee() {
    return deliveryGuarantee;
  }
}
