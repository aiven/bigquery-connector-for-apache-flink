package io.aiven.flink.connectors.bigquery.sink;

import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.CREATE_TABLE_IF_NOT_PRESENT;
import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.DATASET;
import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.DELIVERY_GUARANTEE;
import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.MAX_OUTSTANDING_ELEMENTS_COUNT;
import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.MAX_OUTSTANDING_REQUEST_BYTES;
import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.PROJECT_ID;
import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.SERVICE_ACCOUNT;
import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.TABLE;

import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.storage.v1.TableName;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import org.apache.flink.configuration.ReadableConfig;

public class BigQueryConnectionOptions implements Serializable {

  private static final long serialVersionUID = 1L;
  private final Credentials credentials;

  private final String project;
  private final String dataset;
  private final String table;
  private final long maxOutstandingElementsCount;
  private final long maxOutstandingRequestBytes;

  private final boolean createIfNotExists;
  private final DeliveryGuarantee deliveryGuarantee;

  public BigQueryConnectionOptions(
      String project,
      String dataset,
      String table,
      boolean createIfNotExists,
      DeliveryGuarantee deliveryGuarantee,
      long maxOutstandingElementsCount,
      long maxOutstandingRequestBytes,
      Credentials credentials) {
    this.project = project;
    this.dataset = dataset;
    this.table = table;
    this.createIfNotExists = createIfNotExists;
    this.deliveryGuarantee = deliveryGuarantee;
    this.credentials = credentials;
    this.maxOutstandingElementsCount = maxOutstandingElementsCount;
    this.maxOutstandingRequestBytes = maxOutstandingRequestBytes;
  }

  public static BigQueryConnectionOptions fromReadableConfig(ReadableConfig config) {
    final Credentials credentials;
    try (FileInputStream fis = new FileInputStream(config.get(SERVICE_ACCOUNT))) {
      credentials = ServiceAccountCredentials.fromStream(fis);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return new BigQueryConnectionOptions(
        config.get(PROJECT_ID),
        config.get(DATASET),
        config.get(TABLE),
        config.get(CREATE_TABLE_IF_NOT_PRESENT),
        config.get(DELIVERY_GUARANTEE),
        config.get(MAX_OUTSTANDING_ELEMENTS_COUNT),
        config.get(MAX_OUTSTANDING_REQUEST_BYTES),
        credentials);
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

  public long getMaxOutstandingElementsCount() {
    return maxOutstandingElementsCount;
  }

  public long getMaxOutstandingRequestBytes() {
    return maxOutstandingRequestBytes;
  }
}
