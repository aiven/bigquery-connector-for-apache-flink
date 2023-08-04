package io.aiven.flink.connectors.bigquery.sink;

import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.CREATE_TABLE_IF_NOT_PRESENT;
import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.DATASET;
import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.DELIVERY_GUARANTEE;
import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.PROJECT_ID;
import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.SERVICE_ACCOUNT;
import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.TABLE;

import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import java.io.FileInputStream;
import java.io.IOException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.DeliveryGuarantee;

public class BigQueryConfig {
  protected final ReadableConfig config;
  protected final Credentials credentials;

  public BigQueryConfig(ReadableConfig config) {
    this.config = config;
    try (FileInputStream fis = new FileInputStream(config.get(SERVICE_ACCOUNT))) {
      credentials = ServiceAccountCredentials.fromStream(fis);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Credentials getCredentials() {
    return credentials;
  }

  public String getProjectId() {
    return config.get(PROJECT_ID);
  }

  public String getTableName() {
    return config.get(TABLE);
  }

  public String getDataset() {
    return config.get(DATASET);
  }

  public boolean createTableIfNotExists() {
    return config.get(CREATE_TABLE_IF_NOT_PRESENT);
  }

  public DeliveryGuarantee getDeliveryGuarantee() {
    return config.get(DELIVERY_GUARANTEE);
  }
}
