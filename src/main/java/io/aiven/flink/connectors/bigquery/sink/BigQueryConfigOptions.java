package io.aiven.flink.connectors.bigquery.sink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class BigQueryConfigOptions {
  public static final ConfigOption<String> SERVICE_ACCOUNT =
      ConfigOptions.key("service-account")
          .stringType()
          .noDefaultValue()
          .withDescription("The pass to service account key.");
  public static final ConfigOption<String> PROJECT_ID =
      ConfigOptions.key("project-id")
          .stringType()
          .noDefaultValue()
          .withDescription("Google Cloud Project id.");
  public static final ConfigOption<String> DATASET =
      ConfigOptions.key("dataset")
          .stringType()
          .noDefaultValue()
          .withDescription("BigQuery dataset.");
  public static final ConfigOption<String> TABLE =
      ConfigOptions.key("table").stringType().noDefaultValue().withDescription("BigQuery table.");
}
