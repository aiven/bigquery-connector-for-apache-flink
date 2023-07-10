package io.aiven.flink.connectors.bigquery.sink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class BigQueryConfigOptions {
  public static final ConfigOption<String> SERVICE_ACCOUNT =
      ConfigOptions.key("service-account")
          .stringType()
          .noDefaultValue()
          .withDescription("The path to service account key.");

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

  public static final ConfigOption<Boolean> CREATE_TABLE_IF_NOT_PRESENT =
      ConfigOptions.key("table-create-if-not-exists")
          .booleanType()
          .defaultValue(Boolean.FALSE)
          .withDescription("Determines whether table should be created if not exists");
}
