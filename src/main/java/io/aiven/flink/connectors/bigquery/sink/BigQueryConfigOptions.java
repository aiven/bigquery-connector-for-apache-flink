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

  public static final ConfigOption<DeliveryGuarantee> DELIVERY_GUARANTEE =
      ConfigOptions.key("delivery-guarantee")
          .enumType(DeliveryGuarantee.class)
          .defaultValue(DeliveryGuarantee.AT_LEAST_ONCE)
          .withDescription("Determines delivery guarantee");

  public static final ConfigOption<Long> MAX_OUTSTANDING_ELEMENTS_COUNT =
      ConfigOptions.key("max-outstanding-elements-count")
          .longType()
          .defaultValue(10000L)
          .withDescription("Determines maximum number of concurrent requests to BigQuery");

  public static final ConfigOption<Long> MAX_OUTSTANDING_REQUEST_BYTES =
      ConfigOptions.key("max-outstanding-request-bytes")
          .longType()
          .defaultValue(100 * 1024 * 1024L)
          .withDescription("Determines maximum sum of request sizes in bytes to BigQuery");
}
