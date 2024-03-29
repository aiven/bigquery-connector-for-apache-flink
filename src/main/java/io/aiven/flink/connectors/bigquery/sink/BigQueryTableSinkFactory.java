package io.aiven.flink.connectors.bigquery.sink;

import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.CREATE_TABLE_IF_NOT_PRESENT;
import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.DATASET;
import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.DELIVERY_GUARANTEE;
import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.MAX_OUTSTANDING_ELEMENTS_COUNT;
import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.MAX_OUTSTANDING_REQUEST_BYTES;
import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.PROJECT_ID;
import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.SERVICE_ACCOUNT;
import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.TABLE;

import java.util.Collections;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

public class BigQueryTableSinkFactory implements DynamicTableSinkFactory {
  private static final Set<ConfigOption<?>> REQUIRED_OPTIONS =
      Set.of(
          SERVICE_ACCOUNT,
          PROJECT_ID,
          DATASET,
          TABLE,
          CREATE_TABLE_IF_NOT_PRESENT,
          DELIVERY_GUARANTEE,
          MAX_OUTSTANDING_ELEMENTS_COUNT,
          MAX_OUTSTANDING_REQUEST_BYTES);

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    helper.validate();

    BigQueryConnectionOptions options =
        BigQueryConnectionOptions.fromReadableConfig(helper.getOptions());
    return new BigQueryDynamicTableSink(
        context.getCatalogTable(),
        context.getCatalogTable().getResolvedSchema(),
        context.getPhysicalRowDataType(),
        options);
  }

  @Override
  public String factoryIdentifier() {
    return "bigquery";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return REQUIRED_OPTIONS;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Collections.emptySet();
  }
}
