package io.aiven.flink.connectors.bigquery.sink;

import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.COMPRESSION;
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
import java.util.Collections;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
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
          COMPRESSION);

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    helper.validate();
    Credentials credentials;
    ReadableConfig config = helper.getOptions();
    try (FileInputStream fis = new FileInputStream(config.get(SERVICE_ACCOUNT))) {
      credentials = ServiceAccountCredentials.fromStream(fis);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    BigQueryConnectionOptions options =
        new BigQueryConnectionOptions(
            config.get(PROJECT_ID),
            config.get(DATASET),
            config.get(TABLE),
            config.get(CREATE_TABLE_IF_NOT_PRESENT),
            config.get(DELIVERY_GUARANTEE),
            config.get(COMPRESSION),
            credentials);
    return new BigQuerySink(
        context.getCatalogTable(), context.getCatalogTable().getResolvedSchema(), options);
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
