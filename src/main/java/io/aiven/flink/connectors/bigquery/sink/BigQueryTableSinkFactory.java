package io.aiven.flink.connectors.bigquery.sink;

import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.BATCH_INTERVAL_MS;
import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.BATCH_SIZE;
import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.CREATE_TABLE_IF_NOT_PRESENT;
import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.DATASET;
import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.PROJECT_ID;
import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.SERVICE_ACCOUNT;
import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.TABLE;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.MAX_RETRIES;

import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import java.io.FileInputStream;
import java.io.IOException;
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
          BATCH_SIZE,
          BATCH_INTERVAL_MS,
          MAX_RETRIES);

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    helper.validate();
    Credentials credentials;
    try (FileInputStream fis = new FileInputStream(helper.getOptions().get(SERVICE_ACCOUNT))) {
      credentials = ServiceAccountCredentials.fromStream(fis);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    BigQueryConnectionOptions options =
        new BigQueryConnectionOptions(
            helper.getOptions().get(PROJECT_ID),
            helper.getOptions().get(DATASET),
            helper.getOptions().get(TABLE),
            helper.getOptions().get(CREATE_TABLE_IF_NOT_PRESENT),
            helper.getOptions().get(BATCH_SIZE),
            helper.getOptions().get(BATCH_INTERVAL_MS),
            helper.getOptions().get(MAX_RETRIES),
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
