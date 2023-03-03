package io.aiven.flink.connectors.bigquery.sink;

import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.table.types.DataType;

public class BigQuerySink implements DynamicTableSink {
  private final CatalogTable catalogTable;
  private final ResolvedSchema tableSchema;
  private final BigQueryConnectionOptions options;

  public BigQuerySink(
      CatalogTable catalogTable, ResolvedSchema tableSchema, BigQueryConnectionOptions options) {
    this.catalogTable = catalogTable;
    this.tableSchema = tableSchema;
    this.options = options;
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
    return ChangelogMode.insertOnly();
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {

    String[] fieldNames =
        tableSchema.getColumns().stream()
            .filter(Column::isPhysical)
            .map(Column::getName)
            .toArray(String[]::new);
    DataType[] fieldTypes =
        tableSchema.getColumns().stream()
            .filter(Column::isPhysical)
            .map(Column::getDataType)
            .toArray(DataType[]::new);

    AbstractBigQueryOutputFormat outputFormat =
        new AbstractBigQueryOutputFormat.Builder()
            .withFieldNames(fieldNames)
            .withFieldDataTypes(fieldTypes)
            .withOptions(options)
            .build();
    return OutputFormatProvider.of(outputFormat);
  }

  @Override
  public DynamicTableSink copy() {
    return new BigQuerySink(catalogTable, tableSchema, options);
  }

  @Override
  public String asSummaryString() {
    return "BigQuery sink";
  }
}
