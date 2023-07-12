package io.aiven.flink.connectors.bigquery.sink;

import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

public class BigQuerySink implements DynamicTableSink {
  private static final Map<LogicalTypeRoot, StandardSQLTypeName> DATATYPE_2_BIGQUERY_TYPE =
      new EnumMap<>(LogicalTypeRoot.class);

  static {
    DATATYPE_2_BIGQUERY_TYPE.put(LogicalTypeRoot.VARCHAR, StandardSQLTypeName.STRING);
    DATATYPE_2_BIGQUERY_TYPE.put(LogicalTypeRoot.INTEGER, StandardSQLTypeName.INT64);
    DATATYPE_2_BIGQUERY_TYPE.put(LogicalTypeRoot.TINYINT, StandardSQLTypeName.INT64);
    DATATYPE_2_BIGQUERY_TYPE.put(LogicalTypeRoot.SMALLINT, StandardSQLTypeName.INT64);
    DATATYPE_2_BIGQUERY_TYPE.put(LogicalTypeRoot.BIGINT, StandardSQLTypeName.INT64);
    DATATYPE_2_BIGQUERY_TYPE.put(LogicalTypeRoot.FLOAT, StandardSQLTypeName.FLOAT64);
    DATATYPE_2_BIGQUERY_TYPE.put(LogicalTypeRoot.DOUBLE, StandardSQLTypeName.FLOAT64);
    DATATYPE_2_BIGQUERY_TYPE.put(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE, StandardSQLTypeName.TIME);
    DATATYPE_2_BIGQUERY_TYPE.put(
        LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE, StandardSQLTypeName.TIMESTAMP);
    DATATYPE_2_BIGQUERY_TYPE.put(
        LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, StandardSQLTypeName.TIMESTAMP);
    DATATYPE_2_BIGQUERY_TYPE.put(
        LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE, StandardSQLTypeName.TIMESTAMP);
    DATATYPE_2_BIGQUERY_TYPE.put(LogicalTypeRoot.DATE, StandardSQLTypeName.DATE);
    DATATYPE_2_BIGQUERY_TYPE.put(LogicalTypeRoot.BOOLEAN, StandardSQLTypeName.BOOL);
    DATATYPE_2_BIGQUERY_TYPE.put(LogicalTypeRoot.DECIMAL, StandardSQLTypeName.NUMERIC);
    DATATYPE_2_BIGQUERY_TYPE.put(LogicalTypeRoot.ROW, StandardSQLTypeName.STRUCT);
  }

  private static final Set<LogicalTypeRoot> TYPES_WITH_PRECISION =
      EnumSet.of(LogicalTypeRoot.DECIMAL);

  private static final Set<LogicalTypeRoot> TYPES_WITH_SCALE = EnumSet.of(LogicalTypeRoot.DECIMAL);

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

    if (options.isCreateIfNotExists()) {
      ensureTableExists(fieldNames, fieldTypes, options);
    }
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

  @VisibleForTesting
  static Table ensureTableExists(
      String[] fieldNames, DataType[] types, BigQueryConnectionOptions options) {
    var bigQuery =
        BigQueryOptions.newBuilder()
            .setProjectId(options.getTableName().getProject())
            .setCredentials(options.getCredentials())
            .build()
            .getService();
    var dataset = options.getTableName().getDataset();
    if (bigQuery.getDataset(dataset) == null || !bigQuery.getDataset(dataset).exists()) {
      bigQuery.create(DatasetInfo.newBuilder(dataset).build());
    }
    var tableId =
        TableId.of(
            options.getTableName().getProject(),
            options.getTableName().getDataset(),
            options.getTableName().getTable());
    Table table = bigQuery.getTable(tableId);
    if (table == null || !table.exists()) {
      return bigQuery.create(
          TableInfo.of(
              tableId,
              StandardTableDefinition.newBuilder()
                  .setSchema(schemaBuilder(fieldNames, types))
                  .build()));
    }
    return table;
  }

  private static Schema schemaBuilder(String[] fieldNames, DataType[] types) {
    // ARRAY of ARRAYs is not allowed based on Google BigQuery doc
    // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#constructing_an_array
    Collection<Field> fields = new ArrayList<>();
    for (int i = 0; i < fieldNames.length; i++) {
      LogicalType logicalType = types[i].getLogicalType();
      fields.add(buildField(fieldNames[i], logicalType));
    }
    return Schema.of(fields);
  }

  private static FieldList buildFieldList(LogicalType parentLogicalType) {
    Collection<Field> fields = new ArrayList<>();
    if (parentLogicalType.is(LogicalTypeRoot.ARRAY)) {
      parentLogicalType = parentLogicalType.getChildren().get(0);
    }
    List<LogicalType> logicalTypes = LogicalTypeChecks.getFieldTypes(parentLogicalType);
    List<String> names = LogicalTypeChecks.getFieldNames(parentLogicalType);
    for (int i = 0; i < LogicalTypeChecks.getFieldCount(parentLogicalType); i++) {
      LogicalType childLogicalType = logicalTypes.get(i);
      fields.add(buildField(names.get(i), childLogicalType));
    }
    return FieldList.of(fields);
  }

  private static Field buildField(String fieldName, LogicalType logicalType) {
    StandardSQLTypeName standardSQLTypeName =
        DATATYPE_2_BIGQUERY_TYPE.get(
            logicalType.is(LogicalTypeRoot.ARRAY)
                ? logicalType.getChildren().get(0).getTypeRoot()
                : logicalType.getTypeRoot());
    if (standardSQLTypeName == null) {
      throw new ValidationException("Type " + logicalType + " is not supported");
    }
    Field.Builder fBuilder;
    if (logicalType.is(LogicalTypeRoot.ROW)
        || (logicalType.is(LogicalTypeRoot.ARRAY)
            && logicalType.getChildren().get(0).is(LogicalTypeRoot.ROW))) {
      fBuilder = Field.newBuilder(fieldName, standardSQLTypeName, buildFieldList(logicalType));
    } else {
      fBuilder = Field.newBuilder(fieldName, standardSQLTypeName);
    }
    fBuilder.setMode(logicalType.isNullable() ? Field.Mode.NULLABLE : Field.Mode.REQUIRED);
    if (logicalType.is(LogicalTypeRoot.ARRAY)) {
      fBuilder.setMode(Field.Mode.REPEATED);
    }
    if (TYPES_WITH_PRECISION.contains(logicalType.getTypeRoot())) {
      fBuilder.setPrecision((long) LogicalTypeChecks.getPrecision(logicalType));
    }
    if (TYPES_WITH_SCALE.contains(logicalType.getTypeRoot())) {
      fBuilder.setScale((long) LogicalTypeChecks.getScale(logicalType));
    }
    return fBuilder.build();
  }
}
