package io.aiven.flink.connectors.bigquery.sink;

import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.storage.v1.TableName;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

public class BigQueryDynamicTableSink implements DynamicTableSink {
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
    DATATYPE_2_BIGQUERY_TYPE.put(LogicalTypeRoot.CHAR, StandardSQLTypeName.GEOGRAPHY);
  }

  private static final Set<LogicalTypeRoot> TYPES_WITH_PRECISION =
      EnumSet.of(LogicalTypeRoot.DECIMAL);

  private static final Set<LogicalTypeRoot> TYPES_WITH_SCALE = EnumSet.of(LogicalTypeRoot.DECIMAL);

  private final CatalogTable catalogTable;
  private final ResolvedSchema tableSchema;
  private final BigQueryConnectionOptions options;
  private final DataType expectedDatatype;

  public BigQueryDynamicTableSink(
      CatalogTable catalogTable,
      ResolvedSchema tableSchema,
      DataType expectedDatatype,
      BigQueryConnectionOptions options) {
    this.catalogTable = catalogTable;
    this.tableSchema = tableSchema;
    this.expectedDatatype = expectedDatatype;
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

    LogicalType[] logicalTypes =
        Arrays.stream(fieldTypes).map(DataType::getLogicalType).toArray(LogicalType[]::new);
    return SinkV2Provider.of(new BigQuerySink(fieldNames, logicalTypes, options));
  }

  @Override
  public DynamicTableSink copy() {
    return new BigQueryDynamicTableSink(catalogTable, tableSchema, expectedDatatype, options);
  }

  @Override
  public String asSummaryString() {
    return "BigQuery sink";
  }

  @VisibleForTesting
  static Table ensureTableExists(
      String[] fieldNames, DataType[] types, BigQueryConnectionOptions options) {
    TableName tableName = options.getTableName();
    var bigQueryService =
        BigQueryOptions.newBuilder()
            .setProjectId(tableName.getProject())
            .setCredentials(options.getCredentials())
            .build()
            .getService();
    var dataset = tableName.getDataset();
    if (bigQueryService.getDataset(dataset) == null
        || !bigQueryService.getDataset(dataset).exists()) {
      bigQueryService.create(DatasetInfo.newBuilder(dataset).build());
    }
    var tableId = TableId.of(tableName.getProject(), tableName.getDataset(), tableName.getTable());
    Table table = bigQueryService.getTable(tableId);
    StandardTableDefinition requiredDefinition =
        StandardTableDefinition.newBuilder().setSchema(schemaBuilder(fieldNames, types)).build();
    if (table == null || !table.exists()) {
      return bigQueryService.create(TableInfo.of(tableId, requiredDefinition));
    } else {
      TableDefinition existingDefinition = table.getDefinition();
      FieldList existingFieldList = existingDefinition.getSchema().getFields();
      FieldList fieldList = requiredDefinition.getSchema().getFields();
      validateTableDefinitions(existingFieldList, fieldList, null);
    }
    return table;
  }

  private static void validateTableDefinitions(
      FieldList existingFieldList, FieldList fieldList, String parentTypeName) {
    if (existingFieldList.size() < fieldList.size()) {
      throw new ValidationException(
          "Number of columns in BQ table ("
              + existingFieldList.size()
              + ") should be not less than a number of columns in corresponding Flink table ("
              + fieldList.size()
              + ")");
    }
    int fieldIndex = 0;
    final String parentName = (parentTypeName == null ? "" : parentTypeName + ".");
    for (int i = 0; i < existingFieldList.size(); i++) {
      final Field existingField = existingFieldList.get(i);
      Field.Mode existingFieldMode = existingField.getMode();
      if (fieldIndex >= fieldList.size()) {
        if (existingFieldMode != Field.Mode.NULLABLE) {
          throw new ValidationException(
              "Column #"
                  + (i + 1)
                  + " with name '"
                  + parentName
                  + existingField.getName()
                  + "' in BQ is required however is absent in Flink table");
        }
        continue;
      }
      final Field fieldFromInsert = fieldList.get(fieldIndex);

      if (existingFieldMode == Field.Mode.NULLABLE
          && !existingField.getName().equals(fieldFromInsert.getName())) {
        continue;
      }
      if (!fieldFromInsert.getName().equals(existingField.getName())) {
        throw new ValidationException(
            "Column #"
                + (i + 1)
                + " has name '"
                + parentName
                + existingField.getName()
                + "' in BQ while in Flink it has name '"
                + parentName
                + fieldFromInsert.getName()
                + "'");
      }
      if (!fieldFromInsert
          .getType()
          .getStandardType()
          .equals(existingField.getType().getStandardType())) {
        throw new ValidationException(
            "Column #"
                + (i + 1)
                + " with name '"
                + parentName
                + existingField.getName()
                + "' has type '"
                + existingField.getType().getStandardType()
                + "' in BQ while in Flink it has type '"
                + fieldFromInsert.getType().getStandardType()
                + "'");
      }
      if (existingFieldMode != Field.Mode.NULLABLE
          && fieldFromInsert.getMode() == Field.Mode.NULLABLE) {
        if (!parentName.isBlank()) {
          // currently this is a bug in Calcite/Flink
          // so this validation will be done at runtime on BigQuery side
          fieldIndex++;
          continue;
        }
        throw new ValidationException(
            "Column #"
                + (i + 1)
                + " with name '"
                + parentName
                + existingField.getName()
                + "' is not nullable '"
                + existingField.getType().getStandardType()
                + "' in BQ while in Flink it is nullable '"
                + fieldFromInsert.getType().getStandardType()
                + "'");
      }
      if (LegacySQLTypeName.RECORD.equals(existingField.getType())) {
        validateTableDefinitions(
            existingField.getSubFields(), fieldFromInsert.getSubFields(), existingField.getName());
      }
      fieldIndex++;
    }
    if (fieldIndex != fieldList.size()) {
      throw new ValidationException(
          "There are unknown columns starting with #"
              + (fieldIndex + 1)
              + " with name '"
              + parentName
              + fieldList.get(fieldIndex).getName()
              + "'");
    }
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
    if (logicalType.is(LogicalTypeRoot.ARRAY) && logicalType.getChildren().get(0).isNullable()) {
      throw new ValidationException(
          "Type "
              + logicalType
              + " is not supported (nullable elements of array are not supported by BQ)");
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
