package io.aiven.flink.connectors.bigquery.sink;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.time.format.DateTimeFormatter.ISO_TIME;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.TableId;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.Preconditions;

public class BigQueryStreamingOutputFormat extends AbstractBigQueryOutputFormat {

  private static final long serialVersionUID = 1L;

  private final String[] fieldNames;

  private final LogicalType[] fieldTypes;

  private final BigQueryConnectionOptions options;
  private BigQuery bigQuery;

  protected BigQueryStreamingOutputFormat(
      @Nonnull String[] fieldNames,
      @Nonnull LogicalType[] fieldTypes,
      @Nonnull BigQueryConnectionOptions options) {
    this.fieldNames = Preconditions.checkNotNull(fieldNames);
    this.fieldTypes = Preconditions.checkNotNull(fieldTypes);
    this.options = Preconditions.checkNotNull(options);
  }

  @Override
  public void open(int taskNumber, int numTasks) throws IOException {
    if (bigQuery == null) {
      bigQuery =
          BigQueryOptions.newBuilder()
              .setProjectId(options.getTableId().getProject())
              .setCredentials(options.getCredentials())
              .build()
              .getService();
    }
  }

  @Override
  public void writeRecord(RowData record) throws IOException {
    try {
      Map<String, Object> rowContent = new HashMap<>();
      final int arity = record.getArity();
      for (int i = 0; i < arity; i++) {
        final Object value = retrieveValue(record, fieldTypes[i], i);
        rowContent.put(fieldNames[i], value);
      }
      bigQuery.getTable(TableId.of("aiven-opensource-sandbox", "TestDataSet", "strTest"));
      InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(options.getTableId());
      builder.addRow(rowContent);
      Map<Long, List<BigQueryError>> insertErrors =
          bigQuery.insertAll(builder.build()).getInsertErrors();
      if (insertErrors.size() > 0) {
        throw new RuntimeException(insertErrors.values().toString());
      }
    } catch (BigQueryException e) {
      throw new RuntimeException(e);
    }
  }

  private Object retrieveValue(RowData record, LogicalType type, int i) {
    if (record.isNullAt(i)) {
      return null;
    }
    switch (type.getTypeRoot()) {
      case NULL:
        return null;
      case BIGINT:
        return record.getLong(i);
      case BOOLEAN:
        return record.getBoolean(i);
      case DOUBLE:
        return record.getDouble(i);
      case DECIMAL:
        return record
            .getDecimal(i, LogicalTypeChecks.getPrecision(type), LogicalTypeChecks.getScale(type))
            .toBigDecimal();
      case FLOAT:
        return record.getFloat(i);
      case INTEGER:
        return record.getInt(i);
      case CHAR:
      case VARCHAR:
        return record.getString(i).toString();
      case DATE:
        return LocalDate.ofEpochDay(record.getInt(i)).format(ISO_LOCAL_DATE);
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return record
            .getTimestamp(i, LogicalTypeChecks.getPrecision(type))
            .toLocalDateTime()
            .format(ISO_OFFSET_DATE_TIME);
      case TIME_WITHOUT_TIME_ZONE:
        int millis = record.getInt(i);
        return LocalTime.of(
                millis / 60 / 60 / 1000, (millis / 1000 / 60) % 60, (millis / 1000) % 60)
            .format(ISO_TIME);
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return record
            .getTimestamp(i, LogicalTypeChecks.getPrecision(type))
            .toLocalDateTime()
            .format(ISO_LOCAL_DATE_TIME);
      case ARRAY:
        return retrieveFromArray(record.getArray(i), (ArrayType) type);
      case ROW:
        return retrieveFromRow(record.getRow(i, ((RowType) type).getFieldCount()), (RowType) type);
      default:
        throw new RuntimeException(type.getTypeRoot() + " is not supported");
    }
  }

  private Map<String, Object> retrieveFromRow(RowData rowData, RowType type) {
    final Map<String, Object> map = new HashMap<>(rowData.getArity());
    final List<RowType.RowField> fields = type.getFields();
    for (int i = 0; i < fields.size(); i++) {
      map.put(fields.get(i).getName(), retrieveValue(rowData, fields.get(i).getType(), i));
    }
    return map;
  }

  private Object retrieveFromArray(ArrayData arrayData, ArrayType arrayType) {
    final LogicalType type;
    final Object[] value;
    switch (arrayType.getElementType().getTypeRoot()) {
      case BOOLEAN:
        return arrayData.toBooleanArray();
      case INTEGER:
        return arrayData.toIntArray();
      case SMALLINT:
        return arrayData.toShortArray();
      case BIGINT:
        return arrayData.toLongArray();
      case FLOAT:
        return arrayData.toFloatArray();
      case DOUBLE:
        return arrayData.toDoubleArray();
      case DECIMAL:
        type = arrayType.getElementType();
        value = new BigDecimal[arrayData.size()];
        for (int i = 0; i < value.length; i++) {
          value[i] =
              arrayData.isNullAt(i)
                  ? null
                  : arrayData
                      .getDecimal(
                          i, LogicalTypeChecks.getPrecision(type), LogicalTypeChecks.getScale(type))
                      .toBigDecimal();
        }
        return value;
      case DATE:
        type = arrayType.getElementType();
        value = new String[arrayData.size()];
        for (int i = 0; i < value.length; i++) {
          value[i] =
              arrayData.isNullAt(i)
                  ? null
                  : arrayData
                      .getTimestamp(i, LogicalTypeChecks.getPrecision(type))
                      .toLocalDateTime()
                      .format(ISO_LOCAL_DATE);
        }
        return value;
      case TIME_WITHOUT_TIME_ZONE:
        value = new String[arrayData.size()];
        for (int i = 0; i < value.length; i++) {
          int millis = arrayData.getInt(i);
          value[i] =
              arrayData.isNullAt(i)
                  ? null
                  : LocalTime.of(
                          millis / 60 / 60 / 1000, (millis / 1000 / 60) % 60, (millis / 1000) % 60)
                      .format(ISO_TIME);
        }
        return value;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        type = arrayType.getElementType();
        value = new String[arrayData.size()];
        for (int i = 0; i < value.length; i++) {
          value[i] =
              arrayData.isNullAt(i)
                  ? null
                  : arrayData
                      .getTimestamp(i, LogicalTypeChecks.getPrecision(type))
                      .toLocalDateTime()
                      .format(ISO_OFFSET_DATE_TIME);
        }
        return value;
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        type = arrayType.getElementType();
        value = new String[arrayData.size()];
        for (int i = 0; i < value.length; i++) {
          value[i] =
              arrayData.isNullAt(i)
                  ? null
                  : arrayData
                      .getTimestamp(i, LogicalTypeChecks.getPrecision(type))
                      .toLocalDateTime()
                      .format(ISO_LOCAL_DATE_TIME);
        }
        return value;
      case CHAR:
      case VARCHAR:
        value = new String[arrayData.size()];
        for (int i = 0; i < value.length; i++) {
          value[i] = arrayData.isNullAt(i) ? null : arrayData.getString(i).toString();
        }
        return value;
        /*
        CURRENTLY BIG_QUERY DOES NOT SUPPORT NESTED ARRAYS
        case ARRAY:
        List<LogicalType> children = arrayType.getChildren();
        Object[] result = new Object[arrayData.size()];
        for (int i = 0; i < arrayData.size(); i++) {
          result[i] = retrieveFromArray(arrayData.getArray(i), (ArrayType) arrayType.getElementType());
        }
        return result;
        */
      default:
        throw new RuntimeException(arrayType.getElementType().getTypeRoot() + " is not supported");
    }
  }
}
