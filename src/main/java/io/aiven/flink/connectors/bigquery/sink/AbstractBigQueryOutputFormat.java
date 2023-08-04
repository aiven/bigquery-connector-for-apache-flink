package io.aiven.flink.connectors.bigquery.sink;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.time.format.DateTimeFormatter.ISO_TIME;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.protobuf.Descriptors;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.rmi.RemoteException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Phaser;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.Preconditions;
import org.json.JSONArray;
import org.json.JSONObject;

/** Abstract class of BigQuery output format. */
public abstract class AbstractBigQueryOutputFormat extends RichOutputFormat<RowData> {

  private static final long serialVersionUID = 1L;

  protected final String[] fieldNames;

  protected final LogicalType[] fieldTypes;

  protected final BigQueryConnectionOptions options;
  protected transient BigQueryWriteClient client;

  protected transient JsonStreamWriter streamWriter;

  // Track the number of in-flight requests to wait for all responses before shutting down.
  protected transient Phaser inflightRequestCount;

  protected final Serializable lock = new Serializable() {};

  @GuardedBy("lock")
  protected RuntimeException error = null;

  public AbstractBigQueryOutputFormat(
      @Nonnull String[] fieldNames,
      @Nonnull LogicalType[] fieldTypes,
      @Nonnull BigQueryConnectionOptions options) {
    this.fieldNames = Preconditions.checkNotNull(fieldNames);
    this.fieldTypes = Preconditions.checkNotNull(fieldTypes);
    this.options = Preconditions.checkNotNull(options);
  }

  @Override
  public void configure(Configuration parameters) {}

  @Override
  public void open(int taskNumber, int numTasks) throws IOException {
    try {
      FixedCredentialsProvider creds = FixedCredentialsProvider.create(options.getCredentials());
      inflightRequestCount = new Phaser(1);
      client =
          BigQueryWriteClient.create(
              BigQueryWriteSettings.newBuilder().setCredentialsProvider(creds).build());

      streamWriter = getStreamWriter(options, client);
    } catch (Descriptors.DescriptorValidationException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    // An error could happen before init of inflightRequestCount
    if (inflightRequestCount != null) {
      // Wait for all in-flight requests to complete.
      inflightRequestCount.arriveAndAwaitAdvance();
    }

    // streamWriter could fail to init
    if (streamWriter != null) {
      // Close the connection to the server.
      streamWriter.close();
    }

    // Verify that no error occurred in the stream.
    synchronized (this.lock) {
      if (this.error != null) {
        throw new IOException(this.error);
      }
    }

    if (client != null) {
      try {
        if (streamWriter != null) {
          client.finalizeWriteStream(streamWriter.getStreamName());
        }
      } finally {
        client.close();
      }
    }
  }

  @Override
  public void writeRecord(RowData record) throws IOException {
    try {
      JSONObject rowContent = new JSONObject();
      final int arity = record.getArity();
      for (int i = 0; i < arity; i++) {
        final Object value = retrieveValue(record, fieldTypes[i], i);
        rowContent.put(fieldNames[i], value);
      }

      JSONArray arr = new JSONArray();
      arr.put(rowContent);

      append(arr);
    } catch (BigQueryException
        | Descriptors.DescriptorValidationException
        | InterruptedException
        | ExecutionException e) {
      throw new IOException(e);
    } catch (Exceptions.AppendSerializationError ase) {
      Map<Integer, String> rowIndexToErrorMessage = ase.getRowIndexToErrorMessage();
      if (rowIndexToErrorMessage != null && !rowIndexToErrorMessage.isEmpty()) {
        throw new BigQueryConnectorRuntimeException(rowIndexToErrorMessage.toString(), ase);
      }
      throw ase;
    }
  }

  protected abstract JsonStreamWriter getStreamWriter(
      BigQueryConnectionOptions options, BigQueryWriteClient client)
      throws Descriptors.DescriptorValidationException, IOException, InterruptedException;

  protected abstract void append(JSONArray arr)
      throws Descriptors.DescriptorValidationException, IOException, InterruptedException,
          ExecutionException;

  public static class Builder {

    private DataType[] fieldDataTypes;

    private String[] fieldNames;

    private BigQueryConnectionOptions options;

    public Builder() {}

    public Builder withFieldDataTypes(DataType[] fieldDataTypes) {
      this.fieldDataTypes = fieldDataTypes;
      return this;
    }

    public Builder withFieldNames(String[] fieldNames) {
      this.fieldNames = fieldNames;
      return this;
    }

    public Builder withOptions(BigQueryConnectionOptions options) {
      this.options = options;
      return this;
    }

    public AbstractBigQueryOutputFormat build() {
      Preconditions.checkNotNull(fieldNames);
      Preconditions.checkNotNull(fieldDataTypes);
      LogicalType[] logicalTypes =
          Arrays.stream(fieldDataTypes).map(DataType::getLogicalType).toArray(LogicalType[]::new);
      if (options.getDeliveryGuarantee() == DeliveryGuarantee.AT_LEAST_ONCE) {
        return new BigQueryStreamingAtLeastOnceOutputFormat(fieldNames, logicalTypes, options);
      }
      return new BigQueryStreamingExactlyOnceOutputFormat(fieldNames, logicalTypes, options);
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
            // 6 - max timestamp precision supported by BigQuery
            .getTimestamp(i, Math.min(6, LogicalTypeChecks.getPrecision(type)))
            .toLocalDateTime()
            .format(ISO_OFFSET_DATE_TIME);
      case TIME_WITHOUT_TIME_ZONE:
        int millis = record.getInt(i);
        return LocalTime.of(
                millis / 60 / 60 / 1000, (millis / 1000 / 60) % 60, (millis / 1000) % 60)
            .format(ISO_TIME);
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return record
            .getTimestamp(i, Math.min(6, LogicalTypeChecks.getPrecision(type)))
            .toLocalDateTime()
            .format(ISO_LOCAL_DATE_TIME);
      case ARRAY:
        return new JSONArray(retrieveFromArray(record.getArray(i), (ArrayType) type));
      case ROW:
        return retrieveFromRow(record.getRow(i, ((RowType) type).getFieldCount()), (RowType) type);
      default:
        throw new RuntimeException(type.getTypeRoot() + " is not supported");
    }
  }

  private JSONObject retrieveFromRow(RowData rowData, RowType type) {
    final JSONObject map = new JSONObject();
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
