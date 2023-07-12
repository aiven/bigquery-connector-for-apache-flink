package io.aiven.flink.connectors.bigquery.sink;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.time.format.DateTimeFormatter.ISO_TIME;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.FixedExecutorProvider;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Descriptors;
import io.grpc.Status;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.Preconditions;
import org.json.JSONArray;
import org.json.JSONObject;

public class BigQueryStreamingOutputFormat extends AbstractBigQueryOutputFormat {

  private static final long serialVersionUID = 1L;

  private static final int MAX_RETRY_COUNT = 3;
  private static final int MAX_RECREATE_COUNT = 3;
  private static final ImmutableList<Status.Code> RETRIABLE_ERROR_CODES =
      ImmutableList.of(
          Status.Code.INTERNAL,
          Status.Code.ABORTED,
          Status.Code.CANCELLED,
          Status.Code.FAILED_PRECONDITION,
          Status.Code.DEADLINE_EXCEEDED,
          Status.Code.UNAVAILABLE);

  // Track the number of in-flight requests to wait for all responses before shutting down.
  private transient Phaser inflightRequestCount;
  private final Serializable lock = new Serializable() {};
  private transient JsonStreamWriter streamWriter;

  @GuardedBy("lock")
  private RuntimeException error = null;

  private final AtomicInteger recreateCount = new AtomicInteger(0);

  private final String[] fieldNames;

  private final LogicalType[] fieldTypes;

  private final BigQueryConnectionOptions options;

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
    try {
      FixedCredentialsProvider creds = FixedCredentialsProvider.create(options.getCredentials());
      inflightRequestCount = new Phaser(1);
      streamWriter =
          JsonStreamWriter.newBuilder(
                  options.getTableName().toString(),
                  BigQueryWriteClient.create(
                      BigQueryWriteSettings.newBuilder().setCredentialsProvider(creds).build()))
              .setCredentialsProvider(creds)
              .setExecutorProvider(
                  FixedExecutorProvider.create(Executors.newScheduledThreadPool(100)))
              .setChannelProvider(
                  BigQueryWriteSettings.defaultGrpcTransportProviderBuilder()
                      .setKeepAliveTime(org.threeten.bp.Duration.ofMinutes(1))
                      .setKeepAliveTimeout(org.threeten.bp.Duration.ofMinutes(1))
                      .setKeepAliveWithoutCalls(true)
                      // .setChannelsPerCpu(2)
                      .build())
              .build();

    } catch (Descriptors.DescriptorValidationException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() {
    // Wait for all in-flight requests to complete.
    inflightRequestCount.arriveAndAwaitAdvance();

    // Close the connection to the server.
    streamWriter.close();

    // Verify that no error occurred in the stream.
    synchronized (this.lock) {
      if (this.error != null) {
        throw this.error;
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

      append(new AppendContext(arr, 0));
    } catch (BigQueryException
        | Descriptors.DescriptorValidationException
        | InterruptedException e) {
      throw new IOException(e);
    }
  }

  private void append(AppendContext appendContext)
      throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
    synchronized (this.lock) {
      if (!streamWriter.isUserClosed()
          && streamWriter.isClosed()
          && recreateCount.getAndIncrement() < MAX_RECREATE_COUNT) {
        streamWriter =
            JsonStreamWriter.newBuilder(streamWriter.getStreamName(), BigQueryWriteClient.create())
                .build();
        this.error = null;
      }
      // If earlier appends have failed, we need to reset before continuing.
      if (this.error != null) {
        throw this.error;
      }
    }
    // Append asynchronously for increased throughput.
    ApiFuture<AppendRowsResponse> future = streamWriter.append(appendContext.data);
    ApiFutures.addCallback(
        future, new AppendCompleteCallback(this, appendContext), MoreExecutors.directExecutor());

    // Increase the count of in-flight requests.
    inflightRequestCount.register();
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

  private static class AppendContext {

    JSONArray data;
    int retryCount = 0;

    AppendContext(JSONArray data, int retryCount) {
      this.data = data;
      this.retryCount = retryCount;
    }
  }

  static class AppendCompleteCallback implements ApiFutureCallback<AppendRowsResponse> {

    private final BigQueryStreamingOutputFormat parent;
    private final AppendContext appendContext;

    public AppendCompleteCallback(
        BigQueryStreamingOutputFormat parent, AppendContext appendContext) {
      this.parent = parent;
      this.appendContext = appendContext;
    }

    @Override
    public void onSuccess(AppendRowsResponse response) {
      System.out.format("Append success\n");
      this.parent.recreateCount.set(0);
      done();
    }

    @Override
    public void onFailure(Throwable throwable) {
      // If the wrapped exception is a StatusRuntimeException, check the state of the operation.
      // If the state is INTERNAL, CANCELLED, or ABORTED, you can retry. For more information,
      // see: https://grpc.github.io/grpc-java/javadoc/io/grpc/StatusRuntimeException.html
      Status status = Status.fromThrowable(throwable);
      if (appendContext.retryCount < MAX_RETRY_COUNT
          && RETRIABLE_ERROR_CODES.contains(status.getCode())) {
        appendContext.retryCount++;
        try {
          // Since default stream appends are not ordered, we can simply retry the appends.
          // Retrying with exclusive streams requires more careful consideration.
          this.parent.append(appendContext);
          // Mark the existing attempt as done since it's being retried.
          done();
          return;
        } catch (Exception e) {
          // Fall through to return error.
          System.out.format("Failed to retry append: %s\n", e);
        }
      }

      if (throwable instanceof Exceptions.AppendSerializationError) {
        Exceptions.AppendSerializationError ase = (Exceptions.AppendSerializationError) throwable;
        Map<Integer, String> rowIndexToErrorMessage = ase.getRowIndexToErrorMessage();
        if (rowIndexToErrorMessage.size() > 0) {
          // Omit the faulty rows
          JSONArray dataNew = new JSONArray();
          for (int i = 0; i < appendContext.data.length(); i++) {
            if (!rowIndexToErrorMessage.containsKey(i)) {
              dataNew.put(appendContext.data.get(i));
            } else {
              // process faulty rows by placing them on a dead-letter-queue, for instance
            }
          }

          // Retry the remaining valid rows, but using a separate thread to
          // avoid potentially blocking while we are in a callback.
          if (dataNew.length() > 0) {
            try {
              this.parent.append(new AppendContext(dataNew, 0));
            } catch (Descriptors.DescriptorValidationException
                | IOException
                | InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
          // Mark the existing attempt as done since we got a response for it
          done();
          return;
        }
      }

      synchronized (this.parent.lock) {
        if (this.parent.error == null) {
          Exceptions.StorageException storageException = Exceptions.toStorageException(throwable);
          this.parent.error =
              (storageException != null) ? storageException : new RuntimeException(throwable);
        }
      }
      done();
    }

    private void done() {
      // Reduce the count of in-flight requests.
      this.parent.inflightRequestCount.arriveAndDeregister();
    }
  }
}
