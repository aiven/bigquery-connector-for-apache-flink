package io.aiven.flink.connectors.bigquery.sink;

import static io.grpc.Status.Code.ALREADY_EXISTS;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.time.format.DateTimeFormatter.ISO_TIME;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Descriptors;
import io.grpc.Status;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import org.apache.flink.api.connector.sink2.SinkWriter;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

/** Abstract class of BigQuery output format. */
public abstract class BigQueryWriter implements SinkWriter<RowData> {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryWriter.class);
  private static final ImmutableList<Status.Code> RETRIABLE_ERROR_CODES =
      ImmutableList.of(
          Status.Code.INTERNAL,
          Status.Code.ABORTED,
          Status.Code.CANCELLED,
          Status.Code.FAILED_PRECONDITION,
          Status.Code.DEADLINE_EXCEEDED,
          Status.Code.UNAVAILABLE);
  protected static final int MAX_RECREATE_COUNT = 3;
  protected final String[] fieldNames;

  protected final LogicalType[] fieldTypes;

  protected final BigQueryConnectionOptions options;
  protected transient BigQueryWriteClient client;

  protected transient JsonStreamWriter streamWriter;

  // Track the number of in-flight requests to wait for all responses before shutting down.
  protected transient Phaser inflightRequestCount;
  protected final AtomicInteger recreateCount = new AtomicInteger(0);

  protected final Serializable lock = new Serializable() {};

  @GuardedBy("lock")
  protected RuntimeException error = null;

  public BigQueryWriter(
      @Nonnull String[] fieldNames,
      @Nonnull LogicalType[] fieldTypes,
      @Nonnull BigQueryConnectionOptions options) {
    this.fieldNames = Preconditions.checkNotNull(fieldNames);
    this.fieldTypes = Preconditions.checkNotNull(fieldTypes);
    this.options = Preconditions.checkNotNull(options);
    FixedCredentialsProvider creds = FixedCredentialsProvider.create(options.getCredentials());
    inflightRequestCount = new Phaser(1);
    BigQueryWriteSettings.Builder bigQueryWriteSettingsBuilder = BigQueryWriteSettings.newBuilder();
    bigQueryWriteSettingsBuilder
        .createWriteStreamSettings()
        .setRetrySettings(
            bigQueryWriteSettingsBuilder.createWriteStreamSettings().getRetrySettings().toBuilder()
                .setTotalTimeout(Duration.ofSeconds(30))
                .build());
    try {
      BigQueryWriteSettings bigQueryWriteSettings =
          bigQueryWriteSettingsBuilder.setCredentialsProvider(creds).build();
      client = BigQueryWriteClient.create(bigQueryWriteSettings);
      streamWriter = getStreamWriter(options, client);
    } catch (IOException | Descriptors.DescriptorValidationException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void flush(boolean endOfInput) throws IOException, InterruptedException {}

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
  public void write(RowData record, Context context) throws IOException {
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

    public BigQueryWriter build() {
      Preconditions.checkNotNull(fieldNames);
      Preconditions.checkNotNull(fieldDataTypes);
      LogicalType[] logicalTypes =
          Arrays.stream(fieldDataTypes).map(DataType::getLogicalType).toArray(LogicalType[]::new);
      if (options.getDeliveryGuarantee() == DeliveryGuarantee.AT_LEAST_ONCE) {
        return new BigQueryStreamingAtLeastOnceSinkWriter(fieldNames, logicalTypes, options);
      }
      return new BigQueryStreamingExactlyOnceSinkWriter(fieldNames, logicalTypes, options);
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

  protected void append(AppendContext appendContext)
      throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
    synchronized (this.lock) {
      if (!streamWriter.isUserClosed()
          && streamWriter.isClosed()
          && recreateCount.getAndIncrement() < options.getRecreateCount()) {
        streamWriter =
            JsonStreamWriter.newBuilder(streamWriter.getStreamName(), BigQueryWriteClient.create())
                .setFlowControlSettings(
                    FlowControlSettings.newBuilder()
                        .setMaxOutstandingElementCount(options.getMaxOutstandingElementsCount())
                        .setMaxOutstandingRequestBytes(options.getMaxOutstandingRequestBytes())
                        .build())
                .build();
        this.error = null;
      }
      // If earlier appends have failed, we need to reset before continuing.
      if (this.error != null) {
        throw new IOException(this.error);
      }
    }
    // Append asynchronously for increased throughput.
    ApiFuture<AppendRowsResponse> future = streamWriter.append(appendContext.data);
    ApiFutures.addCallback(
        future, new AppendCompleteCallback(this, appendContext), MoreExecutors.directExecutor());

    // Increase the count of in-flight requests.
    inflightRequestCount.register();
  }

  protected static class AppendContext {

    private final JSONArray data;
    private int retryCount = 0;

    AppendContext(JSONArray data) {
      this.data = data;
    }
  }

  static class AppendCompleteCallback implements ApiFutureCallback<AppendRowsResponse> {

    private final BigQueryWriter parent;
    private final AppendContext appendContext;

    public AppendCompleteCallback(BigQueryWriter parent, AppendContext appendContext) {
      this.parent = parent;
      this.appendContext = appendContext;
    }

    @Override
    public void onSuccess(AppendRowsResponse response) {
      this.parent.recreateCount.set(0);
      done();
    }

    @Override
    public void onFailure(Throwable throwable) {
      // If the wrapped exception is a StatusRuntimeException, check the state of the operation.
      // If the state is INTERNAL, CANCELLED, or ABORTED, you can retry. For more information,
      // see: https://grpc.github.io/grpc-java/javadoc/io/grpc/StatusRuntimeException.html
      Status status = Status.fromThrowable(throwable);
      if (appendContext.retryCount < this.parent.options.getRetryCount()
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
          LOG.error("Failed to retry append: ", e);
          // we should throw exception only when all attempts are used
          e.addSuppressed(throwable);
          throwable = e;
        }
      }
      if (status.getCode() == ALREADY_EXISTS) {
        LOG.info("Message for this offset already exists");
        done();
        return;
      }

      if (throwable instanceof Exceptions.AppendSerializationError) {
        Exceptions.AppendSerializationError ase = (Exceptions.AppendSerializationError) throwable;
        Map<Integer, String> rowIndexToErrorMessage = ase.getRowIndexToErrorMessage();
        if (!rowIndexToErrorMessage.isEmpty()) {
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
          if (!dataNew.isEmpty()) {
            try {
              this.parent.append(new AppendContext(dataNew));
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
