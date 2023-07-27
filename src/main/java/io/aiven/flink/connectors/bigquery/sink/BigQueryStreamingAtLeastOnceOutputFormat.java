package io.aiven.flink.connectors.bigquery.sink;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.FixedExecutorProvider;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Descriptors;
import io.grpc.Status;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import org.apache.flink.table.types.logical.LogicalType;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryStreamingAtLeastOnceOutputFormat extends AbstractBigQueryOutputFormat {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(BigQueryStreamingAtLeastOnceOutputFormat.class);
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

  private final AtomicInteger recreateCount = new AtomicInteger(0);

  protected BigQueryStreamingAtLeastOnceOutputFormat(
      @Nonnull String[] fieldNames,
      @Nonnull LogicalType[] fieldTypes,
      @Nonnull BigQueryConnectionOptions options) {
    super(fieldNames, fieldTypes, options);
  }

  @Override
  protected JsonStreamWriter getStreamWriter(
      BigQueryConnectionOptions options, BigQueryWriteClient client)
      throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
    WriteStream stream = WriteStream.newBuilder().setType(WriteStream.Type.COMMITTED).build();

    CreateWriteStreamRequest createWriteStreamRequest =
        CreateWriteStreamRequest.newBuilder()
            .setParent(options.getTableName().toString())
            .setWriteStream(stream)
            .build();
    WriteStream writeStream = client.createWriteStream(createWriteStreamRequest);

    return JsonStreamWriter.newBuilder(writeStream.getName(), writeStream.getTableSchema(), client)
        .setExecutorProvider(FixedExecutorProvider.create(Executors.newScheduledThreadPool(100)))
        .setChannelProvider(
            BigQueryWriteSettings.defaultGrpcTransportProviderBuilder()
                .setKeepAliveTime(org.threeten.bp.Duration.ofMinutes(1))
                .setKeepAliveTimeout(org.threeten.bp.Duration.ofMinutes(1))
                .setKeepAliveWithoutCalls(true)
                // .setChannelsPerCpu(2)
                .build())
        .build();
  }

  @Override
  protected void append(JSONArray arr)
      throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
    append(new AppendContext(arr, 0));
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

  private static class AppendContext {

    JSONArray data;
    int retryCount = 0;

    AppendContext(JSONArray data, int retryCount) {
      this.data = data;
      this.retryCount = retryCount;
    }
  }

  static class AppendCompleteCallback implements ApiFutureCallback<AppendRowsResponse> {

    private final BigQueryStreamingAtLeastOnceOutputFormat parent;
    private final AppendContext appendContext;

    public AppendCompleteCallback(
        BigQueryStreamingAtLeastOnceOutputFormat parent, AppendContext appendContext) {
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
          LOGGER.error("Failed to retry append: ", e);
          // we should throw exception only when all attempts are used
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
