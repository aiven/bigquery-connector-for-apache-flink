package io.aiven.flink.connectors.bigquery.sink;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Descriptors;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;
import org.apache.flink.table.types.logical.LogicalType;
import org.json.JSONArray;

public class BigQueryStreamingExactlyOnceOutputFormat extends AbstractBigQueryOutputFormat {
  private long offset = 0L;

  protected BigQueryStreamingExactlyOnceOutputFormat(
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

    // Use the JSON stream writer to send records in JSON format.
    // For more information about JsonStreamWriter, see:
    // https://googleapis.dev/java/google-cloud-bigquerystorage/latest/com/google/cloud/bigquery/storage/v1/JsonStreamWriter.html
    JsonStreamWriter.Builder builder =
        JsonStreamWriter.newBuilder(writeStream.getName(), writeStream.getTableSchema(), client);
    if (options.getCompression() == Compression.NO_COMPRESSION) {
      return builder.build();
    }
    return builder.setCompressorName(options.getCompression().getValue()).build();
  }

  @Override
  protected void append(JSONArray arr)
      throws Descriptors.DescriptorValidationException, IOException, ExecutionException {
    append(arr, offset);
    offset += arr.length();
  }

  public void append(JSONArray data, long offset)
      throws Descriptors.DescriptorValidationException, IOException, ExecutionException {
    synchronized (this.lock) {
      // If earlier appends have failed, we need to reset before continuing.
      if (this.error != null) {
        throw this.error;
      }
    }
    // Append asynchronously for increased throughput.
    ApiFuture<AppendRowsResponse> future = streamWriter.append(data, offset);
    ApiFutures.addCallback(
        future, new AppendCompleteCallback(this), MoreExecutors.directExecutor());
    // Increase the count of in-flight requests.
    inflightRequestCount.register();
  }

  static class AppendCompleteCallback implements ApiFutureCallback<AppendRowsResponse> {

    private final BigQueryStreamingExactlyOnceOutputFormat parent;

    public AppendCompleteCallback(BigQueryStreamingExactlyOnceOutputFormat parent) {
      this.parent = parent;
    }

    @Override
    public void onSuccess(AppendRowsResponse response) {
      done();
    }

    @Override
    public void onFailure(Throwable throwable) {
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
