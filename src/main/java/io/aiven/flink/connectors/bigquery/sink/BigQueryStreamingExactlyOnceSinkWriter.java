package io.aiven.flink.connectors.bigquery.sink;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Descriptors;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Phaser;
import javax.annotation.Nonnull;
import org.apache.flink.table.types.logical.LogicalType;
import org.json.JSONArray;

public class BigQueryStreamingExactlyOnceSinkWriter extends BigQueryWriter {
  private long offset = 0L;

  protected BigQueryStreamingExactlyOnceSinkWriter(
      @Nonnull String[] fieldNames,
      @Nonnull LogicalType[] fieldTypes,
      @Nonnull BigQueryConnectionOptions options) {
    super(fieldNames, fieldTypes, options);
    inflightRequestCount = new Phaser(1);
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
        JsonStreamWriter.newBuilder(writeStream.getName(), writeStream.getTableSchema(), client)
            .setFlowControlSettings(
                FlowControlSettings.newBuilder()
                    .setMaxOutstandingElementCount(options.getMaxOutstandingElementsCount())
                    .setMaxOutstandingRequestBytes(options.getMaxOutstandingRequestBytes())
                    .build());
    return builder.build();
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
        future,
        new AppendCompleteCallback(this, new AppendContext(data)),
        MoreExecutors.directExecutor());
    // Increase the count of in-flight requests.
    inflightRequestCount.register();
  }
}
