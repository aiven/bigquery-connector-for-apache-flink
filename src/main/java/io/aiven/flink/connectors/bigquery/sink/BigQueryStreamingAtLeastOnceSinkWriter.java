package io.aiven.flink.connectors.bigquery.sink;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.FixedExecutorProvider;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.protobuf.Descriptors;
import java.io.IOException;
import java.util.concurrent.Executors;
import javax.annotation.Nonnull;
import org.apache.flink.table.types.logical.LogicalType;
import org.json.JSONArray;

public class BigQueryStreamingAtLeastOnceSinkWriter extends BigQueryWriter {

  protected BigQueryStreamingAtLeastOnceSinkWriter(
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

    JsonStreamWriter.Builder builder =
        JsonStreamWriter.newBuilder(writeStream.getName(), writeStream.getTableSchema(), client)
            .setFlowControlSettings(
                FlowControlSettings.newBuilder()
                    .setMaxOutstandingElementCount(options.getMaxOutstandingElementsCount())
                    .setMaxOutstandingRequestBytes(options.getMaxOutstandingRequestBytes())
                    .build());
    return builder
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
    append(new AppendContext(arr));
  }
}
