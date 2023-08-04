package io.aiven.flink.connectors.bigquery.sink;

import java.io.IOException;
import javax.annotation.Nonnull;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

@PublicEvolving
public class BigQuerySink implements Sink<RowData> {
  protected BigQueryConnectionOptions options;
  protected final String[] fieldNames;

  protected final LogicalType[] fieldTypes;

  public BigQuerySink(
      String[] fieldNames, @Nonnull LogicalType[] fieldTypes, BigQueryConnectionOptions options) {
    this.fieldNames = Preconditions.checkNotNull(fieldNames);
    this.fieldTypes = Preconditions.checkNotNull(fieldTypes);
    this.options = options;
  }

  @Override
  public SinkWriter<RowData> createWriter(InitContext context) throws IOException {
    return options.getDeliveryGuarantee() == DeliveryGuarantee.EXACTLY_ONCE
        ? new BigQueryStreamingExactlyOnceSinkWriter(fieldNames, fieldTypes, options)
        : new BigQueryStreamingAtLeastOnceSinkWriter(fieldNames, fieldTypes, options);
  }
}
