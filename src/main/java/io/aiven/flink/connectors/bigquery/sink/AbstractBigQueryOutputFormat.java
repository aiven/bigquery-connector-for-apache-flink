package io.aiven.flink.connectors.bigquery.sink;

import java.util.Arrays;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Abstract class of BigQuery output format. */
public abstract class AbstractBigQueryOutputFormat extends RichOutputFormat<RowData> {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(AbstractBigQueryOutputFormat.class);

  public AbstractBigQueryOutputFormat() {}

  @Override
  public void configure(Configuration parameters) {}

  @Override
  public synchronized void close() {}

  public static class Builder {

    private static final Logger LOG = LoggerFactory.getLogger(Builder.class);

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
      return createBatchOutputFormat(logicalTypes);
    }

    private BigQueryStreamingOutputFormat createBatchOutputFormat(LogicalType[] logicalTypes) {
      return new BigQueryStreamingOutputFormat(fieldNames, logicalTypes, options);
    }
  }
}
