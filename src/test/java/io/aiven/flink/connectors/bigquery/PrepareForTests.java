package io.aiven.flink.connectors.bigquery;

import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

public class PrepareForTests {
    private static final Map<String, String> ENV_PROP_MAP = System.getenv();
    private static final String BIG_QUERY_SERVICE_ACCOUNT =
            ENV_PROP_MAP.get("BIG_QUERY_SERVICE_ACCOUNT");
    private static final String BIG_QUERY_PROJECT_ID = ENV_PROP_MAP.get("BIG_QUERY_PROJECT_ID");

    public static void main(String[] args) {
        final String dataset = "TestDataSet";
        final String table = "test-table";
        Credentials credentials;
        try (FileInputStream fis = new FileInputStream(BIG_QUERY_SERVICE_ACCOUNT)) {
            credentials = ServiceAccountCredentials.fromStream(fis);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        var bigQuery = BigQueryOptions.newBuilder()
                .setProjectId(BIG_QUERY_PROJECT_ID)
                .setCredentials(credentials)
                .build()
                .getService();
        if (bigQuery.getDataset(dataset) == null || !bigQuery.getDataset(dataset).exists()) {
            bigQuery.create(DatasetInfo.newBuilder(dataset).build());
        }
        final TableId tableId = TableId.of(BIG_QUERY_PROJECT_ID, dataset, table);
        if (bigQuery.getTable(tableId) == null || !bigQuery.getTable(tableId).exists()) {
            bigQuery.create(TableInfo.of(tableId, StandardTableDefinition.newBuilder().setSchema(
                    Schema.of(
                            Field.of("string", StandardSQLTypeName.STRING),
                            Field.of("int", StandardSQLTypeName.INT64),
                            Field.of("boolean", StandardSQLTypeName.BOOL),
                            Field.of("float", StandardSQLTypeName.FLOAT64),
                            Field.of("double", StandardSQLTypeName.FLOAT64),
                            Field.of("tmp", StandardSQLTypeName.TIMESTAMP),
                            Field.of("tmp9", StandardSQLTypeName.TIMESTAMP),
                            Field.of("date", StandardSQLTypeName.DATE),
                            Field.of("time", StandardSQLTypeName.TIME),
                            Field.newBuilder("array", StandardSQLTypeName.INT64).setMode(Field.Mode.REPEATED).build(),
                            Field.of("row", StandardSQLTypeName.STRUCT,
                                    Field.of("string_field", StandardSQLTypeName.STRING),
                                    Field.of("int_field", StandardSQLTypeName.INT64),
                                    Field.of("date_field", StandardSQLTypeName.DATE)),
                            Field.of("decimal", StandardSQLTypeName.NUMERIC),
                            Field.newBuilder("decimal_array", StandardSQLTypeName.NUMERIC).setMode(Field.Mode.REPEATED).build()
                    )
            ).build()));
        }
    }
}
