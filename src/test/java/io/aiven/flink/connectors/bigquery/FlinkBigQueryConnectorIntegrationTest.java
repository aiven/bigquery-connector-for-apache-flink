package io.aiven.flink.connectors.bigquery;

import static org.apache.flink.table.api.Expressions.row;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Map;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

public class FlinkBigQueryConnectorIntegrationTest {

  private static final Map<String, String> ENV_PROP_MAP = System.getenv();
  private static final String BIG_QUERY_SERVICE_ACCOUNT =
      ENV_PROP_MAP.get("BIG_QUERY_SERVICE_ACCOUNT");
  private static final String BIG_QUERY_PROJECT_ID = ENV_PROP_MAP.get("BIG_QUERY_PROJECT_ID");

  @Test
  public void testSinkForDifferentTypes() throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(8);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    final ResolvedSchema schema =
        ResolvedSchema.of(
            Column.metadata("string", DataTypes.STRING().nullable(), null, false),
            Column.metadata("int", DataTypes.INT().nullable(), null, false),
            Column.metadata("boolean", DataTypes.BOOLEAN().nullable(), null, false),
            Column.metadata("float", DataTypes.FLOAT().nullable(), null, false),
            Column.metadata("double", DataTypes.DOUBLE().nullable(), null, false),
            Column.metadata("tmp", DataTypes.TIMESTAMP().nullable(), null, false),
            Column.metadata("tmp9", DataTypes.TIMESTAMP(9).nullable(), null, false),
            Column.metadata("date", DataTypes.DATE().nullable(), null, false),
            Column.metadata("time", DataTypes.TIME().nullable(), null, false),
            Column.metadata("array", DataTypes.ARRAY(DataTypes.INT()).nullable(), null, false),
            Column.metadata(
                "row",
                DataTypes.ROW(
                    DataTypes.FIELD("string_field", DataTypes.STRING().nullable()),
                    DataTypes.FIELD("int_field", DataTypes.INT().nullable()),
                    DataTypes.FIELD("date_field", DataTypes.DATE().nullable())),
                null,
                false),
            Column.metadata("decimal", DataTypes.DECIMAL(5, 3).nullable(), null, false),
            Column.metadata(
                "decimal_array",
                DataTypes.ARRAY(DataTypes.DECIMAL(7, 2).nullable()).nullable(),
                null,
                false));
    tableEnv
        .executeSql(
            "CREATE TEMPORARY TABLE test_table ( \n"
                + "  `string` STRING,\n"
                + "  `int` INT,\n"
                + "  `boolean` BOOLEAN,\n"
                + "  `float` FLOAT,\n"
                + "  `double` DOUBLE,\n"
                + "  `tmp` TIMESTAMP(3),\n"
                + "  `tmp9` TIMESTAMP(9),\n"
                + "  `date` DATE,\n"
                + "  `time` TIME,\n"
                + "  `array` ARRAY<INT>,\n"
                + "  `row` ROW<string_field STRING, int_field INT, date_field DATE>,\n"
                + "  `decimal` DECIMAL(5, 3),\n"
                + "  `decimal_array` ARRAY<DECIMAL(7, 2)>\n"
                + ") WITH (\n"
                + "  'connector' = 'bigquery',"
                + "  'service-account' = '"
                + BIG_QUERY_SERVICE_ACCOUNT
                + "',"
                + "  'project-id' = '"
                + BIG_QUERY_PROJECT_ID
                + "',"
                + "  'dataset' = 'TestDataSet',"
                + "  'table' = 'test-table'"
                + ")")
        .await();

    tableEnv
        .fromValues(
            schema.toSinkRowDataType(),
            row(
                "123",
                123,
                false,
                12.345f,
                123.4567d,
                LocalDateTime.of(2021, 10, 31, 23, 34, 56),
                LocalDateTime.of(2021, 10, 31, 23, 34, 56, 987654312),
                LocalDate.of(2000, 9, 22),
                LocalTime.of(12, 13, 14),
                new int[] {1, 2, 3},
                Row.of("test", null, LocalDate.of(2030, 9, 25)),
                BigDecimal.valueOf(12.312),
                new BigDecimal[] {BigDecimal.valueOf(12233.12), BigDecimal.TEN, null}))
        .executeInsert("test_table")
        .await();
  }
}
