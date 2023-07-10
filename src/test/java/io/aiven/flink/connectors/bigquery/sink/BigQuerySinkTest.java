package io.aiven.flink.connectors.bigquery.sink;

import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class BigQuerySinkTest {
  private static final Map<String, String> ENV_PROP_MAP = System.getenv();
  private static final String BIG_QUERY_SERVICE_ACCOUNT =
      ENV_PROP_MAP.get("BIG_QUERY_SERVICE_ACCOUNT");
  private static final String BIG_QUERY_PROJECT_ID = ENV_PROP_MAP.get("BIG_QUERY_PROJECT_ID");
  private static final String DATASET_NAME = "TestDataSet";
  private static final Credentials CREDENTIALS;

  static {
    try (FileInputStream fis = new FileInputStream(BIG_QUERY_SERVICE_ACCOUNT)) {
      CREDENTIALS = ServiceAccountCredentials.fromStream(fis);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @ParameterizedTest
  @MethodSource("datatypeProvider")
  void tableCreationTest(String tableName, String[] fieldNames, DataType[] fieldTypes) {
    BigQueryConnectionOptions options =
        new BigQueryConnectionOptions(
            BIG_QUERY_PROJECT_ID, DATASET_NAME, tableName, true, CREDENTIALS);
    var table = BigQuerySink.ensureTableExists(fieldNames, fieldTypes, options);
    table.delete();
  }

  static Stream<Arguments> datatypeProvider() {
    return Stream.of(
        Arguments.of(
            "decimal-test",
            new String[] {"decimal10_5_notNull"},
            new DataType[] {DataTypes.DECIMAL(10, 5).notNull()}),
        Arguments.of(
            "int-test", new String[] {"int_notNull"}, new DataType[] {DataTypes.INT().notNull()}),
        Arguments.of(
            "array-test",
            new String[] {"array_of_strings"},
            new DataType[] {DataTypes.ARRAY(DataTypes.STRING()).notNull()}),
        Arguments.of(
            "array-row-array-string-int-test",
            new String[] {"row_of_string_int"},
            new DataType[] {
              DataTypes.ARRAY(DataTypes.ROW(DataTypes.ARRAY(DataTypes.ROW(DataTypes.INT()))))
                  .notNull()
            }),
        Arguments.of(
            "row-array-row-array-string-int-test",
            new String[] {"row_of_string_int"},
            new DataType[] {
              DataTypes.ROW(
                      DataTypes.ARRAY(DataTypes.ROW(DataTypes.ARRAY(DataTypes.INT()))),
                      DataTypes.INT())
                  .notNull()
            }),
        Arguments.of(
            "row-string-int-test",
            new String[] {"row_of_string_int"},
            new DataType[] {DataTypes.ROW(DataTypes.STRING(), DataTypes.INT()).notNull()}),
        Arguments.of(
            "row-row-string-int-test",
            new String[] {"row_row_of_string_int"},
            new DataType[] {
              DataTypes.ROW(
                      DataTypes.ROW(DataTypes.DECIMAL(4, 3)), DataTypes.STRING(), DataTypes.INT())
                  .notNull()
            }),
        Arguments.of(
            "row-row-row-string-int-test",
            new String[] {"row_row_row_of_string_int"},
            new DataType[] {
              DataTypes.ROW(
                      DataTypes.ROW(DataTypes.ROW(DataTypes.DATE()), DataTypes.DECIMAL(4, 3)),
                      DataTypes.STRING(),
                      DataTypes.INT())
                  .notNull()
            }),
        Arguments.of(
            "row-row-row-array-string-int-test",
            new String[] {"row_row_row_of_string_int"},
            new DataType[] {
              DataTypes.ROW(
                      DataTypes.ROW(
                          DataTypes.ROW(DataTypes.ARRAY(DataTypes.DATE())),
                          DataTypes.DECIMAL(4, 3)),
                      DataTypes.STRING(),
                      DataTypes.INT())
                  .notNull()
            }));
  }
}
