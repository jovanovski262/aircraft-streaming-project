package com.mk.ukim.finki;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.*;
import java.time.Instant;
import java.util.*;

public class BenchmarkRunner {

    private static final String CLICKHOUSE_URL =
            "jdbc:clickhouse://localhost:8123/aircraft";

    private static final String CLICKHOUSE_USER = "clickhouse";
    private static final String CLICKHOUSE_PASSWORD = "clickhouse";

    private static final String TIMESCALE_URL =
            "jdbc:postgresql://localhost:5434/aircraft";

    private static final String TIMESCALE_USER = "postgres";
    private static final String TIMESCALE_PASSWORD = "postgres";


    private static final int WARMUP_RUNS = 5;
    private static final int MEASURED_RUNS = 20;

    private static final long PAUSE_BETWEEN_DATABASES_MS = 500;

    private static final String RAW_OUTPUT_FILE =
            "benchmark-raw.csv";

    private static final String SUMMARY_OUTPUT_FILE =
            "benchmark-summary.csv";

    private static final String METADATA_OUTPUT_FILE =
            "benchmark-metadata.csv";


    private static volatile long BLACKHOLE = 0;


    public static void main(String[] args) {

        System.out.println("========================================");
        System.out.println(" Aircraft Database Benchmark");
        System.out.println("========================================");
        System.out.println();

        try {

            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
            Class.forName("org.postgresql.Driver");

            List<BenchmarkQuery> queries = createQueries();

            try (
                    Connection clickHouse =
                            DriverManager.getConnection(
                                    CLICKHOUSE_URL,
                                    CLICKHOUSE_USER,
                                    CLICKHOUSE_PASSWORD
                            );

                    Connection timescale =
                            DriverManager.getConnection(
                                    TIMESCALE_URL,
                                    TIMESCALE_USER,
                                    TIMESCALE_PASSWORD
                            );

                    PrintWriter rawWriter =
                            new PrintWriter(
                                    new FileWriter(RAW_OUTPUT_FILE)
                            );

                    PrintWriter summaryWriter =
                            new PrintWriter(
                                    new FileWriter(SUMMARY_OUTPUT_FILE)
                            );

                    PrintWriter metadataWriter =
                            new PrintWriter(
                                    new FileWriter(METADATA_OUTPUT_FILE)
                            )
            ) {

                System.out.println("Connected to ClickHouse.");
                System.out.println("Connected to TimescaleDB.");
                System.out.println();

                configureConnections(
                        clickHouse,
                        timescale
                );


                DatasetInfo clickHouseInfo =
                        getClickHouseDatasetInfo(clickHouse);

                DatasetInfo timescaleInfo =
                        getTimescaleDatasetInfo(timescale);

                validateDatasets(
                        clickHouseInfo,
                        timescaleInfo
                );

                writeMetadata(
                        metadataWriter,
                        clickHouseInfo,
                        timescaleInfo
                );


                rawWriter.println(
                        "query_id,query_description,database,run," +
                                "execution_ms,result_rows"
                );

                summaryWriter.println(
                        "query_id,query_description,database," +
                                "measured_runs,result_rows," +
                                "mean_ms,median_ms,min_ms,max_ms," +
                                "std_dev_ms,p95_ms"
                );


                for (int i = 0; i < queries.size(); i++) {

                    BenchmarkQuery query =
                            queries.get(i);

                    System.out.println();
                    System.out.println(
                            "========================================"
                    );

                    System.out.println(
                            query.id + " - " + query.description
                    );

                    System.out.println(
                            "========================================"
                    );

                    BenchmarkResult clickHouseResult;
                    BenchmarkResult timescaleResult;


                    if (i % 2 == 0) {

                        clickHouseResult =
                                benchmarkDatabase(
                                        query,
                                        "ClickHouse",
                                        clickHouse,
                                        query.clickHouseSql,
                                        rawWriter
                                );

                        pause();

                        timescaleResult =
                                benchmarkDatabase(
                                        query,
                                        "TimescaleDB",
                                        timescale,
                                        query.timescaleSql,
                                        rawWriter
                                );

                    } else {

                        timescaleResult =
                                benchmarkDatabase(
                                        query,
                                        "TimescaleDB",
                                        timescale,
                                        query.timescaleSql,
                                        rawWriter
                                );

                        pause();

                        clickHouseResult =
                                benchmarkDatabase(
                                        query,
                                        "ClickHouse",
                                        clickHouse,
                                        query.clickHouseSql,
                                        rawWriter
                                );
                    }


                    if (clickHouseResult.resultRows
                            != timescaleResult.resultRows) {

                        System.out.println();
                        System.out.println(
                                "WARNING: Result row counts differ!"
                        );

                        System.out.println(
                                "ClickHouse rows: "
                                        + clickHouseResult.resultRows
                        );

                        System.out.println(
                                "TimescaleDB rows: "
                                        + timescaleResult.resultRows
                        );
                    }


                    writeSummary(
                            summaryWriter,
                            query,
                            clickHouseResult
                    );

                    writeSummary(
                            summaryWriter,
                            query,
                            timescaleResult
                    );

                    rawWriter.flush();
                    summaryWriter.flush();
                }

                System.out.println();
                System.out.println(
                        "========================================"
                );

                System.out.println(
                        " Benchmark completed successfully"
                );

                System.out.println(
                        "========================================"
                );

                System.out.println(
                        "Raw results:     "
                                + RAW_OUTPUT_FILE
                );

                System.out.println(
                        "Summary results: "
                                + SUMMARY_OUTPUT_FILE
                );

                System.out.println(
                        "Metadata:        "
                                + METADATA_OUTPUT_FILE
                );

                System.out.println(
                        "Consumption checksum: "
                                + BLACKHOLE
                );
            }

        } catch (Exception e) {

            System.err.println();
            System.err.println("Benchmark failed:");

            e.printStackTrace();
        }
    }


    private static void configureConnections(
            Connection clickHouse,
            Connection timescale
    ) throws SQLException {

        clickHouse.setReadOnly(true);
        timescale.setReadOnly(true);
    }


    private static DatasetInfo getClickHouseDatasetInfo(
            Connection connection
    ) throws SQLException {

        String sql =
                """
                SELECT
                    COUNT(*) AS row_count,
                    toUnixTimestamp(MIN(event_timestamp))
                        AS min_timestamp,
                    toUnixTimestamp(MAX(event_timestamp))
                        AS max_timestamp,
                    COUNT(DISTINCT icao24)
                        AS distinct_aircraft
                FROM processed_aircraft_events
                """;

        try (
                Statement statement =
                        connection.createStatement();

                ResultSet resultSet =
                        statement.executeQuery(sql)
        ) {

            if (!resultSet.next()) {

                throw new SQLException(
                        "Could not retrieve ClickHouse dataset information."
                );
            }

            return new DatasetInfo(
                    resultSet.getLong("row_count"),
                    resultSet.getLong("min_timestamp"),
                    resultSet.getLong("max_timestamp"),
                    resultSet.getLong("distinct_aircraft")
            );
        }
    }

    private static DatasetInfo getTimescaleDatasetInfo(
            Connection connection
    ) throws SQLException {

        String sql =
                """
                SELECT
                    COUNT(*) AS row_count,
                    EXTRACT(
                        EPOCH FROM MIN(event_timestamp)
                    )::BIGINT AS min_timestamp,
                    EXTRACT(
                        EPOCH FROM MAX(event_timestamp)
                    )::BIGINT AS max_timestamp,
                    COUNT(DISTINCT icao24)
                        AS distinct_aircraft
                FROM processed_aircraft_events
                """;

        try (
                Statement statement =
                        connection.createStatement();

                ResultSet resultSet =
                        statement.executeQuery(sql)
        ) {

            if (!resultSet.next()) {

                throw new SQLException(
                        "Could not retrieve TimescaleDB dataset information."
                );
            }

            return new DatasetInfo(
                    resultSet.getLong("row_count"),
                    resultSet.getLong("min_timestamp"),
                    resultSet.getLong("max_timestamp"),
                    resultSet.getLong("distinct_aircraft")
            );
        }
    }


    private static void validateDatasets(
            DatasetInfo clickHouse,
            DatasetInfo timescale
    ) {

        System.out.println("Dataset validation");
        System.out.println("--------------------");

        System.out.println(
                "ClickHouse rows:       "
                        + clickHouse.rowCount
        );

        System.out.println(
                "TimescaleDB rows:      "
                        + timescale.rowCount
        );

        System.out.println(
                "ClickHouse aircraft:   "
                        + clickHouse.distinctAircraft
        );

        System.out.println(
                "TimescaleDB aircraft:  "
                        + timescale.distinctAircraft
        );

        System.out.println(
                "ClickHouse min epoch:  "
                        + clickHouse.minTimestamp
        );

        System.out.println(
                "TimescaleDB min epoch: "
                        + timescale.minTimestamp
        );

        System.out.println(
                "ClickHouse max epoch:  "
                        + clickHouse.maxTimestamp
        );

        System.out.println(
                "TimescaleDB max epoch: "
                        + timescale.maxTimestamp
        );

        boolean equal =
                clickHouse.rowCount
                        == timescale.rowCount

                        && clickHouse.distinctAircraft
                        == timescale.distinctAircraft

                        && clickHouse.minTimestamp
                        == timescale.minTimestamp

                        && clickHouse.maxTimestamp
                        == timescale.maxTimestamp;

        if (!equal) {

            throw new IllegalStateException(
                    "Datasets are not equivalent. "
                            + "Benchmark aborted."
            );
        }

        if (clickHouse.rowCount == 0) {

            throw new IllegalStateException(
                    "Dataset is empty. "
                            + "Benchmark aborted."
            );
        }

        System.out.println();
        System.out.println(
                "Dataset validation: PASSED"
        );
    }


    private static BenchmarkResult benchmarkDatabase(
            BenchmarkQuery query,
            String database,
            Connection connection,
            String sql,
            PrintWriter rawWriter
    ) throws SQLException {

        System.out.println();
        System.out.println(database);
        System.out.println("--------------------");


        for (int i = 1;
             i <= WARMUP_RUNS;
             i++) {

            executeAndConsume(
                    connection,
                    sql
            );

            System.out.println(
                    "Warm-up "
                            + i
                            + " completed"
            );
        }


        List<Double> times =
                new ArrayList<>();

        int expectedResultRows = -1;

        for (int run = 1;
             run <= MEASURED_RUNS;
             run++) {

            long start =
                    System.nanoTime();

            int resultRows =
                    executeAndConsume(
                            connection,
                            sql
                    );

            long end =
                    System.nanoTime();

            double executionMs =
                    (end - start)
                            / 1_000_000.0;

            if (expectedResultRows == -1) {

                expectedResultRows =
                        resultRows;

            } else if (
                    resultRows
                            != expectedResultRows
            ) {

                throw new IllegalStateException(
                        query.id
                                + " returned inconsistent "
                                + "result row counts."
                );
            }

            times.add(executionMs);

            rawWriter.printf(
                    Locale.US,
                    "%s,\"%s\",%s,%d,%.3f,%d%n",
                    query.id,
                    escapeCsv(query.description),
                    database,
                    run,
                    executionMs,
                    resultRows
            );

            System.out.printf(
                    Locale.US,
                    "Run %2d: %9.3f ms | rows: %d%n",
                    run,
                    executionMs,
                    resultRows
            );
        }


        double mean =
                calculateMean(times);

        double median =
                calculateMedian(times);

        double min =
                Collections.min(times);

        double max =
                Collections.max(times);

        double stdDev =
                calculateSampleStandardDeviation(
                        times,
                        mean
                );

        double p95 =
                calculatePercentile(
                        times,
                        0.95
                );

        System.out.println();
        System.out.println("Statistics");
        System.out.println("--------------------");

        System.out.printf(
                Locale.US,
                "Mean:    %.3f ms%n",
                mean
        );

        System.out.printf(
                Locale.US,
                "Median:  %.3f ms%n",
                median
        );

        System.out.printf(
                Locale.US,
                "Min:     %.3f ms%n",
                min
        );

        System.out.printf(
                Locale.US,
                "Max:     %.3f ms%n",
                max
        );

        System.out.printf(
                Locale.US,
                "StdDev:  %.3f ms%n",
                stdDev
        );

        System.out.printf(
                Locale.US,
                "P95:     %.3f ms%n",
                p95
        );

        return new BenchmarkResult(
                database,
                expectedResultRows,
                mean,
                median,
                min,
                max,
                stdDev,
                p95
        );
    }


    private static int executeAndConsume(
            Connection connection,
            String sql
    ) throws SQLException {

        int rowCount = 0;

        long checksum = 0;

        try (
                Statement statement =
                        connection.createStatement();

                ResultSet resultSet =
                        statement.executeQuery(sql)
        ) {

            ResultSetMetaData metadata =
                    resultSet.getMetaData();

            int columns =
                    metadata.getColumnCount();

            while (resultSet.next()) {

                rowCount++;

                for (int i = 1;
                     i <= columns;
                     i++) {

                    Object value =
                            resultSet.getObject(i);

                    if (value != null) {

                        checksum +=
                                value.hashCode();
                    }
                }
            }
        }

        BLACKHOLE ^= checksum;

        return rowCount;
    }


    private static double calculateMean(
            List<Double> values
    ) {

        return values.stream()
                .mapToDouble(
                        Double::doubleValue
                )
                .average()
                .orElseThrow();
    }

    private static double calculateMedian(
            List<Double> values
    ) {

        List<Double> sorted =
                new ArrayList<>(values);

        Collections.sort(sorted);

        int n =
                sorted.size();

        if (n % 2 == 0) {

            return (
                    sorted.get(n / 2 - 1)
                            +
                            sorted.get(n / 2)
            ) / 2.0;
        }

        return sorted.get(n / 2);
    }


    private static double calculateSampleStandardDeviation(
            List<Double> values,
            double mean
    ) {

        if (values.size() < 2) {
            return 0;
        }

        double squaredDifferences =
                0;

        for (double value : values) {

            double difference =
                    value - mean;

            squaredDifferences +=
                    difference * difference;
        }

        return Math.sqrt(
                squaredDifferences
                        /
                        (values.size() - 1)
        );
    }


    private static double calculatePercentile(
            List<Double> values,
            double percentile
    ) {

        List<Double> sorted =
                new ArrayList<>(values);

        Collections.sort(sorted);

        int index =
                (int) Math.ceil(
                        percentile
                                * sorted.size()
                ) - 1;

        index =
                Math.max(
                        0,
                        Math.min(
                                index,
                                sorted.size() - 1
                        )
                );

        return sorted.get(index);
    }


    private static void writeSummary(
            PrintWriter writer,
            BenchmarkQuery query,
            BenchmarkResult result
    ) {

        writer.printf(
                Locale.US,
                "%s,\"%s\",%s,%d,%d," +
                        "%.3f,%.3f,%.3f,%.3f,%.3f,%.3f%n",

                query.id,
                escapeCsv(query.description),
                result.database,
                MEASURED_RUNS,
                result.resultRows,
                result.mean,
                result.median,
                result.min,
                result.max,
                result.stdDev,
                result.p95
        );
    }

    private static void writeMetadata(
            PrintWriter writer,
            DatasetInfo clickHouse,
            DatasetInfo timescale
    ) {

        String benchmarkStarted =
                Instant.now().toString();

        writer.println(
                "property,clickhouse,timescaledb"
        );

        writer.printf(
                "row_count,%d,%d%n",
                clickHouse.rowCount,
                timescale.rowCount
        );

        writer.printf(
                "distinct_aircraft,%d,%d%n",
                clickHouse.distinctAircraft,
                timescale.distinctAircraft
        );

        writer.printf(
                "min_timestamp_epoch,%d,%d%n",
                clickHouse.minTimestamp,
                timescale.minTimestamp
        );

        writer.printf(
                "max_timestamp_epoch,%d,%d%n",
                clickHouse.maxTimestamp,
                timescale.maxTimestamp
        );

        writer.printf(
                "warmup_runs,%d,%d%n",
                WARMUP_RUNS,
                WARMUP_RUNS
        );

        writer.printf(
                "measured_runs,%d,%d%n",
                MEASURED_RUNS,
                MEASURED_RUNS
        );

        writer.printf(
                "benchmark_started_utc,\"%s\",\"%s\"%n",
                benchmarkStarted,
                benchmarkStarted
        );

        writer.flush();
    }

    private static String escapeCsv(
            String value
    ) {

        return value.replace(
                "\"",
                "\"\""
        );
    }


    private static void pause() {

        try {

            Thread.sleep(
                    PAUSE_BETWEEN_DATABASES_MS
            );

        } catch (InterruptedException e) {

            Thread.currentThread()
                    .interrupt();

            throw new RuntimeException(e);
        }
    }


    private static List<BenchmarkQuery> createQueries() {

        List<BenchmarkQuery> queries =
                new ArrayList<>();


        queries.add(
                new BenchmarkQuery(
                        "Q1",
                        "Count all observations",

                        """
                        SELECT COUNT(*)
                        FROM processed_aircraft_events
                        """,

                        """
                        SELECT COUNT(*)
                        FROM processed_aircraft_events
                        """
                )
        );


        queries.add(
                new BenchmarkQuery(
                        "Q2",
                        "Count distinct aircraft",

                        """
                        SELECT COUNT(DISTINCT icao24)
                        FROM processed_aircraft_events
                        """,

                        """
                        SELECT COUNT(DISTINCT icao24)
                        FROM processed_aircraft_events
                        """
                )
        );


        queries.add(
                new BenchmarkQuery(
                        "Q3",
                        "Observations by origin country",

                        """
                        SELECT
                            origin_country,
                            COUNT(*) AS observations
                        FROM processed_aircraft_events
                        GROUP BY origin_country
                        ORDER BY observations DESC
                        """,

                        """
                        SELECT
                            origin_country,
                            COUNT(*) AS observations
                        FROM processed_aircraft_events
                        GROUP BY origin_country
                        ORDER BY observations DESC
                        """
                )
        );


        queries.add(
                new BenchmarkQuery(
                        "Q4",
                        "Average velocity by origin country",

                        """
                        SELECT
                            origin_country,
                            AVG(velocity_kmh) AS avg_velocity
                        FROM processed_aircraft_events
                        WHERE velocity_kmh IS NOT NULL
                        GROUP BY origin_country
                        ORDER BY avg_velocity DESC
                        """,

                        """
                        SELECT
                            origin_country,
                            AVG(velocity_kmh) AS avg_velocity
                        FROM processed_aircraft_events
                        WHERE velocity_kmh IS NOT NULL
                        GROUP BY origin_country
                        ORDER BY avg_velocity DESC
                        """
                )
        );


        queries.add(
                new BenchmarkQuery(
                        "Q5",
                        "Flight phase distribution",

                        """
                        SELECT
                            flight_phase,
                            COUNT(*) AS observations
                        FROM processed_aircraft_events
                        GROUP BY flight_phase
                        ORDER BY observations DESC
                        """,

                        """
                        SELECT
                            flight_phase,
                            COUNT(*) AS observations
                        FROM processed_aircraft_events
                        GROUP BY flight_phase
                        ORDER BY observations DESC
                        """
                )
        );


        queries.add(
                new BenchmarkQuery(
                        "Q6",
                        "Observations per minute",

                        """
                        SELECT
                            toStartOfMinute(event_timestamp)
                                AS minute,
                            COUNT(*) AS observations
                        FROM processed_aircraft_events
                        GROUP BY minute
                        ORDER BY minute
                        """,

                        """
                        SELECT
                            time_bucket(
                                '1 minute',
                                event_timestamp
                            ) AS minute,
                            COUNT(*) AS observations
                        FROM processed_aircraft_events
                        GROUP BY minute
                        ORDER BY minute
                        """
                )
        );


        queries.add(
                new BenchmarkQuery(
                        "Q7",
                        "Average altitude per minute",

                        """
                        SELECT
                            toStartOfMinute(event_timestamp)
                                AS minute,
                            AVG(altitude_feet)
                                AS avg_altitude
                        FROM processed_aircraft_events
                        WHERE altitude_feet IS NOT NULL
                        GROUP BY minute
                        ORDER BY minute
                        """,

                        """
                        SELECT
                            time_bucket(
                                '1 minute',
                                event_timestamp
                            ) AS minute,
                            AVG(altitude_feet)
                                AS avg_altitude
                        FROM processed_aircraft_events
                        WHERE altitude_feet IS NOT NULL
                        GROUP BY minute
                        ORDER BY minute
                        """
                )
        );


        queries.add(
                new BenchmarkQuery(
                        "Q8",
                        "Recent 10-minute time-range aggregation",

                        """
                        SELECT
                            COUNT(*) AS observations,
                            AVG(velocity_kmh)
                                AS avg_velocity,
                            AVG(altitude_feet)
                                AS avg_altitude
                        FROM processed_aircraft_events
                        WHERE event_timestamp >=
                        (
                            SELECT MAX(event_timestamp)
                            FROM processed_aircraft_events
                        ) - INTERVAL 10 MINUTE
                        """,

                        """
                        SELECT
                            COUNT(*) AS observations,
                            AVG(velocity_kmh)
                                AS avg_velocity,
                            AVG(altitude_feet)
                                AS avg_altitude
                        FROM processed_aircraft_events
                        WHERE event_timestamp >=
                        (
                            SELECT MAX(event_timestamp)
                            FROM processed_aircraft_events
                        ) - INTERVAL '10 minutes'
                        """
                )
        );


        queries.add(
                new BenchmarkQuery(
                        "Q9",
                        "Trajectory of most observed aircraft",

                        """
                        SELECT
                            event_timestamp,
                            latitude,
                            longitude,
                            altitude_feet,
                            velocity_kmh
                        FROM processed_aircraft_events
                        WHERE icao24 = (
                            SELECT icao24
                            FROM processed_aircraft_events
                            GROUP BY icao24
                            ORDER BY
                                COUNT(*) DESC,
                                icao24 ASC
                            LIMIT 1
                        )
                        ORDER BY event_timestamp
                        """,

                        """
                        SELECT
                            event_timestamp,
                            latitude,
                            longitude,
                            altitude_feet,
                            velocity_kmh
                        FROM processed_aircraft_events
                        WHERE icao24 = (
                            SELECT icao24
                            FROM processed_aircraft_events
                            GROUP BY icao24
                            ORDER BY
                                COUNT(*) DESC,
                                icao24 ASC
                            LIMIT 1
                        )
                        ORDER BY event_timestamp
                        """
                )
        );


        queries.add(
                new BenchmarkQuery(
                        "Q10",
                        "Latest observation for every aircraft",

                        """
                        SELECT
                            icao24,
                            argMax(
                                latitude,
                                event_timestamp
                            ) AS latitude,
                            argMax(
                                longitude,
                                event_timestamp
                            ) AS longitude,
                            argMax(
                                altitude_feet,
                                event_timestamp
                            ) AS altitude_feet,
                            MAX(event_timestamp)
                                AS latest_timestamp
                        FROM processed_aircraft_events
                        GROUP BY icao24
                        """,

                        """
                        SELECT DISTINCT ON (icao24)
                            icao24,
                            latitude,
                            longitude,
                            altitude_feet,
                            event_timestamp
                                AS latest_timestamp
                        FROM processed_aircraft_events
                        ORDER BY
                            icao24,
                            event_timestamp DESC
                        """
                )
        );

        return queries;
    }


    private static class BenchmarkQuery {

        private final String id;
        private final String description;

        private final String clickHouseSql;
        private final String timescaleSql;

        private BenchmarkQuery(
                String id,
                String description,
                String clickHouseSql,
                String timescaleSql
        ) {

            this.id = id;
            this.description = description;

            this.clickHouseSql =
                    clickHouseSql;

            this.timescaleSql =
                    timescaleSql;
        }
    }

    private static class BenchmarkResult {

        private final String database;

        private final int resultRows;

        private final double mean;
        private final double median;
        private final double min;
        private final double max;
        private final double stdDev;
        private final double p95;

        private BenchmarkResult(
                String database,
                int resultRows,
                double mean,
                double median,
                double min,
                double max,
                double stdDev,
                double p95
        ) {

            this.database =
                    database;

            this.resultRows =
                    resultRows;

            this.mean =
                    mean;

            this.median =
                    median;

            this.min =
                    min;

            this.max =
                    max;

            this.stdDev =
                    stdDev;

            this.p95 =
                    p95;
        }
    }


    private static class DatasetInfo {

        private final long rowCount;

        private final long minTimestamp;
        private final long maxTimestamp;

        private final long distinctAircraft;

        private DatasetInfo(
                long rowCount,
                long minTimestamp,
                long maxTimestamp,
                long distinctAircraft
        ) {

            this.rowCount =
                    rowCount;

            this.minTimestamp =
                    minTimestamp;

            this.maxTimestamp =
                    maxTimestamp;

            this.distinctAircraft =
                    distinctAircraft;
        }
    }
}