package com.mk.ukim.finki;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

public class TimescaleDBSink implements Sink<ProcessedAircraftEvent> {

    private static final String JDBC_URL =
            "jdbc:postgresql://timescaledb:5432/aircraft";

    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "postgres";

    @Override
    public SinkWriter<ProcessedAircraftEvent> createWriter(
            WriterInitContext context
    ) throws IOException {

        try {
            return new TimescaleDBSinkWriter();
        } catch (SQLException e) {
            throw new IOException(
                    "Failed to create TimescaleDB sink writer",
                    e
            );
        }
    }

    private static class TimescaleDBSinkWriter
            implements SinkWriter<ProcessedAircraftEvent> {

        private static final int BATCH_SIZE = 1000;

        private final Connection connection;
        private final PreparedStatement statement;

        private int currentBatchSize = 0;

        private TimescaleDBSinkWriter() throws SQLException {

            try {
                Class.forName("org.postgresql.Driver");
            } catch (ClassNotFoundException e) {
                throw new SQLException(
                        "PostgreSQL JDBC driver not found",
                        e
                );
            }

            connection = DriverManager.getConnection(
                    JDBC_URL,
                    USERNAME,
                    PASSWORD
            );

            String sql = """
                    INSERT INTO processed_aircraft_events
                    (
                        icao24,
                        callsign,
                        origin_country,
                        event_timestamp,
                        longitude,
                        latitude,
                        barometric_altitude,
                        velocity,
                        heading,
                        vertical_rate,
                        on_ground,
                        geometric_altitude,
                        squawk,
                        position_source,
                        category,
                        velocity_kmh,
                        altitude_feet,
                        flight_phase,
                        processed_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """;

            statement = connection.prepareStatement(sql);
        }

        @Override
        public void write(
                ProcessedAircraftEvent event,
                Context context
        ) throws IOException {

            try {
                statement.setString(
                        1,
                        event.getIcao24()
                );

                setNullableString(
                        statement,
                        2,
                        event.getCallsign()
                );

                setNullableString(
                        statement,
                        3,
                        event.getOriginCountry()
                );

                statement.setTimestamp(
                        4,
                        new Timestamp(
                                event.getTimestamp() * 1000L
                        )
                );

                statement.setDouble(
                        5,
                        event.getLongitude()
                );

                statement.setDouble(
                        6,
                        event.getLatitude()
                );

                setNullableDouble(
                        statement,
                        7,
                        event.getBarometricAltitude()
                );

                setNullableDouble(
                        statement,
                        8,
                        event.getVelocity()
                );

                setNullableDouble(
                        statement,
                        9,
                        event.getHeading()
                );

                setNullableDouble(
                        statement,
                        10,
                        event.getVerticalRate()
                );

                if (event.getOnGround() == null) {
                    statement.setNull(
                            11,
                            Types.BOOLEAN
                    );
                } else {
                    statement.setBoolean(
                            11,
                            event.getOnGround()
                    );
                }

                setNullableDouble(
                        statement,
                        12,
                        event.getGeometricAltitude()
                );

                setNullableString(
                        statement,
                        13,
                        event.getSquawk()
                );

                setNullableInteger(
                        statement,
                        14,
                        event.getPositionSource()
                );

                setNullableInteger(
                        statement,
                        15,
                        event.getCategory()
                );

                setNullableDouble(
                        statement,
                        16,
                        event.getVelocityKmh()
                );

                setNullableDouble(
                        statement,
                        17,
                        event.getAltitudeFeet()
                );

                setNullableString(
                        statement,
                        18,
                        event.getFlightPhase()
                );

                statement.setTimestamp(
                        19,
                        new Timestamp(
                                event.getProcessedAt()
                        )
                );

                statement.addBatch();
                currentBatchSize++;

                if (currentBatchSize >= BATCH_SIZE) {
                    flushBatch();
                }

            } catch (SQLException e) {
                throw new IOException(
                        "Failed to write event to TimescaleDB",
                        e
                );
            }
        }

        @Override
        public void flush(
                boolean endOfInput
        ) throws IOException {

            try {
                flushBatch();
            } catch (SQLException e) {
                throw new IOException(
                        "Failed to flush TimescaleDB batch",
                        e
                );
            }
        }

        private void flushBatch() throws SQLException {

            if (currentBatchSize == 0) {
                return;
            }

            statement.executeBatch();
            currentBatchSize = 0;
        }

        @Override
        public void close() throws Exception {

            try {
                flushBatch();
            } finally {
                statement.close();
                connection.close();
            }
        }

        private static void setNullableString(
                PreparedStatement statement,
                int index,
                String value
        ) throws SQLException {

            if (value == null) {
                statement.setNull(
                        index,
                        Types.VARCHAR
                );
            } else {
                statement.setString(
                        index,
                        value
                );
            }
        }

        private static void setNullableDouble(
                PreparedStatement statement,
                int index,
                Double value
        ) throws SQLException {

            if (value == null) {
                statement.setNull(
                        index,
                        Types.DOUBLE
                );
            } else {
                statement.setDouble(
                        index,
                        value
                );
            }
        }

        private static void setNullableInteger(
                PreparedStatement statement,
                int index,
                Integer value
        ) throws SQLException {

            if (value == null) {
                statement.setNull(
                        index,
                        Types.INTEGER
                );
            } else {
                statement.setInt(
                        index,
                        value
                );
            }
        }
    }
}