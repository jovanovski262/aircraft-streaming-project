package com.mk.ukim.finki;

import org.apache.flink.api.common.functions.MapFunction;

public class AircraftTransformer
        implements MapFunction<AircraftEvent, ProcessedAircraftEvent> {

    @Override
    public ProcessedAircraftEvent map(AircraftEvent event) {

        Double velocityKmh = null;
        if (event.getVelocity() != null) {
            velocityKmh = event.getVelocity() * 3.6;
        }

        Double altitudeFeet = null;
        if (event.getBarometricAltitude() != null) {
            altitudeFeet = event.getBarometricAltitude() * 3.28084;
        }

        String flightPhase;

        if (Boolean.TRUE.equals(event.getOnGround())) {
            flightPhase = "GROUND";
        } else if (event.getVerticalRate() != null && event.getVerticalRate() > 1.0) {
            flightPhase = "CLIMBING";
        } else if (event.getVerticalRate() != null && event.getVerticalRate() < -1.0) {
            flightPhase = "DESCENDING";
        } else {
            flightPhase = "LEVEL";
        }

        Long processedAt = System.currentTimeMillis();

        return new ProcessedAircraftEvent(
                event.getIcao24(),
                event.getCallsign(),
                event.getOriginCountry(),
                event.getTimestamp(),
                event.getLongitude(),
                event.getLatitude(),
                event.getBarometricAltitude(),
                event.getVelocity(),
                event.getHeading(),
                event.getVerticalRate(),
                event.getOnGround(),
                event.getGeometricAltitude(),
                event.getSquawk(),
                event.getPositionSource(),
                event.getCategory(),
                velocityKmh,
                altitudeFeet,
                flightPhase,
                processedAt
        );
    }
}