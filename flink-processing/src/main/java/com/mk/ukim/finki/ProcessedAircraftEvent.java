package com.mk.ukim.finki;

public class ProcessedAircraftEvent {

    private String icao24;
    private String callsign;
    private String originCountry;
    private Long timestamp;
    private Double longitude;
    private Double latitude;
    private Double barometricAltitude;
    private Double velocity;
    private Double heading;
    private Double verticalRate;
    private Boolean onGround;
    private Double geometricAltitude;
    private String squawk;
    private Integer positionSource;
    private Integer category;

    private Double velocityKmh;
    private Double altitudeFeet;
    private String flightPhase;
    private Long processedAt;

    public ProcessedAircraftEvent() {
    }

    public ProcessedAircraftEvent(
            String icao24,
            String callsign,
            String originCountry,
            Long timestamp,
            Double longitude,
            Double latitude,
            Double barometricAltitude,
            Double velocity,
            Double heading,
            Double verticalRate,
            Boolean onGround,
            Double geometricAltitude,
            String squawk,
            Integer positionSource,
            Integer category,
            Double velocityKmh,
            Double altitudeFeet,
            String flightPhase,
            Long processedAt
    ) {
        this.icao24 = icao24;
        this.callsign = callsign;
        this.originCountry = originCountry;
        this.timestamp = timestamp;
        this.longitude = longitude;
        this.latitude = latitude;
        this.barometricAltitude = barometricAltitude;
        this.velocity = velocity;
        this.heading = heading;
        this.verticalRate = verticalRate;
        this.onGround = onGround;
        this.geometricAltitude = geometricAltitude;
        this.squawk = squawk;
        this.positionSource = positionSource;
        this.category = category;
        this.velocityKmh = velocityKmh;
        this.altitudeFeet = altitudeFeet;
        this.flightPhase = flightPhase;
        this.processedAt = processedAt;
    }

    public String getIcao24() {
        return icao24;
    }

    public String getCallsign() {
        return callsign;
    }

    public String getOriginCountry() {
        return originCountry;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Double getLongitude() {
        return longitude;
    }

    public Double getLatitude() {
        return latitude;
    }

    public Double getBarometricAltitude() {
        return barometricAltitude;
    }

    public Double getVelocity() {
        return velocity;
    }

    public Double getHeading() {
        return heading;
    }

    public Double getVerticalRate() {
        return verticalRate;
    }

    public Boolean getOnGround() {
        return onGround;
    }

    public Double getGeometricAltitude() {
        return geometricAltitude;
    }

    public String getSquawk() {
        return squawk;
    }

    public Integer getPositionSource() {
        return positionSource;
    }

    public Integer getCategory() {
        return category;
    }

    public Double getVelocityKmh() {
        return velocityKmh;
    }

    public Double getAltitudeFeet() {
        return altitudeFeet;
    }

    public String getFlightPhase() {
        return flightPhase;
    }

    public Long getProcessedAt() {
        return processedAt;
    }

    @Override
    public String toString() {
        return "ProcessedAircraftEvent{" +
                "icao24='" + icao24 + '\'' +
                ", callsign='" + callsign + '\'' +
                ", originCountry='" + originCountry + '\'' +
                ", timestamp=" + timestamp +
                ", longitude=" + longitude +
                ", latitude=" + latitude +
                ", velocity=" + velocity +
                ", velocityKmh=" + velocityKmh +
                ", altitude=" + barometricAltitude +
                ", altitudeFeet=" + altitudeFeet +
                ", verticalRate=" + verticalRate +
                ", flightPhase='" + flightPhase + '\'' +
                ", onGround=" + onGround +
                ", processedAt=" + processedAt +
                '}';
    }
}