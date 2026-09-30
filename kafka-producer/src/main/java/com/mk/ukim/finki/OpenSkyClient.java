package com.mk.ukim.finki;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;

public class OpenSkyClient {

    private static final String OPEN_SKY_URL =
            "https://opensky-network.org/api/states/all";

    private final HttpClient httpClient;

    public OpenSkyClient() {
        this.httpClient = HttpClient.newHttpClient();
    }

    public List<AircraftEvent> fetchAircraft() throws IOException, InterruptedException {

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(OPEN_SKY_URL))
                .GET()
                .build();

        HttpResponse<String> response =
                httpClient.send(
                        request,
                        HttpResponse.BodyHandlers.ofString()
                );

        if (response.statusCode() != 200) {
            throw new RuntimeException(
                    "OpenSky request failed. Status code: " + response.statusCode()
            );
        }

        JsonObject jsonObject =
                JsonParser.parseString(response.body()).getAsJsonObject();

        JsonArray states = jsonObject.getAsJsonArray("states");

        List<AircraftEvent> aircraftEvents = new ArrayList<>();

        if (states == null) {
            return aircraftEvents;
        }

        for (JsonElement element : states) {

            JsonArray state = element.getAsJsonArray();

            AircraftEvent aircraftEvent = new AircraftEvent(
                    getString(state, 0),
                    getString(state, 1),
                    getString(state, 2),
                    getLong(state, 4),
                    getDouble(state, 5),
                    getDouble(state, 6),
                    getDouble(state, 7),
                    getDouble(state, 9),
                    getDouble(state, 10),
                    getDouble(state, 11),
                    getBoolean(state, 8),
                    getDouble(state, 13),
                    getString(state, 14),
                    getInteger(state, 16),
                    getInteger(state, 17)
            );

            aircraftEvents.add(aircraftEvent);
        }

        return aircraftEvents;
    }

    private String getString(JsonArray array, int index) {

        if (index >= array.size() || array.get(index).isJsonNull()) {
            return null;
        }

        return array.get(index).getAsString().trim();
    }

    private Double getDouble(JsonArray array, int index) {

        if (index >= array.size() || array.get(index).isJsonNull()) {
            return null;
        }

        return array.get(index).getAsDouble();
    }

    private Long getLong(JsonArray array, int index) {

        if (index >= array.size() || array.get(index).isJsonNull()) {
            return null;
        }

        return array.get(index).getAsLong();
    }

    private Integer getInteger(JsonArray array, int index) {

        if (index >= array.size() || array.get(index).isJsonNull()) {
            return null;
        }

        return array.get(index).getAsInt();
    }

    private Boolean getBoolean(JsonArray array, int index) {

        if (index >= array.size() || array.get(index).isJsonNull()) {
            return null;
        }

        return array.get(index).getAsBoolean();
    }
}