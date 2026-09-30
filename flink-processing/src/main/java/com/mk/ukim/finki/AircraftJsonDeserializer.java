package com.mk.ukim.finki;

import com.google.gson.Gson;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

public class AircraftJsonDeserializer extends RichMapFunction<String, AircraftEvent> {

    private transient Gson gson;

    @Override
    public void open(OpenContext openContext) {
        gson = new Gson();
    }

    @Override
    public AircraftEvent map(String json) {
        return gson.fromJson(json, AircraftEvent.class);
    }
}