package com.github.alexsni.psyquation.views;

import java.io.Serializable;
import java.time.LocalDateTime;

public class JsonView implements Serializable {
    private LocalDateTime timeSlotStart;
    private String location;
    private Double tempMin;
    private Double tempMax;
    private Double tempAvg;
    private Double tempCnt;
    private boolean presence;
    private Double presenceCnt;

    public JsonView() {
    }

    public JsonView(LocalDateTime timeSlotStart, String location, Double tempMin, Double tempMax, Double tempAvg, Double tempCnt, boolean presence, Double presenceCnt) {
        this.timeSlotStart = timeSlotStart;
        this.location = location;
        this.tempMin = tempMin;
        this.tempMax = tempMax;
        this.tempAvg = tempAvg;
        this.tempCnt = tempCnt;
        this.presence = presence;
        this.presenceCnt = presenceCnt;
    }

    public LocalDateTime getTimeSlotStart() {
        return timeSlotStart;
    }

    public void setTimeSlotStart(LocalDateTime timeSlotStart) {
        this.timeSlotStart = timeSlotStart;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public Double getTempMin() {
        return tempMin;
    }

    public void setTempMin(Double tempMin) {
        this.tempMin = tempMin;
    }

    public Double getTempMax() {
        return tempMax;
    }

    public void setTempMax(Double tempMax) {
        this.tempMax = tempMax;
    }

    public Double getTempAvg() {
        return tempAvg;
    }

    public void setTempAvg(Double tempAvg) {
        this.tempAvg = tempAvg;
    }

    public Double getTempCnt() {
        return tempCnt;
    }

    public void setTempCnt(Double tempCnt) {
        this.tempCnt = tempCnt;
    }

    public boolean isPresence() {
        return presence;
    }

    public void setPresence(boolean presence) {
        this.presence = presence;
    }

    public Double getPresenceCnt() {
        return presenceCnt;
    }

    public void setPresenceCnt(Double presenceCnt) {
        this.presenceCnt = presenceCnt;
    }

    @Override
    public String toString() {
        return "com.github.alexsni.psyquation.views.JsonView{" +
                "timeSlotStart=" + timeSlotStart +
                ", location='" + location + '\'' +
                ", tempMin=" + tempMin +
                ", tempMax=" + tempMax +
                ", tempAvg=" + tempAvg +
                ", tempCnt=" + tempCnt +
                ", presence=" + presence +
                ", presenceCnt=" + presenceCnt +
                '}';
    }
}
