package ru.sentinelcredit.callsync.model;

import lombok.Data;

@Data
public final class CampConType {
    private String campConId;
    private String timezoneId;
    private Integer chainId;
    private String dmStrat;
}
