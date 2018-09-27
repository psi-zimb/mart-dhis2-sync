package com.thoughtworks.martdhis2sync.util;

import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.model.EventTracker;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITH_24HR_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getDateFromString;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getUnquotedString;

public class EventUtil {

    public static Date date = new Date(Long.MIN_VALUE);

    @Getter
    private static List<EventTracker> existingEventTrackers = new ArrayList<>();

    @Getter
    private static List<EventTracker> newEventTrackers = new ArrayList<>();

    public static void addExistingEventTracker(JsonObject tableRow) {
        existingEventTrackers.add(getEventTracker(tableRow));
    }

    public static void addNewEventTracker(JsonObject tableRow) {
        newEventTrackers.add(getEventTracker(tableRow));
    }

    private static EventTracker getEventTracker(JsonObject tableRow) {
        String programStartDate = getUnquotedString(tableRow.get("program_start_date").toString());
        String programUniqueId = getUnquotedString(tableRow.get("program_unique_id").toString());
        return new EventTracker(
                getUnquotedString(tableRow.get("event_id").toString()),
                getUnquotedString(tableRow.get("instance_id").toString()),
                getUnquotedString(tableRow.get("program").toString()),
                Integer.parseInt(programUniqueId),
                getDateFromString(programStartDate, DATEFORMAT_WITH_24HR_TIME)
        );
    }

    public static List<EventTracker> getEventTrackers() {
        return Stream.of(newEventTrackers, existingEventTrackers)
                .flatMap(List:: stream)
                .collect(Collectors.toList());
    }

    public static void resetEventTrackersList() {
        existingEventTrackers.clear();
        newEventTrackers.clear();
    }
}
