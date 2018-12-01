package com.thoughtworks.martdhis2sync.util;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.model.Event;
import com.thoughtworks.martdhis2sync.model.EventTracker;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.getUnquotedString;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.hasValue;

public class EventUtil {

    public static Date date = new Date(Long.MIN_VALUE);

    @Getter
    private static List<EventTracker> existingEventTrackers = new ArrayList<>();

    @Getter
    @Setter
    private static List<String> elementsOfTypeDateTime;

    @Getter
    private static List<EventTracker> newEventTrackers = new ArrayList<>();

    public static List<EventTracker> eventsToSaveInTracker = new ArrayList<>();

    public static void addExistingEventTracker(JsonObject tableRow) {
        existingEventTrackers.add(getEventTracker(tableRow));
    }

    public static void addNewEventTracker(JsonObject tableRow) {
        newEventTrackers.add(getEventTracker(tableRow));
    }

    private static EventTracker getEventTracker(JsonObject tableRow) {
        JsonElement eventIdElement = tableRow.get("event_id");
        String eventId = hasValue(eventIdElement) ? getUnquotedString(eventIdElement.toString()) : "";

        return new EventTracker(
                eventId,
                getUnquotedString(tableRow.get("instance_id").toString()),
                getUnquotedString(tableRow.get("program").toString()),
                getUnquotedString(tableRow.get("event_unique_id").toString()),
                getUnquotedString(tableRow.get("program_stage").toString())
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

    public static List<EventTracker> getEventTrackers(List<Event> events) {
        return events.stream().map(event -> new EventTracker(
                event.getEvent(),
                event.getTrackedEntityInstance(),
                event.getProgram(),
                event.getEventUniqueId(),
                event.getProgramStage()
            )
        ).collect(Collectors.toList());
    }
}
