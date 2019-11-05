package com.thoughtworks.martdhis2sync.util;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.model.Event;
import com.thoughtworks.martdhis2sync.model.EventTracker;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.*;

public class EventUtil {

    public static Date date = new Date(Long.MIN_VALUE);

    @Getter
    private static List<EventTracker> existingEventTrackers = new ArrayList<>();

    @Getter
    @Setter
    private static List<String> elementsOfTypeDateTime;

    @Getter
    @Setter
    private static List<String> elementsOfTypeDate;

    @Getter
    private static List<EventTracker> newEventTrackers = new ArrayList<>();

    public static List<EventTracker> eventsToSaveInTracker = new ArrayList<>();

    private static Logger logger = LoggerFactory.getLogger(EventUtil.class.getName());

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

    public static void updateLatestEventDateCreated(String dateCreated) {
        Date bahmniDateCreated = getDateFromString(dateCreated, DATEFORMAT_WITH_24HR_TIME);
        if (date.compareTo(bahmniDateCreated) < 1) {
            date = bahmniDateCreated;
        }
    }


    public static Map<String, String> getDataValues(JsonObject tableRow, JsonObject mapping) {
        Set<String> keys = tableRow.keySet();
        Map<String, String> dataValues = new HashMap<>();

        for (String key : keys) {
            JsonElement dataElement = mapping.get(key);
            if (hasValue(dataElement)) {
                String value = tableRow.get(key).getAsString();
                String dataElementInStringFormat = dataElement.getAsString();
                dataValues.put(
                        dataElementInStringFormat,
                        changeFormatIfDate(dataElementInStringFormat, value)
                );
            }
        }
        return dataValues;
    }

    private static String changeFormatIfDate(String elementId, String value) {
        logger.info("Event Processor : changeFormatIfDate: " + elementId + ", " + value);
        if (getElementsOfTypeDate() != null && getElementsOfTypeDate().contains(elementId)) {
            String result =  BatchUtil.getDateOnly(value);
            logger.info("Event Processor : (Date): " + result);
            return result;
        } else {
            if (getElementsOfTypeDateTime() != null && getElementsOfTypeDateTime().contains(elementId)) {
                String result = getFormattedDateString(
                        value,
                        DATEFORMAT_WITH_24HR_TIME,
                        DHIS_ACCEPTABLE_DATEFORMAT
                );
                logger.info("Event Processor : (DateTime): " + result);
                return result;
            }
        }

        return value;
    }

    public static List<Event> placeNewEventsFirst(List<Event> events) {
        List<Event> newEvents = new LinkedList<>();
        List<Event> updateEvents = new LinkedList<>();

        events.forEach(event -> {
            if (StringUtils.isEmpty(event.getEvent())) {
                newEvents.add(event);
            } else {
                updateEvents.add(event);
            }
        });

        return Stream.of(newEvents, updateEvents)
                .flatMap(List:: stream)
                .collect(Collectors.toList());
    }
}
