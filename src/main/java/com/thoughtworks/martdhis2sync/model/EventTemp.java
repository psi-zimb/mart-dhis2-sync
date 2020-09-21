package com.thoughtworks.martdhis2sync.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EventTemp {
    public static final String STATUS_COMPLETED = "COMPLETED";

    @JsonProperty("event")
    private String event;
    @JsonProperty("trackedEntityInstance")
    private String trackedEntityInstance;
    @JsonProperty("enrollment")
    private String enrollment;
    @JsonProperty("program")
    private String program;
    @JsonProperty("programStage")
    private String programStage;
    @JsonProperty("orgUnit")
    private String orgUnit;
    @JsonProperty("eventDate")
    private String eventDate;
    @JsonProperty("status")
    private String status;
    @JsonProperty("eventUniqueId")
    private String eventUniqueId;
    @JsonProperty("dataValues")
    private List<Map<String, String>> dataValues;
}
