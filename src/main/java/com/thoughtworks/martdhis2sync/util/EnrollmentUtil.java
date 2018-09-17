package com.thoughtworks.martdhis2sync.util;

import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.model.Enrollment;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.getUnquotedString;

public class EnrollmentUtil {
    @Getter
    private static List<Enrollment> enrollmentsList = new ArrayList<>();

    public static void addEnrollment(JsonObject tableRowJsonObject) {
        enrollmentsList.add(
                new Enrollment(tableRowJsonObject.get("enrollment_id").toString(),
                        getUnquotedString(tableRowJsonObject.get("instance_id").toString()),
                        getUnquotedString(tableRowJsonObject.get("program").toString()),
                        BatchUtil.getDateFromString(
                                getUnquotedString(tableRowJsonObject.get("enrollment_date").toString()),
                                BatchUtil.DATEFORMAT_WITHOUT_TIME),
                        getUnquotedString(tableRowJsonObject.get("status").toString().toUpperCase())));
    }

    public static void resetEnrollmentsList() {
        enrollmentsList.clear();
    }
}
