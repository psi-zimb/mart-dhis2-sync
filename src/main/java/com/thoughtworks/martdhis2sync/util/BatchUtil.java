package com.thoughtworks.martdhis2sync.util;

import com.google.gson.JsonElement;
import org.apache.commons.io.IOUtils;
import org.springframework.core.io.Resource;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class BatchUtil {

    private static final String DATEFORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String DATEFORMAT_WITH_24HR_TIME = "yyyy-MM-dd kk:mm:ss";
    public static final String DHIS_ACCEPTABLE_DATEFORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    public static final String DATEFORMAT_WITHOUT_TIME = "yyyy-MM-dd";
    public static final String EMPTY_STRING = "\"\"";

    public static String convertResourceOutputToString(Resource resource) throws IOException {
        return IOUtils.toString(resource.getInputStream());
    }

    public static String getUnquotedString(String string) {
        return StringUtils.replace(string, "\"", "");
    }

    public static String getQuotedString(String string) {
        return "\"" + string + "\"";
    }

    public static String GetUTCDateTimeAsString() {
        final SimpleDateFormat sdf = new SimpleDateFormat(DATEFORMAT);
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(new Date());
    }

    public static String getStringFromDate(Date date, String format) {
        SimpleDateFormat outputFormat = new SimpleDateFormat(format);
        return outputFormat.format(date);
    }

    public static Date getDateFromString(String date, String format) {
        SimpleDateFormat outputFormat = new SimpleDateFormat(format);
        try {
            return outputFormat.parse(date);
        } catch (ParseException ignored) {
            System.out.println(ignored);
        }
        return new Date(Long.MIN_VALUE);
    }

    public static String getFormattedDateString(String date, String existingFormat, String expectedFormat) {
        return getStringFromDate(getDateFromString(date, existingFormat), expectedFormat);
    }

    public static boolean hasValue(JsonElement element) {
        return (null != element && !EMPTY_STRING.equals(element.toString()));
    }

    public static String removeLastChar(StringBuilder value) {
        int length = value.length();

        return length > 0 ? value.deleteCharAt(length - 1).toString() : "";
    }

    public static String getDateOnly(String dateString) {
        return getStringFromDate(getDateFromString(dateString, DATEFORMAT_WITHOUT_TIME), DATEFORMAT_WITHOUT_TIME);
    }

    public static String getDateTime(String dateString) {
        SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATEFORMAT_WITH_24HR_TIME);
        SimpleDateFormat dateFormat = new SimpleDateFormat(DATEFORMAT_WITHOUT_TIME);
        try {
            dateTimeFormat.parse(dateString);
            return dateString;
        } catch (ParseException ignored) {
            Calendar cal = Calendar.getInstance();
            Date date = null;
            try {
                date = dateFormat.parse(dateString);
                //System.out.println(date);
                cal.setTime(date);
                cal.set(Calendar.HOUR_OF_DAY, 0);
                return dateTimeFormat.format(cal.getTime());
            } catch (ParseException e) {
                e.printStackTrace();
                return null;
            }
        }
    }


}
