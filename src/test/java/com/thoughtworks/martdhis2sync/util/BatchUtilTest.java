package com.thoughtworks.martdhis2sync.util;

import com.google.gson.JsonElement;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITHOUT_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITH_24HR_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.convertResourceOutputToString;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getDateFromString;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getFormattedDateString;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.hasValue;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.removeLastChar;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@PrepareForTest({IOUtils.class, BatchUtil.class})
@RunWith(PowerMockRunner.class)
public class BatchUtilTest {

    @Before
    public void setUp() throws Exception {
        mockStatic(IOUtils.class);
    }

    @Test
    public void shouldConvertResourceOutputToString() throws Exception {
        ClassPathResource classPathResource = mock(ClassPathResource.class);
        InputStream inputStream = mock(InputStream.class);
        when(classPathResource.getInputStream()).thenReturn(inputStream);
        String expectedString = "stringEquivalentOfClassPathResource";
        when(IOUtils.toString(inputStream)).thenReturn(expectedString);

        assertEquals(expectedString, convertResourceOutputToString(classPathResource));
        verify(classPathResource, times(1)).getInputStream();
    }

    @Test
    public void shouldThrowBatchResourceException() throws Exception {
        ClassPathResource classPathResource = mock(ClassPathResource.class);
        when(classPathResource.getInputStream()).thenThrow(new IOException());

        try {
            convertResourceOutputToString(classPathResource);
        } catch (IOException e) {
            verify(classPathResource, times(1)).getInputStream();
        }
    }

    @Test
    public void shouldReturnMinDateValueWhenParseThrowsException() throws Exception {
        String expected  = "Sun Dec 02 22:17:04 IST 292269055";
        SimpleDateFormat simpleDateFormat = mock(SimpleDateFormat.class);
        whenNew(SimpleDateFormat.class).withArguments("yyyy-MM-dd kk:mm:ss").thenReturn(simpleDateFormat);
        when(simpleDateFormat.parse(null)).thenThrow(ParseException.class);

        Date actual = getDateFromString(null, DATEFORMAT_WITH_24HR_TIME);

        assertEquals(expected, actual.toString());
    }

    @Test
    public void shouldGetFormattedInExceptedFormat() {
        String givenFormat = "2018-03-12 13:00:00";
        String expectedFormat = "2018-03-12";

        String formattedDateString = getFormattedDateString(givenFormat, DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME);

        assertEquals(expectedFormat, formattedDateString);
    }

    @Test
    public void shouldReturnFalseWhenElementIsNull() {
        assertFalse(hasValue(null));
    }

    @Test
    public void shouldReturnFalseWhenElementHas2DoubleQuotes() {
        JsonElement element = new JsonElement() {
            @Override
            public JsonElement deepCopy() {
                return null;
            }

            @Override
            public String toString() {
                return "\"\"";
            }
        };

        assertFalse(hasValue(element));
    }

    @Test
    public void shouldReturnTrueWhenElementHasValue() {
        JsonElement element = new JsonElement() {
            @Override
            public JsonElement deepCopy() {
                return null;
            }

            @Override
            public String toString() {
                return "\"some value\"";
            }
        };

        assertTrue(hasValue(element));
    }

    @Test
    public void shouldReturnEmptyStringWhenGivenStringIsEmpty() {
        assertEquals("", removeLastChar(new StringBuilder()));
    }

    @Test
    public void shouldRemoveLastCharForTheGivenString() {
        assertEquals("someValu", removeLastChar(new StringBuilder("someValue")));
    }
}