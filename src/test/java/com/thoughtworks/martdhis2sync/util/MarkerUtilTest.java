package com.thoughtworks.martdhis2sync.util;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static com.thoughtworks.martdhis2sync.util.MarkerUtil.CATEGORY_INSTANCE;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
public class MarkerUtilTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    private MarkerUtil markerUtil;

    @Before
    public void setUp() throws Exception {
        markerUtil = new MarkerUtil();
        setValuesForMemberFields(markerUtil, "jdbcTemplate", jdbcTemplate);
    }

    @Test
    public void shouldUpdateMarkerTable() {
        String programName = "HTS Service";
        String category = CATEGORY_INSTANCE;
        String date = "292269055-12-02 22:17:04";

        String sql = String.format("UPDATE marker SET last_synced_date = '%s' WHERE program_name = '%s' AND category = '%s'",
                date, programName, category);

        when(jdbcTemplate.update(sql)).thenReturn(1);

        markerUtil.updateMarkerEntry(programName, category, date);

        verify(jdbcTemplate, times(1)).update(sql);
    }

    @Test
    public void shouldReturnMinDateValueWhenSqlResponseIsNull() {
        String expected = "Sun Dec 02 22:17:04 IST 292269055";
        String programName = "HTS Service";
        String category = CATEGORY_INSTANCE;
        Map<String, Object> syncedDate = new HashMap<>();
        syncedDate.put("last_synced_date", null);
        String sql = String.format("SELECT last_synced_date FROM marker WHERE program_name='%s' AND category='%s'",
                programName, category);

        when(jdbcTemplate.queryForList(sql)).thenReturn(Collections.singletonList(syncedDate));

        Date lastSyncedDate = markerUtil.getLastSyncedDate(programName, category);

        Assert.assertEquals(expected, lastSyncedDate.toString());
    }

    @Test
    public void shouldReturnDateObjectFromSqlResponse() {
        String expected = "Sun Dec 02 22:17:04 IST 2018";
        String programName = "HTS Service";
        String category = CATEGORY_INSTANCE;
        Map<String, Object> syncedDate = new HashMap<>();
        syncedDate.put("last_synced_date", "2018-12-02 22:17:04");
        String sql = String.format("SELECT last_synced_date FROM marker WHERE program_name='%s' AND category='%s'",
                programName, category);

        when(jdbcTemplate.queryForList(sql)).thenReturn(Collections.singletonList(syncedDate));

        Date lastSyncedDate = markerUtil.getLastSyncedDate(programName, category);

        Assert.assertEquals(expected, lastSyncedDate.toString());
    }
}