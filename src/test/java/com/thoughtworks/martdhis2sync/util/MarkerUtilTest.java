package com.thoughtworks.martdhis2sync.util;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.jdbc.core.JdbcTemplate;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
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
        String syncedDate = "2018-06-19 13:53:28.000000";
        String programName = "HTS Service";
        String category = "instance";

        String sql = String.format("UPDATE marker SET last_synced_date = '%s' WHERE program_name = '%s' AND category = '%s'",
                syncedDate, programName, category);

        when(jdbcTemplate.update(sql)).thenReturn(1);

        markerUtil.updateMarkerEntry(syncedDate, programName, category);

        verify(jdbcTemplate, times(1)).update(sql);
    }
}