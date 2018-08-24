package com.thoughtworks.martdhis2sync.reader;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.jdbc.core.ColumnMapRowMapper;

import javax.sql.DataSource;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@RunWith(PowerMockRunner.class)
@PrepareForTest(MappingReader.class)
public class MappingReaderTest {

    @Mock
    private DataSource dataSource;

    @Mock
    private JdbcCursorItemReader jdbcCursorItemReader;

    @Mock
    private ColumnMapRowMapper columnMapRowMapper;

    private MappingReader mappingReader;

    @Before
    public void setUp() throws Exception {
        mappingReader = new MappingReader();
        setValuesForMemberFields(mappingReader, "dataSource", dataSource);
    }

    @Test
    public void shouldReturnReaderWithDetailsBasedOnInput() throws Exception {
        String lookupTable = "patient_identifier";
        String sql = "SELECT * FROM patient_identifier";

        whenNew(JdbcCursorItemReader.class).withNoArguments().thenReturn(jdbcCursorItemReader);
        whenNew(ColumnMapRowMapper.class).withNoArguments().thenReturn(columnMapRowMapper);
        doNothing().when(jdbcCursorItemReader).setDataSource(dataSource);
        doNothing().when(jdbcCursorItemReader).setSql(sql);
        doNothing().when(jdbcCursorItemReader).setRowMapper(columnMapRowMapper);

        JdbcCursorItemReader<Map<String, Object>> actual = mappingReader.get(lookupTable);

        assertEquals(jdbcCursorItemReader, actual);

        verify(jdbcCursorItemReader, times(1)).setDataSource(dataSource);
        verify(jdbcCursorItemReader, times(1)).setSql(sql);
        verify(jdbcCursorItemReader, times(1)).setRowMapper(columnMapRowMapper);

    }
}