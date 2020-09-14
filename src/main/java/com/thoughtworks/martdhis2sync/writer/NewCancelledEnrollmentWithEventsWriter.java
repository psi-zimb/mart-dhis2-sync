package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.model.ProcessedTableRow;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import com.thoughtworks.martdhis2sync.util.MarkerUtil;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@StepScope
public class NewCancelledEnrollmentWithEventsWriter  extends NewEnrollmentWithEventsWriter implements ItemWriter<ProcessedTableRow> {

    @Override
    public void write(List<? extends ProcessedTableRow> tableRows) throws Exception {
        processWrite(tableRows);
        EnrollmentUtil.updateMarker(markerUtil, programName, MarkerUtil.CATEGORY_NEW_CANCELLED_ENROLLMENT);
    }
}