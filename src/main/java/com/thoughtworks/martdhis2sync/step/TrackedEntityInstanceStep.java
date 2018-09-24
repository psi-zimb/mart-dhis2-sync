package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.processor.TrackedEntityInstanceProcessor;
import com.thoughtworks.martdhis2sync.reader.MappingReader;
import com.thoughtworks.martdhis2sync.util.MarkerUtil;
import com.thoughtworks.martdhis2sync.util.TEIUtil;
import com.thoughtworks.martdhis2sync.writer.TrackedEntityInstanceWriter;
import org.springframework.batch.core.Step;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static com.thoughtworks.martdhis2sync.util.MarkerUtil.CATEGORY_INSTANCE;


@Component
public class TrackedEntityInstanceStep implements StepBuilderContract {

    @Autowired
    private MappingReader mappingReader;

    @Autowired
    private ObjectFactory<TrackedEntityInstanceProcessor> processorObjectFactory;

    @Autowired
    private TrackedEntityInstanceWriter writer;

    @Autowired
    private MarkerUtil markerUtil;

    @Autowired
    private StepFactory stepFactory;

    private static final String TEI_STEP_NAME = "Tracked Entity Step";

    @Override
    public Step get(String lookupTable, String programName, Object mappingObj) {
        TEIUtil.resetPatientTEIUidMap();
        TEIUtil.date = markerUtil.getLastSyncedDate(programName, CATEGORY_INSTANCE);

        return stepFactory.build(TEI_STEP_NAME, mappingReader.getInstanceReader(lookupTable, programName), getProcessor(mappingObj), writer);
    }

    private TrackedEntityInstanceProcessor getProcessor(Object mappingObj) {
        TrackedEntityInstanceProcessor processor = processorObjectFactory.getObject();
        processor.setMappingObj(mappingObj);

        return processor;
    }
}
