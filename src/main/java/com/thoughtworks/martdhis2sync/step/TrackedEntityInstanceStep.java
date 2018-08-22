package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.processor.TrackedEntityInstanceProcessor;
import com.thoughtworks.martdhis2sync.reader.MappingReader;
import com.thoughtworks.martdhis2sync.writer.TrackedEntityInstanceWriter;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


@Component
public class TrackedEntityInstanceStep {

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private MappingReader mappingReader;

    @Autowired
    private ObjectFactory<TrackedEntityInstanceProcessor> processorObjectFactory;

    @Autowired
    private TrackedEntityInstanceWriter trackedEntityInstanceWriter;

    public Step get(String lookupTable, Object mappingObj) {
        return stepBuilderFactory.get("TrackedEntityInstanceStep")
                .chunk(500)
                .reader(mappingReader.get(lookupTable))
                .processor(getProcessor(mappingObj))
                .writer(trackedEntityInstanceWriter)
                .build();
    }

    private TrackedEntityInstanceProcessor getProcessor(Object mappingObj) {
        TrackedEntityInstanceProcessor processor = processorObjectFactory.getObject();
        processor.setMappingObj(mappingObj);

        return processor;
    }
}
