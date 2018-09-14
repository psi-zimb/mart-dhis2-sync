package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.processor.ProgramEnrollmentProcessor;
import com.thoughtworks.martdhis2sync.reader.MappingReader;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ProgramEnrollmentStep {

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private MappingReader mappingReader;

    @Autowired
    private ProgramEnrollmentProcessor processor;
    
    public Step get(String lookupTable) {
        return stepBuilderFactory.get("ProgramEnrollmentStep")
                .chunk(500)
                .reader(mappingReader.getEnrollmentReader(lookupTable))
                .processor(processor)
                .build();
    }
}
