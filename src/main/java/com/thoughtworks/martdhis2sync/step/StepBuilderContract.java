package com.thoughtworks.martdhis2sync.step;

import org.springframework.batch.core.Step;

public interface StepBuilderContract {

    Step get(String lookupTable, String programName, Object mappingObj);
}
