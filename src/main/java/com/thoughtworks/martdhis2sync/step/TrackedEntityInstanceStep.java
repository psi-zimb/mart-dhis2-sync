package com.thoughtworks.martdhis2sync.step;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class TrackedEntityInstanceStep {

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    public Step getStep() {
        return stepBuilderFactory.get("TrackedEntityInstanceStep")
                .chunk(10)
                .reader(new DummyItemReader())
                .writer(new DummyItemWriter())
                .build();
    }

    private class DummyItemReader implements ItemReader {
        @Override
        public Object read() throws Exception {
            return null;
        }
    }

    private class DummyItemWriter implements ItemWriter {
        @Override
        public void write(List items) throws Exception {

        }
    }

}
