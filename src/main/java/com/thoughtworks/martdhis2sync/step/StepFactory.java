package com.thoughtworks.martdhis2sync.step;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class StepFactory {

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    protected Step build(String stepName, JdbcCursorItemReader<Map<String, Object>> reader, ItemProcessor processor, ItemWriter writer) {
        return stepBuilderFactory.get(stepName)
                .chunk(500)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    protected Step build(String stepName, Tasklet tasklet) {
        return stepBuilderFactory.get(stepName)
                .tasklet(tasklet)
                .build();
    }
}
