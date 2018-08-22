package com.thoughtworks.martdhis2sync;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing
public class MartDhis2SyncApplication {

    public static void main(String[] args) {
        SpringApplication.run(MartDhis2SyncApplication.class, args);
    }
}
