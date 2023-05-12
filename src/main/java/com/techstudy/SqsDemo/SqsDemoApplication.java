package com.techstudy.SqsDemo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SqsDemoApplication {

    @Autowired
    private ReceiverService receiverService;

    public static void main(String[] args) {
        SpringApplication.run(SqsDemoApplication.class, args);
//        ReceiverService.receiveMessage();
//        ReceiverService.receiveDeadLetterMessage();
        ReceiverService.receiveMessageInBatch();
    }

}
