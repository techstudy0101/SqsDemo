package com.techstudy.SqsDemo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@RestController
public class MessageController {

    @Autowired
    private SQSService sqsService;

    @Autowired
    private SQSBufferedService sqsBufferedService;

    @GetMapping("/normalClient/standard/single/{message}")
    public String sendSingleMessage(@PathVariable("message") String message) {
        sqsService.sendSingleMessage(message);
        return message + " sent";
    }

    @GetMapping("/normalClient/standard/batch/{message}")
    public String sendBatchMessages(@PathVariable("message") String message) {
        sqsService.sendBatchMessages(message);
        return message + " sent";
    }

    @GetMapping("/normalClient/fifo/single/{message}")
    public String sendFifoSingleMessage(@PathVariable("message") String message, @RequestParam String messageGroupId) {
        sqsService.sendFIFOSingleMessage(message, SQSConfig.getDefaultGroupId(messageGroupId));
        return message + " sent";
    }

    @GetMapping("/normalClient/fifo/batch/{message}")
    public String sendFifoBatchMessage(@PathVariable("message") String message, @RequestParam String messageGroupId) throws ExecutionException, InterruptedException {
        sqsService.sendFIFOBatchMessages(message, SQSConfig.getDefaultGroupId(messageGroupId));
        return message + " sent";
    }

    @GetMapping("/bufferedClient/standard/single/{message}")
    public String sendBufferedSingleMessage(@PathVariable("message") String message) {
        sqsBufferedService.sendSingleMessage(message);
        return message + " sent";
    }

    @GetMapping("/bufferedClient/standard/batch/{message}")
    public String sendBufferBatchMessage(@PathVariable("message") String message) throws ExecutionException, InterruptedException {
        sqsBufferedService.sendBatchMessages(message);
        return message + " sent";
    }

    @GetMapping("/bufferedClient/fifo/single/{message}")
    public String sendBufferedFifoSingleMessage(@PathVariable("message") String message, @RequestParam String messageGroupId) {
        sqsBufferedService.sendFIFOSingleMessage(message, SQSConfig.getDefaultGroupId(messageGroupId));
        return message + " sent";
    }

    @GetMapping("/bufferedClient/fifo/batch/{message}")
    public String sendBufferedFifoBatchMessage(@PathVariable("message") String message, @RequestParam String messageGroupId) throws ExecutionException, InterruptedException {
        sqsBufferedService.sendFIFOBatchMessages(message, SQSConfig.getDefaultGroupId(messageGroupId));
        return message + " sent";
    }

    @GetMapping("receive/bufferedClient")
    public String receiveBufferedMessages() throws ExecutionException, InterruptedException {
        ReceiverService.receiveBufferedMessages(SQSConfig.bufferedSqs);
        return "received";
    }

}
