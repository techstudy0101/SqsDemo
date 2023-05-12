package com.techstudy.SqsDemo;

import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class SQSService {

    public SQSService() {
        SQSConfig.init();
    }

    public void sendSingleMessage(String message) {
        SendMessageRequest sendMessageRequest = new SendMessageRequest();
        sendMessageRequest.setQueueUrl(SQSConfig.endpoint);
        String sendMessage = message;
        sendMessageRequest.setMessageBody(sendMessage);
        SQSConfig.sqs.sendMessage(sendMessageRequest);
    }

    public void sendFIFOSingleMessage(String message, String messageGroupId) {
        SendMessageRequest sendMessageRequest = new SendMessageRequest();
        sendMessageRequest.setQueueUrl(SQSConfig.endpoint);
        String sendMessage = message;
        sendMessage = sendMessage + "_groupId=" + messageGroupId;
        sendMessageRequest.setMessageGroupId(messageGroupId);
        sendMessageRequest.setMessageBody(sendMessage);
        sendMessageRequest.setMessageDeduplicationId(UUID.randomUUID().toString());
        SQSConfig.sqs.sendMessage(sendMessageRequest);
    }

    public void sendBatchMessages(String message) {

        SendMessageBatchRequest sqsBatchRequest = new SendMessageBatchRequest();
        List<SendMessageBatchRequestEntry> entryList = Stream.iterate(1, i -> i < 10, i -> i + 1)
                .map(numb -> {
                    String messageId = "id-" + numb;
                    String sendMessage = message;
                    sendMessage = sendMessage + "__" + messageId;
                    SendMessageBatchRequestEntry entry = new SendMessageBatchRequestEntry();
                    entry.setMessageBody(sendMessage);
                    entry.setId(messageId);
                    return entry;
                })
                .collect(Collectors.toList());

        sqsBatchRequest.setEntries(entryList);
        System.out.println("Sending...");
        SendMessageBatchResult result = SQSConfig.sqs.sendMessageBatch(SQSConfig.endpoint, entryList);

        System.out.println("Sent...");
        result.getFailed().stream()
                .forEach(fm -> System.out.println("failed " + fm.getMessage() + ", " + fm.getCode() + ", " + fm.getSenderFault()));
        result.getSuccessful().stream()
                .forEach(fm -> System.out.println("success " + fm.getMessageId()));
    }

    public void sendFIFOBatchMessages(String message, String messageGroupId) {

        SendMessageBatchRequest sqsBatchRequest = new SendMessageBatchRequest();
        sqsBatchRequest.setQueueUrl(SQSConfig.endpoint);
        List<SendMessageBatchRequestEntry> entryList = Stream.iterate(1, i -> i < 10, i -> i + 1)
                .map(numb -> {
                    String messageId = "id-" + numb;
                    String sendMessage = message;
                    sendMessage = sendMessage + "__messageGroupId=" + messageGroupId + "__id=" + messageId;
                    SendMessageBatchRequestEntry entry = new SendMessageBatchRequestEntry();
                    entry.setMessageBody(sendMessage);
                    entry.setMessageGroupId(messageGroupId);
                    entry.setMessageDeduplicationId(UUID.randomUUID().toString());
                    entry.setId(messageId);

                    return entry;
                })
                .collect(Collectors.toList());

        sqsBatchRequest.setEntries(entryList);
        System.out.println("Sending...");
        SendMessageBatchResult result = SQSConfig.sqs.sendMessageBatch(sqsBatchRequest);
        System.out.println("Sent...");
        result.getFailed().stream().forEach(fm -> System.out.println("failed " + fm.getMessage() + ", " + fm.getCode() + ", " + fm.getSenderFault()));
        result.getSuccessful().stream().forEach(fm -> System.out.println("success " + fm.getMessageId()));
    }

    private void printSendMessage(String message) {
        System.out.println("printSendMessage - " + message);
    }

}
