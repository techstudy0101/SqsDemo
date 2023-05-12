package com.techstudy.SqsDemo;

import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.model.*;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class ReceiverService {

    public ReceiverService() {

    }

    public static void receiveMessage() {
        Runnable runnable = new Runnable() {
            @Override
            public void run() {

                while (true) {
                    System.out.println("receiving start");
                    ReceiveMessageRequest request = new ReceiveMessageRequest();
                    request.setQueueUrl(SQSConfig.endpoint);
                    request.setMaxNumberOfMessages(10);
                    request.setWaitTimeSeconds(20);
                    List<Message> messageList = SQSConfig.sqs.receiveMessage(request)
                            .getMessages();

                    if (messageList.isEmpty()) {
                        System.out.println("receiving is empty.");
                    } else {
                        System.out.println("Received messages count = " + messageList.size());
                    }

                    messageList.stream()
                            .forEach(m ->
                                    {
                                        m.getAttributes().entrySet().stream().forEach(e -> System.out.println("receiving " + e.getKey() + ", " + e.getValue()));
                                        System.out.println(System.currentTimeMillis() + " receiving = " + m.getMessageId() + ", " + m.getBody());
                                        SQSConfig.sqs.deleteMessage(SQSConfig.endpoint, m.getReceiptHandle());
                                    }
                            );
                }
            }
        };
        Thread thread = new Thread(runnable);
        thread.start();

    }

    public static void receiveDeadLetterMessage() {
        Runnable runnable = new Runnable() {
            @Override
            public void run() {

                while (true) {
                    System.out.println("receiveDeadLetterMessage start");
                    ReceiveMessageRequest request = new ReceiveMessageRequest();
                    request.setQueueUrl(SQSConfig.deadLetterEndpoint);
                    request.setWaitTimeSeconds(20);
                    List<Message> messageList = SQSConfig.sqs.receiveMessage(request)
                            .getMessages();
                    if (messageList.isEmpty()) {
                        System.out.println("receiveDeadLetterMessage is empty.");
                    }
                    messageList.stream()
                            .forEach(m ->
                                    {
                                        m.getAttributes().entrySet().stream().forEach(e -> System.out.println("receiveDeadLetterMessage " + e.getKey() + ", " + e.getValue()));
                                        System.out.println(System.currentTimeMillis() + " receiveDeadLetterMessage = " + m.getMessageId() + ", " + m.getBody());
                                        SQSConfig.deadLetterSQS.deleteMessage(SQSConfig.deadLetterEndpoint, m.getReceiptHandle());
                                    }
                            );
                }
            }
        };
        Thread thread = new Thread(runnable);
        thread.start();

    }

    public static void receiveMessageInBatch() {
        receiveMessageInBatchAsync(SQSConfig.bufferedSqs);
    }

    public static void receiveMessageInBatchAsync(AmazonSQSAsync bufferedSqs) {
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                while (true) {
                    receiveBufferedMessages(bufferedSqs);
                }
            }
        };

        Thread thread = new Thread(runnable);
        thread.start();
    }

    public static void receiveBufferedMessages(AmazonSQSAsync bufferedSqs) {
        System.out.println("receiveMessageInBatchAsync...");
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
        receiveMessageRequest.setQueueUrl(SQSConfig.endpoint);
        ReceiveMessageResult receiveMessageResult = bufferedSqs.receiveMessage(receiveMessageRequest);
        List<Message> messages = receiveMessageResult.getMessages();

        DeleteMessageBatchRequest deleteMessageBatchRequest = new DeleteMessageBatchRequest();
        List<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntries = new ArrayList<>();
        System.out.println("Messages Received = " + (messages.size()));
        messages.stream().forEach(
                result -> {
                    printSqsMessage(result);
                    DeleteMessageBatchRequestEntry deleteEntry = new DeleteMessageBatchRequestEntry();
                    deleteEntry.setId("ReceiptId" + result.getMessageId());
                    deleteEntry.setReceiptHandle(result.getReceiptHandle());
                    deleteMessageBatchRequestEntries.add(deleteEntry);
                }
        );

        deleteMessageBatchRequest.setEntries(deleteMessageBatchRequestEntries);
        deleteMessageBatchRequest.setQueueUrl(SQSConfig.endpoint);


        if (deleteMessageBatchRequest.getEntries() != null &&
                !deleteMessageBatchRequest.getEntries().isEmpty()) {
            System.out.println("Delete batch size = " + (deleteMessageBatchRequest.getEntries().size()));
            bufferedSqs.deleteMessageBatchAsync(deleteMessageBatchRequest);
        }
    }

    public static void printSqsMessage(Message message) {
        System.out.println(message.getMessageId() + " " + message.getBody());
    }

}
