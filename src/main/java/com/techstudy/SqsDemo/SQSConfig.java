package com.techstudy.SqsDemo;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient;
import com.amazonaws.services.sqs.buffered.QueueBufferConfig;

public class SQSConfig {

    public static String region = "us-east-1";
    public static String queueName = "DemoQueue";
    public static final String endpoint = "https://sqs.us-east-1.amazonaws.com/594301834106/DemoQueue";
    public static final String deadLetterEndpoint = "https://sqs.us-east-1.amazonaws.com/594301834106/DemoQueueDeadLetterQueue";
    public static final String deadLetterName = "DemoQueueDeadLetterQueue";
    public static final String groupIdPrefix = "groupId_test_";
    public static final String defaultGroupId = "defaultGroupId";
    public static AmazonSQS sqs;
    public static AmazonSQS deadLetterSQS;
    public static AmazonSQSAsync bufferedSqs;


    public static String getDefaultGroupId(String groupId) {
        return groupId == null ? defaultGroupId : groupId;
    }

    public static void init() {
        initSimpleClient();
        initBufferedClient();
        initDeadLetterSimpleClient();
    }

    private static void initSimpleClient() {
        AwsClientBuilder.EndpointConfiguration configuration =
                new AwsClientBuilder.EndpointConfiguration(endpoint, region);

        AWSStaticCredentialsProvider provider = new AWSStaticCredentialsProvider(getCreds());

        sqs = AmazonSQSClientBuilder
                .standard()
                .withEndpointConfiguration(configuration)
                .withCredentials(provider)
                .build();
        String url = sqs.getQueueUrl(queueName).getQueueUrl();
        System.out.println("url = " + url);
    }

    private static void initDeadLetterSimpleClient() {
        AwsClientBuilder.EndpointConfiguration configuration =
                new AwsClientBuilder.EndpointConfiguration(deadLetterEndpoint, region);

        AWSStaticCredentialsProvider provider = new AWSStaticCredentialsProvider(getCreds());
        deadLetterSQS = AmazonSQSClientBuilder
                .standard()
                .withEndpointConfiguration(configuration)
                .withCredentials(provider)
                .build();
        String url = sqs.getQueueUrl(deadLetterName).getQueueUrl();
        System.out.println("url = " + url);
    }

    public static AWSCredentials getCreds() {
        AWSCredentials creds = new BasicAWSCredentials(AWSKeys.accessKey, AWSKeys.secretKey);
        return creds;
    }

    public static void initBufferedClient() {
        final AmazonSQSAsync sqsAsync = new AmazonSQSAsyncClient(SQSConfig.getCreds());

        final QueueBufferConfig config = new QueueBufferConfig()
                .withMaxBatchOpenMs(200)
                .withMaxBatchSize(10)
                .withMaxInflightOutboundBatches(20)
                .withMaxInflightReceiveBatches(20)
                .withMaxDoneReceiveBatches(100);

        config.setLongPoll(true);

        bufferedSqs = new AmazonSQSBufferedAsyncClient(sqsAsync, config);
    }


}
