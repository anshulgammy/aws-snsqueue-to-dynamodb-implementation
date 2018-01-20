package com.aws.pushtosns;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;

public class PushToSns {
    public static String TOPIC_ARN;

    public static String ACCESS_KEY;

    public static String SECURITY_KEY;

    public static final String SEND_FAILED = "Sending Message Failed";

    public static final String SEND_SUCCESS = "Sending Message Success!!!";

    final static Logger log = Logger.getLogger(PushToSns.class);

    public static void main(String[] args) {
        log.info("Message ID : " + pushMessageToSns());
    }

    public static String pushMessageToSns() {
        log.info("Starting push pushMessageToSns() method");
        // Loading details from properties file
        final Properties properties = new Properties();
        try {
            // load a properties file
            properties.load(new FileInputStream("src/main/resources/config.properties"));
            // get the values from the properties file
            TOPIC_ARN = properties.getProperty("TOPIC_ARN");
            ACCESS_KEY = properties.getProperty("ACCESS_KEY");
            SECURITY_KEY = properties.getProperty("SECURITY_KEY");

        } catch (IOException ex) {
            log.error("Exception Occurred while fetching details from config.properties file.", ex);
            return SEND_FAILED;
        } finally {
            properties.clear();
        }
        // created a new SNS client and set endpoint
        if (null != TOPIC_ARN && null != ACCESS_KEY && null != SECURITY_KEY) {
            final BasicAWSCredentials awsCredentials = new BasicAWSCredentials(ACCESS_KEY, SECURITY_KEY);
            // Currently region is set as US_EAST_2
            final AmazonSNS snsClient = AmazonSNSClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                    .withRegion(Regions.US_EAST_2)
                    .build();
            String jsonString = null;
            try {
                final InputStream inputStream = new FileInputStream("src/main/resources/jsontext.json");
                jsonString = IOUtils.toString(inputStream, "UTF-8");
                log.info("The JSON String being sent is : " + jsonString);
            } catch (Exception e) {
                log.error(e);
            }
            // publish JSON string to an SNS topic
            if (null != jsonString) {
                PublishRequest publishRequest = new PublishRequest(TOPIC_ARN, jsonString);
                PublishResult publishResult = snsClient.publish(publishRequest);
                log.info(SEND_SUCCESS);
                return publishResult.getMessageId();
            }
        }
        return SEND_FAILED;
    }
}
