package com.amazonaws.samples;


import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.Region;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.AmazonRekognitionException;
import com.amazonaws.services.rekognition.model.DetectLabelsRequest;
import com.amazonaws.services.rekognition.model.DetectLabelsResult;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.Label;
import com.amazonaws.services.rekognition.model.S3Object;
import java.util.List;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.AmazonClientException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;


import java.io.IOException;

public class InstanceA {

    public static void main(String[] args) throws IOException {
    	
    	//----------------------------------------------- Read S3 Bucket  -----------------------------------------------
    	Regions clientRegion = Regions.US_EAST_1;
        String bucketName = "njit-cs-643";
        String image_names[] = new String[10];
        int i = 0;

        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    //.withCredentials(new ProfileCredentialsProvider())
                    .withRegion(clientRegion)
                    .build();

            ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName).withMaxKeys(2);
            ListObjectsV2Result result;

            do {
                result = s3Client.listObjectsV2(req);

                for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                	image_names[i] = objectSummary.getKey();
                    //System.out.printf(" - %s\n", objectSummary.getKey());
                	i = i + 1;
                }

                String token = result.getNextContinuationToken();
                req.setContinuationToken(token);
            } while (result.isTruncated());
        } catch (AmazonServiceException e) {
            e.printStackTrace();
        } catch (SdkClientException e) {
            e.printStackTrace();
        }
        
        System.out.println("Images in S3 Bucket: ");
        for (String element: image_names) {
            System.out.print(element+"  ");
        }
        System.out.println("\n");
        
        //----------------------------------------------- Amazon Rekognition -----------------------------------------------
        //String photo = "8.jpg";

        AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.defaultClient();
        final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
        String myQueueUrl = "https://sqs.us-east-1.amazonaws.com/802645029462/myQueue.fifo";

        
        for (String photo: image_names) {
            float conf = (float) 0.00;

	        DetectLabelsRequest request = new DetectLabelsRequest()
	             .withImage(new Image()
	             .withS3Object(new S3Object()
	             .withName(photo).withBucket(bucketName)));
	
	        try {
	           DetectLabelsResult result = rekognitionClient.detectLabels(request);
	           List <Label> labels = result.getLabels();
	
	           System.out.println("Rekognition: " + photo);
	           
	           for (Label label: labels) {
	        	   if (label.getName().equals("Car")) {
	        		   conf = label.getConfidence();
	        		   System.out.println("Car" + ": " + conf);
	        	   }
	           }
	           if (conf == 0) {
	        	   System.out.println("No Car Detected");
	           }
	        } catch(AmazonRekognitionException e) {
	           e.printStackTrace();
	        }
	        
	        //----------------------------------------------- SQS -----------------------------------------------
	        if (conf > 90.0) {
	        	SendMessageRequest sendMessageRequest = new SendMessageRequest(myQueueUrl, photo);
	        	sendMessageRequest.setMessageGroupId("messageGroup1");
	        	sendMessageRequest.setMessageDeduplicationId(photo);

	        	SendMessageResult sendMessageResult = sqs.sendMessage(sendMessageRequest);
	        	System.out.println("SendMessage succeed for "+photo);
	        }
	        
        }
        
        SendMessageRequest sendMessageRequest = new SendMessageRequest(myQueueUrl, "-1");
    	sendMessageRequest.setMessageGroupId("messageGroup1");
    	sendMessageRequest.setMessageDeduplicationId("-1");

    	SendMessageResult sendMessageResult = sqs.sendMessage(sendMessageRequest);
    	System.out.println("SendMessage succeed for -1\n");
    	
    	System.out.println("Instance A Done");
        
    }
}


