package com.amazonaws.samples;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.AmazonRekognitionException;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.S3Object;
import com.amazonaws.services.rekognition.model.DetectTextRequest;
import com.amazonaws.services.rekognition.model.DetectTextResult;
import com.amazonaws.services.rekognition.model.TextDetection;
import java.util.List;

public class InstanceB {
	

    public static void main(String[] args) throws IOException {
        String myQueueUrl = "https://sqs.us-east-1.amazonaws.com/802645029462/myQueue.fifo";
        String bucket = "njit-cs-643";
        File file = new File("/home/ec2-user/result.txt");
        
		 //Create the file
        if (file.createNewFile()) {
        	System.out.println("File created: " + file.getName());
        } else {
        	System.out.println("File already exists.");
        }
		   
		FileWriter writer = new FileWriter(file);
		writer.write("Images with Cars and Text: \n");

        List<Message> messages;
        boolean isEnd = false;
        System.out.println("Receiving");
        String photo=null;
    	AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.defaultClient();
        
        while (true) {
        	AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                    .withQueueUrl(myQueueUrl);
                    //.withWaitTimeSeconds(2);

            receiveMessageRequest.setMaxNumberOfMessages(1);
	        messages = sqs.receiveMessage(receiveMessageRequest).getMessages();

	        if (!messages.isEmpty()) {
	        	
		        for (final Message message : messages) {
		            System.out.println("Processing Photo: " + message.getBody());
		            photo = message.getBody();
		            if (message.getBody().equals("-1")){
		            	isEnd = true;
		            }
		            
		        }
		        System.out.println("Deleting Photo from Queue: " + photo);
		        String messageReceiptHandle = messages.get(0).getReceiptHandle();
		        sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageReceiptHandle));
		        if (isEnd) {
		        	break;
		        }
		
	            DetectTextRequest request = new DetectTextRequest()
	                    .withImage(new Image()
	                    .withS3Object(new S3Object()
	                    .withName(photo)
	                    .withBucket(bucket)));
	          
	
	            try {
	               DetectTextResult result = rekognitionClient.detectText(request);
	               List<TextDetection> textDetections = result.getTextDetections();
	
	               if (textDetections.isEmpty()) {
	            	   continue;
	               }
	               writer.write("Image: " + photo + "\n");
	               //System.out.println("Detected lines and words for " + photo);
	               for (TextDetection text: textDetections) {
	            	   writer.write("Detected Text: " + text.getDetectedText() + "\n");
	                   //System.out.println("Detected: " + text.getDetectedText());
	                   //System.out.println();
	               }
	            } catch(AmazonRekognitionException e) {
	               e.printStackTrace();
	            }
	            writer.write("\n\n");
	        }
         }
        writer.close();
        System.out.println("Instance B Done");

    }

}
