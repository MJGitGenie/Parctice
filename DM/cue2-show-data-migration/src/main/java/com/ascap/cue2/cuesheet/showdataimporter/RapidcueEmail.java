package com.ascap.cue2.cuesheet.showdataimporter;
 
import static com.amazonaws.regions.Regions.US_EAST_1;
import static java.lang.System.getenv;

import java.io.File;
import java.util.Properties;
import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RapidcueEmail {
	
	private String host = null;
	private String port = null;
	private String user = null;
	private String password = null;
	private String emailRecipient = null; 

	
	private AWSSecretsManager secretsManager
    = AWSSecretsManagerClientBuilder.standard().withRegion(US_EAST_1).build(); 

	JsonNode getSecretObject(String name) throws Exception {
	GetSecretValueRequest request = new GetSecretValueRequest().withSecretId(name);
	GetSecretValueResult result = secretsManager.getSecretValue(request);
	String secretString = result.getSecretString();
	ObjectMapper objectMapper = new ObjectMapper();
	JsonFactory jsonFactory = objectMapper.getFactory();
	try {
	    JsonParser jsonParser = jsonFactory.createParser(secretString);
	    return objectMapper.readTree(jsonParser);
	
	} catch (Exception e) {
	    throw new Exception("Failed to parse secret string as JSON: " + secretString, e);
	}
	}

	public RapidcueEmail() throws Exception {
		try {
			init();
		} catch (Exception e) { 
			throw e;
		}
	} 
	
void init() throws Exception {     
        String rapidcueSecret = getenv("RAPIDCUE_SECRET_NAME");
        JsonNode jsonNode = getSecretObject(rapidcueSecret); 
             
        host = jsonNode.get("EMAIL_HOST").asText() ;   
    	port = jsonNode.get("EMAIL_PORT").asText() ;    
    	user = jsonNode.get("EMAIL_USER").asText() ;    
    	password = jsonNode.get("EMAIL_PASSWORD").asText() ;    
    	emailRecipient = jsonNode.get("EMAIL_RECIPIENT").asText() ;    
	}
	
    public void send(String msg) throws Exception {
    	
    	String env = getenv("APP_ENV");
    	
        // Sender's email configuration
        String senderEmail = user; 
        String senderPassword = password; 

        // Recipient's email address
        String recipientEmail = emailRecipient;

        // Email configuration
        String subject = "Error: Rapidcue File Xfer from Rapidcue to Ascap ftp ";
        String body = msg;

        // Email properties
        Properties properties = new Properties();
        properties.put("mail.smtp.auth", "true");
        properties.put("mail.smtp.starttls.enable", "true");
        properties.put("mail.smtp.host", host);  
        properties.put("mail.smtp.port", port);  

        // Creating a session with authentication
        Session session = Session.getInstance(properties, new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(senderEmail, senderPassword);
            }
        });

        try {
            // Creating a message
            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress(senderEmail));
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(recipientEmail));
            message.setSubject(subject);
            message.setText(body);

            // Sending the email
            Transport.send(message); 
        } catch (Exception e) {
        	throw new Exception("Error while sending email " + e.getMessage()); 
        }
    }
}

