package com.ascap.cue2.cuesheet.showdataimporter;


import static com.amazonaws.regions.Regions.US_EAST_1;
import static java.lang.System.getenv;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption; 
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Base64;
import java.util.Date; 
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.ChannelSftp.LsEntrySelector;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session; 
import com.jcraft.jsch.SftpATTRS; 
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult; 
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper; 


public class RapidcueSFTPUtil {
	
	
	
	private static final Logger LOG = LogManager.getLogger(RapidcueSFTPUtil.class);
	private String localDir = null;
	private String remoteDirRapidcue = null; 
	private String remoteArchiveDirRapidcue = null; 
	private String remoteDirAscap = null;  
	private String pvtkeyPathRapidcue = null;
	private String pvtkeySecretnameRapidcue = null;
	private String pvtkeyPassphraseRapidcue = null;
	private String usernameRapidcue = null;
	private String passwordRapidcue = null;
	private String hostRapidcue = null; 
	private String hostAscapSftp = null;
	private String usernameAscapSftp = null;
	private String passwordAscapSftp = null;
	
	
	/* AWS secrets manager client */
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
    
    
	public RapidcueSFTPUtil() throws Exception {
		try {
			init(); 
		} catch (Exception e) { 
			throw e;
		}
	} 
	 
	
	void init() throws Exception {     
		 
		localDir=System.getProperty("java.io.tmpdir");  
        
		//File file = File.createTempFile("Download", ".TEMP");  
        //localDir = file.getParent();  
        
        String rapidcueSecret = getenv("RAPIDCUE_SECRET_NAME");
        JsonNode jsonNode = getSecretObject(rapidcueSecret);
        LOG.info("Got values from secret: {}", rapidcueSecret); 
             
        remoteDirRapidcue = jsonNode.get("RAPIDCUE_REMOTE_DIR").asText() ;   
        remoteArchiveDirRapidcue = jsonNode.get("RAPIDCUE_ARCHIVE_REMOTE_DIR").asText() ;  
    	remoteDirAscap = jsonNode.get("ASCAP_REMOTE_DIR").asText() ;    
    	pvtkeySecretnameRapidcue = jsonNode.get("RAPIDCUE_PVTKEY_SECRET").asText() ;   
    	pvtkeyPassphraseRapidcue = jsonNode.get("RAPIDCUE_KEY_PASSPHRASE").asText() ;   
    	usernameRapidcue = jsonNode.get("RAPIDCUE_USERNAME").asText() ;   
    	passwordRapidcue = jsonNode.get("RAPIDCUE_PASSWORD").asText() ;   
    	hostRapidcue = jsonNode.get("RAPIDCUE_HOSTNAME").asText() ;    
    	hostAscapSftp = jsonNode.get("ASCAP_HOSTNAME").asText() ;   
    	usernameAscapSftp = jsonNode.get("ASCAP_USERNAME").asText() ;   
    	passwordAscapSftp = jsonNode.get("ASCAP_PASSWORD").asText() ;   
    	
    	File privatekeyfile = getPrivatekey();
    	pvtkeyPathRapidcue = privatekeyfile.getAbsolutePath();
 
	}
    	
	
	public File getPrivatekey() throws IOException {

        LOG.info("Getting privatekey from secret" );
    
            InputStream inputStream = new ByteArrayInputStream(getKey().getBytes());   
            File file = new File(localDir+"RapidcuePrivatekeyDownload.ppk");
            //File file = File.createTempFile("RapidcuePrivatekeyDownload", ".ppk");   
            java.nio.file.Files.copy(inputStream, file.toPath(), StandardCopyOption.REPLACE_EXISTING);
            inputStream.close();  
            
        LOG.info("Got privatekey from secret");
        return file;
    }
	
    
    public String getKey() {

        String secretName = pvtkeySecretnameRapidcue; 
        String region = System.getenv("AWS_DEFAULT_REGION");  
        
        // Create a Secrets Manager client
        AWSSecretsManager client  = AWSSecretsManagerClientBuilder.standard()
                                        .withRegion(region)
                                        .build();
          
        String secret, decodedBinarySecret;
        GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest()
                        .withSecretId(secretName);
        GetSecretValueResult getSecretValueResult = null; 
        
        try {
            getSecretValueResult = client.getSecretValue(getSecretValueRequest);
        } catch (Exception e) {  
            throw e;
        }

        // Decrypts secret using the associated KMS key.
        // Depending on whether the secret is a string or binary, one of these fields will be populated.
        if (getSecretValueResult.getSecretString() != null) {
            secret = getSecretValueResult.getSecretString(); 
            return secret;
        }
        else {
            decodedBinarySecret = new String(Base64.getDecoder().decode(getSecretValueResult.getSecretBinary()).array()); 
            return decodedBinarySecret;
        }

        
    }
	
	
	
	public void transferFiles () throws Exception {
		 
           
		LOG.info("Entering transferFiles"); 
		LOG.info("RAPIDCUE_REMOTE_DIR: {}, ASCAP_REMOTE_DIR: {}, LOCAL_DIR: {} ",remoteDirRapidcue, remoteDirAscap,  localDir); 
   
        String filename = null;
        int discoveredFiles = 0;  
        int processedFiles = 0; 
        
        Session session = null;

        try {
        	  
            JSch jsch = new JSch(); 
            session = jsch.getSession(usernameRapidcue, hostRapidcue, 22);
            session.setPassword(passwordRapidcue);
            session.setConfig("StrictHostKeyChecking", "no"); 
            jsch.addIdentity(pvtkeyPathRapidcue, pvtkeyPassphraseRapidcue );
             
            session.connect(10000);

            Channel channel = session.openChannel("sftp"); 
            channel.connect(5000);  
            
            ChannelSftp channelSftp = (ChannelSftp) channel; 
            
            channelSftp.cd(remoteDirRapidcue);
            Vector<String> filelist=new Vector<String>();
            
            LsEntrySelector selector = new LsEntrySelector() {
                public int select(LsEntry entry)  {
                    final String filename = entry.getFilename();
                    if (filename.equals(".") || filename.equals("..")) {
                        return CONTINUE;
                    }
                    if (!entry.getAttrs().isLink() && !entry.getAttrs().isDir()) {
                    	filelist.addElement(entry.getFilename()); 
                    }  
                    return CONTINUE;
                }
            }; 
            channelSftp.ls(remoteDirRapidcue,selector);
            
            discoveredFiles = filelist.size();
            LOG.info("Total files found on the source ftp : {}  ", discoveredFiles ); 
 
             
            for(int i=0; i<filelist.size();i++){
            	filename = filelist.get(i).toString(); 
                
            	try {
            		
            	
                channelSftp.get(remoteDirRapidcue+"/"+filename, localDir+"/"+filename);
  
                // update timestamp of the downloaded file
                SftpATTRS attrs = channelSftp.lstat(remoteDirRapidcue+"/"+filename);
                Date origDate = new Date(attrs.getMTime() * 1000L);
                File downloadedFile = new File(localDir+"/"+filename);
                boolean dataModified = downloadedFile.setLastModified(origDate.getTime()); 
                
                if (dataModified == false)
                throw new Exception("File timestamp could not be modified after downloading file: " + filename); 
 
                
                uploadFileUsingPwd (filename); 
                //LOG.info("file : {} uploaded to SFTP : {} " ,localDir+"/"+filename, remoteDirAscap+"/"+filename); 
  
                //archive the file
                channelSftp.put(localDir+"/"+filename, remoteArchiveDirRapidcue+"/"+filename);
                
                //delete file from source sftp 
                channelSftp.rm(remoteDirRapidcue+"/"+filename); 
                 
                //LOG.info("file : {} downloaded from SFTP to : {} " ,remoteDirRapidcue+"/"+filename, localDir+"/"+filename); 
 
                
                Path localFilePath = Paths.get(localDir+"/"+filename);
                Files.deleteIfExists(localFilePath);
                
                processedFiles++;
                
                LOG.info("NOTE: File# "+processedFiles+" Transferred: " + filename); 
 
            	} catch (Exception e) { 
            		LOG.info("Error caught while transferring the file: " + filename); 
            		LOG.info("Error:: ", e); 
            	}
                

            } 
            
            LOG.info("Total {} files trnsferred out of total {} discovered files ", processedFiles,  discoveredFiles); 
            LOG.info("Exiting transferFiles"); 
  
            
            
        } catch (Exception e) {  
            throw new Exception("Error while downloading " + e.getMessage()); 
        } finally {
            if (session != null) {
                session.disconnect();
            }
        } 
	}
	
	
	
	

	
	public void uploadFileUsingPwd (String filename) throws Exception {
		 
        
        Session session = null;

        try {
        	  
            JSch jsch = new JSch(); 
            session = jsch.getSession(usernameAscapSftp, hostAscapSftp, 22);
            session.setPassword(passwordAscapSftp);
            session.setConfig("StrictHostKeyChecking", "no");  
             
            session.connect(10000); 
            Channel channel = session.openChannel("sftp"); 
            channel.connect(5000); 
            ChannelSftp channelSftp = (ChannelSftp) channel;

            // transfer file from local to remote server
            channelSftp.put(localDir+"/"+filename, remoteDirAscap+"/"+filename);
 
            // update timestamp of the uploaded file
            Path localFilepath = Paths.get(localDir+"/"+filename);
            BasicFileAttributes attr = Files.readAttributes(localFilepath, BasicFileAttributes.class);  
            FileTime filetime = attr.lastModifiedTime();  
        	Date origDate = new Date( filetime.toMillis() ); 
            int time = (int) (origDate.getTime() / 1000L);  
            channelSftp.setMtime(remoteDirAscap+"/"+filename, time);
             
            channelSftp.exit(); 
            
        } catch (Exception e) {  
            throw new Exception("Error while uploading: " + filename + " Error: " + e.getMessage()); 
        } finally {
            if (session != null) {
                session.disconnect();
            }
        }

	}


}
