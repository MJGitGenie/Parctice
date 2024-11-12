package com.ascap.cue2.cuesheet.showdataimporter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

 

public class RapidcueFileTransfer {
	
	
	private static final Logger LOG = LogManager.getLogger(RapidcueFileTransfer.class);
	 
	public static void main(String[] args) throws Exception {
		 
		LOG.info("File transfer batch started"); 
		
		try {
			RapidcueSFTPUtil sftp = new RapidcueSFTPUtil();
			sftp.transferFiles(); 
		} catch (Exception ex) {
			//RapidcueEmail email = new RapidcueEmail(); 
			//email.send("Error: " + ex.getMessage()); 
			throw new Exception("Error while file transfer " + ex.getMessage());  
		}
		
		 
		LOG.info("File transfer batch completed."); 
	
    }
	

}
