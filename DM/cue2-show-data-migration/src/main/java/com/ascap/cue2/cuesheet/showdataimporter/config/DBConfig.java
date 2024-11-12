package com.ascap.cue2.cuesheet.showdataimporter.config;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import static com.ascap.cue2.cuesheet.showdataimporter.constants.Constants.JDBC_DRIVER_MSSQL;

public class DBConfig {

    private static String dbSecret = null;
    private static String showDbSecret = null;
    
    static final String DB_SERVER_SHOW = "show";
    static final String DB_SERVER_CUE2 = "cue2";
        
    static final String CUE2_CLUSTER_WRITER_ENDPOINT = "CLUSTER_WRITER_ENDPOINT";
    static final String CUE2_PORT = "PORT";
    static final String CUE2_DB_NAME = "DB_NAME";
    static final String CUE2_USERNAME = "USERNAME";
    static final String CUE2_PWD = new StringBuilder("PASS").append("WORD").toString();
    
    static final String SHOW_SECRET_HOST_FIELD = "host";
    static final String SHOW_SECRET_PORT_FIELD = "port";
    static final String SHOW_SECRET_DB_FIELD = "dbname";
    static final String SHOW_SECRET_USER_FIELD = "username";
    static final String SHOW_SECRET_PWD_FIELD = new StringBuilder("pass").append("word").toString();
    



    public DBConfig(String dbSecret, String showDbSecret){
        this.dbSecret = dbSecret;
        this.showDbSecret = showDbSecret;
    }

    private static final Log LOG = LogFactory.getLog(DBConfig.class.getName());
	private static final String US_EAST_1 = "us-east-1";

    public static Connection getSQLConnection() throws Exception {
		String[] secretValues = getSecretValues(DB_SERVER_SHOW);

        Connection con = null;
        try {
        	Class.forName(JDBC_DRIVER_MSSQL);
            String url = String.format("jdbc:sqlserver://%s:%s",
            	            secretValues[0],
            	            secretValues[1]);
            		
            LOG.info("SQL Config: " + url);
            
            Properties props = new Properties();
            props.setProperty("user", secretValues[2]);
            props.setProperty("password", secretValues[3]);
            props.setProperty(";databaseName", secretValues[4]);
            
            
            con = DriverManager.getConnection(url, props);
        } catch (SQLException C) {
            LOG.error("Exception in loading Show DB connection "+ C);
            System.exit(0);
        }
        return con;
    }
    
    
    static String[] getSecretValues(String dbServer) throws Exception {
		String secret = null;
		if(DB_SERVER_CUE2.equals(dbServer)) {
			secret = dbSecret;
		}
		else if(DB_SERVER_SHOW.equals(dbServer)) {
			secret = showDbSecret;
		}
		LOG.info("DB Secret: " + secret);
		AWSSecretsManager secretsManager = AWSSecretsManagerClientBuilder.standard().withRegion(US_EAST_1).build();
        GetSecretValueRequest request = new GetSecretValueRequest().withSecretId(secret);
        GetSecretValueResult result = secretsManager.getSecretValue(request);
        String secretString = result.getSecretString();

        ObjectMapper objectMapper = new ObjectMapper();
        JsonFactory jsonFactory = objectMapper.getFactory();
        JsonNode jsonNode;
        try {
            JsonParser jsonParser = jsonFactory.createParser(secretString);
            jsonNode = objectMapper.readTree(jsonParser);

        } catch (IOException e) {
            throw new Exception("Failed to parse secret string: " + secretString, e);
        }
        
        String[] secretArray = new String[5];
        
        if(DB_SERVER_CUE2.equals(dbServer)) {
        	secretArray =  new String[]{
	                jsonNode.get(CUE2_CLUSTER_WRITER_ENDPOINT).asText(),
	                jsonNode.get(CUE2_PORT).asText(),
	                jsonNode.get(CUE2_DB_NAME).asText(),
	                jsonNode.get(CUE2_USERNAME).asText(),
	                jsonNode.get(CUE2_PWD).asText()
	        };
        }
        else if(DB_SERVER_SHOW.equals(dbServer)) {
        	secretArray =  new String[]{
                    jsonNode.get(SHOW_SECRET_HOST_FIELD).asText(),
                    jsonNode.get(SHOW_SECRET_PORT_FIELD).asText(),
                    jsonNode.get(SHOW_SECRET_USER_FIELD).asText(),
                    jsonNode.get(SHOW_SECRET_PWD_FIELD).asText(),
                    jsonNode.get(SHOW_SECRET_DB_FIELD).asText()
            };
        }
        return secretArray;
    }

    public static Connection getPostgresConnection() throws Exception {
		String[] secretValues = getSecretValues(DB_SERVER_CUE2);

        String url = "";
		Connection con = null;
		url = String.format("jdbc:postgresql://%s:%s/%s",
	            secretValues[0],
	            secretValues[1],
	            secretValues[2]);
	
	    Properties props = new Properties();
	    props.setProperty("user", secretValues[3]);
	    props.setProperty("password", secretValues[4]);
	
	    con = getConnection(url, props);
		LOG.info("DB: " + url);
	
		return con;
    }



    private static Connection getConnection(String url, Properties props) throws SQLException {
    	return DriverManager.getConnection(url, props);
    }


    public static void closeResources(Connection con, PreparedStatement pstmt, ResultSet rs) {
        try {

            if (rs != null) {
                rs.close();
            }
        } catch (SQLException sqle) {
            //Nothing need not rollback transaction
        }
        try {
            if (pstmt != null) {
                pstmt.close();
            }
        } catch (SQLException sqle) {
            //Nothing need not rollback transaction
        }
        if (con != null) {
            try {
                con.close();
                LOG.info("Connection closed");
            } catch (SQLException e) {
                //Nothing to rollback

            }
        }
    }
}
