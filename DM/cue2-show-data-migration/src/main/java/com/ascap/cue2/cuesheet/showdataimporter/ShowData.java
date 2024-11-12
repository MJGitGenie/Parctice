package com.ascap.cue2.cuesheet.showdataimporter;

import static com.ascap.cue2.cuesheet.showdataimporter.constants.ShowQueryConstants.NTILE_COUNT_QUERY;
import static com.ascap.cue2.cuesheet.showdataimporter.constants.ShowQueryConstants.NTILE_ON_PROGRAM_CODE_QUERY;
import static com.ascap.cue2.cuesheet.showdataimporter.constants.ShowQueryConstants.DELETE_INC_STG_TABLE;



import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ascap.cue2.cuesheet.showdataimporter.config.DBConfig;

public class ShowData {

    private final static Log LOG = LogFactory.getLog(ShowData.class.getName());

     Properties properties;
     DBConfig dbConfig;


    public ShowData(Properties properties){
        this.properties = properties;
        LOG.info(properties.getProperty("env") + properties.getProperty("app_name") + " " + properties.getProperty("db_secret") + " " +  properties.getProperty("show_db_secret"));
        dbConfig = new DBConfig(properties.getProperty("db_secret"), properties.getProperty("show_db_secret"));

    }
    public ShowData() {
    }
    
    ProcessData processData = new ProcessData();

    public Connection getConnection(Boolean incremental, int maxNumberOfPrograms) throws Exception{
    	LOG.info("incremental " + incremental);
        Connection connection = dbConfig.getSQLConnection();
        Connection pgCon = dbConfig.getPostgresConnection();
        long maxIncrementalId = 0;
        if(Boolean.TRUE.equals(incremental)) {
        	LOG.info("Incremental Data Migration......");
        	maxIncrementalId = getMaxIncrementalId(connection);
        	populateDMData(connection);
        }
        LOG.info("incremental " + incremental);
        processPrograms(maxNumberOfPrograms, connection, pgCon, incremental, maxIncrementalId);
        /*if(Boolean.TRUE.equals(incremental)) {
        	Connection conn = dbConfig.getSQLConnection();
        	truncateStagingTable(conn, maxIncrementalId);
        }
        */
        
        return connection;
    }


    protected long getMaxIncrementalId(Connection connection) {
    	LOG.info("In getMaxIncrementalId");
    	
    	long maxIncrId = 0;
    	
    	try (final PreparedStatement pstmt = connection.prepareStatement("select max(incremental_id) incr_id from DM_Incremental_PgmCde where status = 'PENDING'");
    			final ResultSet rs = pstmt.executeQuery();) {
    		if(rs.next()) {
    			maxIncrId = rs.getLong(1);
    		}
    	}
    	catch (Exception e) {
			LOG.error(e);
		}
    	
    	LOG.info("In getMaxIncrementalId " + maxIncrId);
    	return maxIncrId;
	}

    protected void truncateStagingTable(Connection connection, long incrementalId, List<String> programCodes) {
    	LOG.info("Updating staging table");
    	PreparedStatement pstmt = null;
    	int count = 0;
    	try {
    		String qry = "update DM_Incremental_PgmCde set status = 'COMPLETE', updatedatetime = getdate() where incremental_id <= ? and status = 'INPROGRESS' ";
    		qry += " and pgmcde in (" + programCodes.stream()
            .collect(Collectors.joining(","))+ ")";
    		
    		pstmt = connection.prepareStatement(qry);
    		
    		pstmt.setLong(1, incrementalId);
    		count = pstmt.executeUpdate();
    	}
    	catch(Exception e) {
    		LOG.error("Error in truncateStagingTable ", e);
    	}
    	finally {	    		
    		if(pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {}
    		}
    		/*if(connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {}
    		}*/
    	}
    	LOG.info("Deleting from staging table Done, rows Updated: " + count);
	}
    protected void populateDMData(Connection connection) {
    	LOG.info("Entering populateDMData");
    	//if(true) return;
    	CallableStatement cstmt = null;
    	List<String> procList = new ArrayList<>();
    	procList.add("{ call DM_Prc_Load_Program_Series('INCREMENTAL') }");
    	procList.add("{ call DM_Prc_Load_MusCtnUniverse('INCREMENTAL') }");
    	procList.add("{ call DM_Prc_Cuesht_Info('INCREMENTAL') }");
    	procList.add("{ call DM_Prc_TblSPuAKa('INCREMENTAL') }");
    	procList.add("{ call DM_Prc_Load_DirAct('INCREMENTAL') }");

    	for (String procName : procList) {
    		LOG.info("Running populateDMData for " + procName);
	    	try {
	    		cstmt = connection.prepareCall(procName);
	    		cstmt.execute();
	    		connection.commit();
	    	}
	    	catch(Exception e) {
	    		LOG.error("Error in populateDMData ", e);
	    		throw new RuntimeException("Procedure execution failed. ", e);
	    	       
	    	}
	    	finally {	    		
	    		if(cstmt != null) {
					try {
						cstmt.close();
					} catch (SQLException e) {}
	    		}
	    	}
    	}
    	LOG.info("Exiting populateDMData");
	}
    
    
    public int deleteDuplicateAltTitles(Connection con) {
    	int count = 0;
    	String qry = "DELETE FROM cue2.alt_ttl " + 
    			" WHERE alt_ttl_id IN ( " + 
    			" SELECT alt_ttl_id " + 
    			"    FROM  " + 
    			"        (SELECT alt_ttl_id, " + 
    			"         ROW_NUMBER() OVER( PARTITION BY PGM_CDE, SER_CDE, PROD_TTL, ALT_LNG, DEL_FLG " + 
    			"        ORDER BY  alt_ttl_id DESC ) AS row_num " + 
    			"        FROM cue2.alt_ttl  " + 
    			") t " + 
    			"        WHERE t.row_num > 1) ";
    	
    	try(PreparedStatement pstmt = con.prepareStatement(qry);) {
    		count = pstmt.executeUpdate();
    	} catch (SQLException e) {
			LOG.error("Error while deleting duplicates from Alt Title Tables ", e);
		}
    	LOG.info("Deleted Duplicate Alt title count " + count);
    	return count;
    }
    
	public int processPrograms(int maxNumberOfPrograms, Connection connection, Connection pgCon, Boolean incremental, long maxIncrementalId) throws Exception{
		pgCon.setAutoCommit(false);
        int ntileCount = 0;
        
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        ResultSet rs1 = null;
        LinkedHashMap<Integer,Integer> programCodeRange = new LinkedHashMap<>();

        long startTime =   System.currentTimeMillis();
        try{
        	if(Boolean.TRUE.equals(incremental)) { 
        		ntileCount = 1000000000;
        		maxNumberOfPrograms = 1000000000;
    			LOG.info("Incremental is true.. returning doing nothing ntileCount "+ ntileCount +" maxNumberOfPrograms " + maxNumberOfPrograms );
    			}
        	else {
        	
	        	LOG.info("Executing NTILE query : "+ (System.currentTimeMillis() - startTime));
	        	preparedStatement = connection.prepareStatement(NTILE_COUNT_QUERY);
	            preparedStatement.setInt(1, maxNumberOfPrograms);
	            rs1 = preparedStatement.executeQuery();
	            if(rs1.next()) {
	            	ntileCount = rs1.getInt(1);
	            }
	            preparedStatement.close();
        	}
        	
            preparedStatement = connection.prepareStatement(NTILE_ON_PROGRAM_CODE_QUERY);
            preparedStatement.setInt(1, ntileCount);
            preparedStatement.setInt(2, maxNumberOfPrograms);

            rs = preparedStatement.executeQuery();
            List<String> programCodesList = new ArrayList<>();
            int i =0;
             while(rs.next()){
                programCodeRange.put(rs.getInt(1),rs.getInt(2));
                if(!incremental) {
                	LOG.info(rs.getInt(1) + " and " + rs.getInt(2));
                }
                else {
                	i++;
                	programCodesList.add(rs.getString(1));
                	if(i % 1000 == 0) {
                		doIncremetalUpate(connection, pgCon, programCodesList, maxIncrementalId);
                		programCodesList.clear();
                	}
                }
             }
             if(programCodesList.size() > 0 ) {
         		doIncremetalUpate(connection, pgCon, programCodesList, maxIncrementalId);
             }
             if(Boolean.TRUE.equals(incremental)) { 
            	 LOG.info("Incremental is true..returning doing nothing");
            	 //truncateStagingTable(connection, maxIncrementalId);
             }

             if(!incremental) {
            LOG.info("Total runtime for NTILE query : "+ (System.currentTimeMillis() - startTime));
             pgCon.setAutoCommit(false);

         for (HashMap.Entry<Integer,Integer> entry : programCodeRange.entrySet()) {
        	 
	        	 if(Boolean.TRUE.equals(incremental)) { 
	            	 /*LOG.info("Incremental is true..Deleting Records from PG");*/
	            	 //deleteFromCue2(entry, pgCon);
	             }
        	 	
        	 	long firstDb = System.currentTimeMillis();
                
                 processData.insertCueHeader(entry, connection, pgCon, false, null);
                 processData.insertMusicContent(entry, connection, pgCon, false, null);
                 processData.insertAVParties(entry, connection, pgCon, false, null);
                 processData.insertIntParties(entry, connection, pgCon, false, null);
                 processData.insertAltTitles(entry, connection, pgCon, false, null);
                 processData.insertErrors(entry, connection, pgCon, false, null);
                 processData.insertUmbrellaElements(entry, connection, pgCon, false, null);
                 
                 
                 pgCon.commit();
          }
             }
         
        deleteDuplicateAltTitles(pgCon);
        pgCon.commit();

        }catch (SQLException e){
           throw new RuntimeException("Cannot load ntile program code range", e);
        }finally{
            DBConfig.closeResources(connection, preparedStatement, rs);
            DBConfig.closeResources(null, null, rs1);
            DBConfig.closeResources(pgCon, null, null);
           }
        return maxNumberOfPrograms;

    }
	
	
	
	private void doIncremetalUpate(Connection connection, Connection pgCon, List<String> programCodesList, long maxIncrementalId)
			throws SQLException {
		pgCon.setAutoCommit(false);

		LOG.info("Incremental is true..Deleting Records from PG");
		deleteFromCue2(pgCon, programCodesList);

		long firstDb = System.currentTimeMillis();

		processData.insertCueHeader(null, connection, pgCon, true, programCodesList);
		processData.insertMusicContent(null, connection, pgCon, true, programCodesList);
		processData.insertAVParties(null, connection, pgCon, true, programCodesList);
		processData.insertIntParties(null, connection, pgCon, true, programCodesList);
		processData.insertAltTitles(null, connection, pgCon, true, programCodesList);
		processData.insertErrors(null, connection, pgCon, true, programCodesList);
		processData.insertUmbrellaElements(null, connection, pgCon, true, programCodesList);

		pgCon.commit();
		
		truncateStagingTable(connection, maxIncrementalId, programCodesList);
	}
	/*
	public int processProgramsInc(int maxNumberOfPrograms, Connection connection, Connection pgCon, Boolean incremental) throws Exception{

        int ntileCount = 0;
        
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        ResultSet rs1 = null;
        LinkedHashMap<Integer,Integer> programCodeRange = new LinkedHashMap<>();

        long startTime =   System.currentTimeMillis();
        try{
        	if(Boolean.TRUE.equals(incremental)) { 
        		ntileCount = 1000000000;
        		maxNumberOfPrograms = 1000000000;
    			LOG.info("Incremental is true.. returning doing nothing ntileCount "+ ntileCount +" maxNumberOfPrograms " + maxNumberOfPrograms );
    			}
        	else {
        	
	        	LOG.info("Executing NTILE query : "+ (System.currentTimeMillis() - startTime));
	        	preparedStatement = connection.prepareStatement(NTILE_COUNT_QUERY);
	            preparedStatement.setInt(1, maxNumberOfPrograms);
	            rs1 = preparedStatement.executeQuery();
	            if(rs1.next()) {
	            	ntileCount = rs1.getInt(1);
	            }
	            preparedStatement.close();
        	}
        	
            preparedStatement = connection.prepareStatement(NTILE_ON_PROGRAM_CODE_QUERY);
            preparedStatement.setInt(1, ntileCount);
            preparedStatement.setInt(2, maxNumberOfPrograms);

            rs = preparedStatement.executeQuery();

             while(rs.next()){
                programCodeRange.put(rs.getInt(1),rs.getInt(2));
                if(!incremental) {
                	LOG.info(rs.getInt(1) + " and " + rs.getInt(2));
                }
             }
             if(Boolean.TRUE.equals(incremental)) { 
            	 LOG.info("Incremental is true..returning doing nothing");
             }

            LOG.info("Total runtime for NTILE query : "+ (System.currentTimeMillis() - startTime));
             pgCon.setAutoCommit(false);

         for (HashMap.Entry<Integer,Integer> entry : programCodeRange.entrySet()) {
        	 
	        	 if(Boolean.TRUE.equals(incremental)) { 
	            	 deleteFromCue2(entry, pgCon);
	             }
        	 	
        	 	long firstDb = System.currentTimeMillis();
                
                 processData.insertCueHeader(entry, connection, pgCon, incremental);
                 processData.insertMusicContent(entry, connection, pgCon, incremental);
                 processData.insertAVParties(entry, connection, pgCon, incremental);
                 processData.insertIntParties(entry, connection, pgCon, incremental);
                 processData.insertAltTitles(entry, connection, pgCon, incremental);
                 processData.insertErrors(entry, connection, pgCon, incremental);
                 processData.insertUmbrellaElements(entry, connection, pgCon, incremental);
                 
                 pgCon.commit();
          }
         
        deleteDuplicateAltTitles(pgCon);
        pgCon.commit();

        }catch (SQLException e){
           throw new RuntimeException("Cannot load ntile program code range", e);
        }finally{
            DBConfig.closeResources(connection, preparedStatement, rs);
            DBConfig.closeResources(null, null, rs1);
            DBConfig.closeResources(pgCon, null, null);
           }
        return maxNumberOfPrograms;

    }
    */
	

    public int deleteFromCue2(Connection pgCon, List<String> programCodesList) throws SQLException {
		int count = 0;
    	List<String> updateTableList = Arrays.asList(
    			" cue2.av_int_pty ",
    			" cue2.mus_int_pty ",
    			" cue2.mus_content ",
    			" cue2.alt_ttl ",
    			" cue2.cue_sht_hdr "
    			);
    	
    	for(String tableName : updateTableList) {
			String qry = "delete from  "+ tableName + "where pgm_cde in ("+programCodesList.stream()
            .collect(Collectors.joining(","))+") and del_flg = 'N'";
			  //LOG.info(qry + " " + programCodesList.stream()
              //.collect(Collectors.joining(","))); 
			try(PreparedStatement pstmt = pgCon.prepareStatement(qry);) {
			 /* LOG.info(entry.getKey() + " Deleting the table.. " + qry); */
				//pstmt.setLong(	1, 	pgCon.getKey());
				count = pstmt.executeUpdate();
			} catch (SQLException e) {
				LOG.error("ERROR while deleteing from Tables.. ", e);
				throw new RuntimeException(e.getMessage());
			}
			
		}
    	
    	return count;
    
    }

}
