package com.ascap.cue2.cuesheet.showdataimporter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ascap.cue2.cuesheet.showdataimporter.config.AVInterestedParty;
import com.ascap.cue2.cuesheet.showdataimporter.config.AlternateProductionTitle;
import com.ascap.cue2.cuesheet.showdataimporter.config.MusicContentItem;
import com.ascap.cue2.cuesheet.showdataimporter.config.MusicInterestedParty;
import com.ascap.cue2.cuesheet.showdataimporter.config.Production;
import com.ascap.cue2.cuesheet.showdataimporter.config.ProgramError;
import com.ascap.cue2.cuesheet.showdataimporter.config.UmbrellaElement;
import com.ascap.cue2.cuesheet.showdataimporter.constants.ShowQueryConstants;
import com.ascap.cue2.cuesheet.showdataimporter.utils.DateValidationUtils;

public class ProcessData {

    private final Log LOG = LogFactory.getLog(ProcessData.class.getName());
    
    protected DateValidationUtils dateUtilsYMD = new DateValidationUtils("yyyy-MM-dd");

    protected DateValidationUtils dateUtilsYMDHMS = new DateValidationUtils("yyyy-MM-dd HH:mm:ss");
    
    public int insertUmbrellaElements(Entry<Integer, Integer> entry, Connection con, Connection pgCon, Boolean incremental, List<String> programCodesList) {
    	/* LOG.info(" Starting insertUmbrellaElements " + entry); */
    	int minProgramCode = -1;
        int maxprogramCode = -1; 
        if(!incremental) {
        	 minProgramCode = entry.getKey();
             maxprogramCode = entry.getValue();
        }
    	int count = 0;
    	
    	String sql = ShowQueryConstants.SELECT_UMB_1;
        if(!incremental) {
    		sql += " BETWEEN "+minProgramCode+" AND "+maxprogramCode;
    	}
    	else {
    		sql += " in ("+programCodesList.stream()
            .collect(Collectors.joining(","))+") ";
    	}
    		/*	
    			"select UmbCde, PgmCde, convert(varchar, CreTs, 121) as CreTs, CreUid, convert(varchar, UpdTs, 121) as UpdTs "+
    				  ", UpdUid from tblUmbSpec where UmbCde BETWEEN "+minProgramCode+" AND "+maxprogramCode;
    	*/
		List<UmbrellaElement> itemList = new ArrayList<>();
		try (final PreparedStatement pstmt = con.prepareStatement(sql); final ResultSet rs = pstmt.executeQuery();) {

			UmbrellaElement muItem = null;
			while (rs.next()) {
				muItem = new UmbrellaElement();

				muItem.setProgramCode(rs.getLong("UmbCde"));
				muItem.setUmbrellaProgramCode(rs.getLong("PgmCde"));
				muItem.setUmbrellaCreateDateTime(rs.getString("CreTs"));
				muItem.setUmbrellaCreateId(rs.getString("CreUid"));
				muItem.setUmbrellaUpdateDateTime(rs.getString("UpdTs"));
				muItem.setUmbrellaUpdateId(rs.getString("UpdUid"));
				itemList.add(muItem);

			}

		} catch (Exception e) {
			LOG.error(e);
		}
		/* LOG.info(" Done Selecting data for Umbrella Elements"); */
		String sqlPg = " insert into cue2 . umb_elem ( pgm_cde, umb_elem, cre_id, cre_dt, upd_id, upd_dt ) " +
				" values (?,?,?,?,?,?)";
		try (final PreparedStatement pstmt1 = pgCon.prepareStatement(sqlPg);) {
			int rowNum = 0;
			for (UmbrellaElement item : itemList) {
				rowNum++;
				pstmt1.setLong(1, item.getProgramCode());
				pstmt1.setLong(2, item.getUmbrellaProgramCode());
				pstmt1.setString(3, StringUtils.isBlank(item.getUmbrellaCreateId())?null:item.getUmbrellaCreateId().trim()); 
				pstmt1.setObject(4, StringUtils.isBlank(item.getUmbrellaCreateDateTime())?null:java.sql.Timestamp.valueOf(item.getUmbrellaCreateDateTime()));
				pstmt1.setString(5, StringUtils.isBlank(item.getUmbrellaUpdateId())?null:item.getUmbrellaUpdateId().trim());
				pstmt1.setObject(6, StringUtils.isBlank(item.getUmbrellaUpdateDateTime())?null:java.sql.Timestamp.valueOf(item.getUmbrellaUpdateDateTime()));
				pstmt1.addBatch();

				if (rowNum % 100000 == 0) {
					pstmt1.executeBatch();
					pstmt1.clearBatch();
					LOG.info(" Updated Batch for insertUmbrellaElements: " + rowNum);
				}

			}
			pstmt1.executeBatch();
			/* LOG.info("Insert Completed Umbrella Elements: " + rowNum); */
		} catch (Exception e) {
			LOG.error(e);
		}

		return count;
    }
    
    public int insertMusicContent(Entry<Integer, Integer> entry, Connection con, Connection pgCon, Boolean incremental, List<String> programCodesList) throws SQLException {
    	/* LOG.info(" Starting insertMusicContent " + entry); */
    	int minProgramCode = -1;
        int maxprogramCode = -1; 
        if(!incremental) {
        	 minProgramCode = entry.getKey();
             maxprogramCode = entry.getValue();
        }
    	int count = 0;
    	
    	String sql = ShowQueryConstants.SELECT_MUS_CONTENT_1;
        if(!incremental) {
    		sql += " BETWEEN "+minProgramCode+" AND "+maxprogramCode;
    	}
    	else {
    		sql += " in ("+programCodesList.stream()
            .collect(Collectors.joining(","))+") ";
    	}
    	
    	/*
    	String sql =  
    			" SELECT  action " + 
    			"      , actiontimestamp " + 
    			"      , actionuserid " + 
    			"      , programcode " + 
    			"      , sequnecenumber " +  //5
    			"      , musictitle " + 
    			"      , MusicdurationHH " + 
    			"      , musicdurationmm " + 
    			"      , musicdurationss " + 
    			"      , starttimehh " + //10
    			"      , starttimemm " + 
    			"      , starttimess " + 
    			"      , iswc " + 
    			"      , ISRC " + 
    			"      , musicusagecode " + //15
    			"      , AscapWorkId " + 
    			"      , programoccurance " + 
    			"      , performancecode " + 
    			"      , usetyp " + 
    			"      , MusLibId " +   //20
    			"      , AmbMusInd " + 
    			"      , createdatetime " + 
    			"      , createid " + 
    			"      , updateid " + 
    			"      , updatedattime " + //25
    			"      , matchuserid " + 
    			"      , matchdattime " + 
    			"      , source " + 
    			"      , cuematchtype " + 
    			"      , directsourceind " + //30
    			"      , publicDomainIndicator " +
    			"      , publicDomainTerritory " + 
    			"      , submitterWorkID " + 
    			"      , (MusicdurationHH * 3600 + musicdurationmm * 60 + musicdurationss) as dur  " +
    			"      , non_pay_rsn AS no_pay_rsn " + 
    			"	   , pay_ind AS pay_ind "+
    			"	   , deleteflag "+
    			"      , (starttimehh * 3600 + starttimemm * 60 + starttimess) as time_in  " +
    			"  FROM  dbo . DM_MusCtnUniverse  "
    			+ " where programcode between "+minProgramCode+" and " +maxprogramCode ;
    	*/
    	 List<MusicContentItem> muItemList = new ArrayList<>();
    	 
    	 try ( final PreparedStatement pstmt = con.prepareStatement(sql);
    			 final ResultSet rs = pstmt.executeQuery();) {
    		 MusicContentItem muItem = null;
     		while(rs.next()) {
     			muItem = new MusicContentItem();
     			muItem.setCueAction(rs.getString(1));
     			muItem.setProgramCode(rs.getLong(4));
     			muItem.setSequenceNumber(rs.getString(5));
     			muItem.setMusicTitle(rs.getString(6));
     			muItem.setIswc(rs.getString(13));
     			muItem.setIsrc(rs.getString(14));
     			muItem.setMusicUsageCode(rs.getString(15));
     			muItem.setAscapWorkId(rs.getString(16));
     			muItem.setProgramOccurrence(rs.getString(17));
     			muItem.setPerformanceCode(rs.getString(18));
     			muItem.setUseType(rs.getString(19));
     			muItem.setMusicLibraryIndicator(rs.getString(20));
     			muItem.setAmbientMusicIndicator(rs.getString(21));
     			muItem.setShowCreateDateTime(rs.getString(22));
     			muItem.setShowCreateId(rs.getString(23));
     			muItem.setShowUpdateId(rs.getString(24));
     			muItem.setShowUpdateDateTime(rs.getString(25));
     			muItem.setMatchUserId(rs.getString(26));
     			muItem.setMatchDateTime(rs.getString(27));
     			muItem.setCueMatchType(rs.getString(29));
     			muItem.setDirectSourceIndicator(rs.getString(30));
     			muItem.setMusicDuration(null);
     			muItem.setSubmitterWorkId(rs.getString(33));
     			muItem.setMusicDuration(rs.getString(34));
     			muItem.setNoPayReasonCode(rs.getString(35));
     			muItem.setPayIndicator(rs.getString(36));
     			muItem.setDeleteFlag(rs.getString(37));
     			muItem.setStartTime(rs.getString(38));
     			muItemList.add(muItem);
     			
     		}
    		 
    			} catch (Exception e) {
    				LOG.error(e);
    			}
    	/* LOG.info(" Done Selecting data"); */
    	String sql1 = "insert into  cue2 . mus_content  ( " + 
    			" pgm_cde , " + 
    			" amb_mus_ind , " + 
    			" asc_wrk_id  , " + 
    			" cue_mtch_typ  , " + 
    			" dir_src_ind,   " + //5
    			" isrc , " + 
    			" iswc , " + 
    			" mus_lib_ind , " + 
    			" seq_nr , " + 
    			" mus_ttl,  " + //10
    			" mus_usage_cde, " + 
    			" use_typ, " + 
    			" cue_occ, " + 
    			" cre_dt, " + 
    			" cre_id,  " + //15
    			" mus_dur,  " + 
    			" subm_wrk_id,  " + 
    			" upd_dt, " + 
    			" upd_id,  " + 
    			" mtch_dt,  " + //20
    			" mtch_user_id,  " + 
    			" perf_cde, " +
    			" non_pay_rsn, "+
    			" pay_ind, " +
    			" cue_act_orig, " +
    			" del_flg, " +
    			" time_in " +
    			") " + 
    			"values ( " + 
    			"?,?,?,?,?, " + 
    			"?,?,?,?,?, "+ 
    			"?,?,?,?,?, " + 
    			"?,?,?,?,?, "+ 
    			"?,?,?,?,?, " +
    			"?,?)";
    	try (final PreparedStatement pstmt1 = pgCon.prepareStatement(sql1);) {
   			 
    		int rowNumMus = 0;
    		for(MusicContentItem muItem : muItemList) {
    			rowNumMus ++;
    			pstmt1.setLong(1, muItem.getProgramCode());
    			pstmt1.setString(2, muItem.getAmbientMusicIndicator());
    			pstmt1.setLong(3, Long.parseLong(muItem.getAscapWorkId()));
    			pstmt1.setString(4, muItem.getCueMatchType());
    			pstmt1.setString(5, muItem.getDirectSourceIndicator());
    			pstmt1.setString(6, muItem.getIsrc());
    			pstmt1.setString(7, muItem.getIswc());
    			pstmt1.setString(8, muItem.getMusicLibraryIndicator());
    			pstmt1.setInt(9, Integer.parseInt(muItem.getSequenceNumber()));
    			pstmt1.setString(10, muItem.getMusicTitle());
    			pstmt1.setString(11, muItem.getMusicUsageCode());
    			pstmt1.setString(12, muItem.getUseType());
    			pstmt1.setObject(13, StringUtils.isBlank(muItem.getProgramOccurrence())?null:NumberUtils.toInt(muItem.getProgramOccurrence()));
    			pstmt1.setObject(14, StringUtils.isBlank(muItem.getShowCreateDateTime())?null:java.sql.Timestamp.valueOf(muItem.getShowCreateDateTime()));
    			pstmt1.setString(15, StringUtils.isBlank(muItem.getShowCreateId())?null:muItem.getShowCreateId().trim());
    			pstmt1.setObject(16, StringUtils.isBlank(muItem.getMusicDuration())?null:NumberUtils.toInt(muItem.getMusicDuration()));
    			pstmt1.setString(17, muItem.getSubmitterWorkId());
    			pstmt1.setObject(18, StringUtils.isBlank(muItem.getShowUpdateDateTime())?null:java.sql.Timestamp.valueOf(muItem.getShowUpdateDateTime()));
    			pstmt1.setString(19, StringUtils.isBlank(muItem.getShowUpdateId())?null:muItem.getShowUpdateId().trim());
    			pstmt1.setObject(20, StringUtils.isBlank(muItem.getMatchDateTime())?null:java.sql.Timestamp.valueOf(muItem.getMatchDateTime()));
    			pstmt1.setString(21, muItem.getMatchUserId());
    			pstmt1.setString(22, muItem.getPerformanceCode());
    			pstmt1.setString(23, muItem.getNoPayReasonCode());
    			pstmt1.setString(24, muItem.getPayIndicator());
    			pstmt1.setString(25, muItem.getCueAction());
    			pstmt1.setString(26, muItem.getDeleteFlag());
    			pstmt1.setObject(27, StringUtils.isBlank(muItem.getStartTime())?null:NumberUtils.toInt(muItem.getStartTime()));
    			
    			pstmt1.addBatch();
    			
    			if(rowNumMus % 50000 == 0 || rowNumMus == muItemList.size()) { 
					pstmt1.executeBatch();
					pstmt1.clearBatch();
					LOG.info(" Updated Batch for Music Content:  " + rowNumMus);
				}
				
    		}
    		//pstmt1.executeBatch();
    		 LOG.info(" Insert Completed for Music Content: " + rowNumMus); 
    	}
    	catch(java.sql.BatchUpdateException e) {
    		LOG.error("ERROR IN Music Content Insert: " + e.getNextException());
    	} catch(Exception e) {
    		LOG.error(e);
    	}
    	
    	return count;
    }
    
    public int insertIntParties(Entry<Integer, Integer> entry, Connection con, Connection pgCon, Boolean incremental, List<String> programCodesList) throws SQLException {
    	/* LOG.info(" Starting insertIntParties " + entry); */
    	int minProgramCode = -1;
        int maxprogramCode = -1; 
        if(!incremental) {
        	 minProgramCode = entry.getKey();
             maxprogramCode = entry.getValue();
        }
    	int count = 0;
    	String sql =  ShowQueryConstants.SELECT_INT_PTY_1;
        if(!incremental) {
    		sql += " BETWEEN "+minProgramCode+" AND "+maxprogramCode;
    	}
    	else {
    		sql += " in ("+programCodesList.stream()
            .collect(Collectors.joining(","))+") ";
    	}
    	/*
    	" SELECT  PgmCde " + 
    			"      , SeqNr " + 
    			"      , RvsTyp " + 
    			"      , case when LTRIM(RTRIM(mbrtyp)) in ('A','W') THEN 'A' " + 
    			" 		when LTRIM(RTRIM(mbrtyp)) in ('AD') THEN 'AM' " + 
    			" 		when LTRIM(RTRIM(mbrtyp)) in ('AR') THEN 'AR' " + 
    			" 		when LTRIM(RTRIM(mbrtyp)) in ('C') THEN 'C' " + 
    			" 		when LTRIM(RTRIM(mbrtyp)) in ('CA') THEN 'CA' " + 
    			" 		when LTRIM(RTRIM(mbrtyp)) in ('E','P') THEN 'E' " + 
    			" 		when LTRIM(RTRIM(mbrtyp)) in ('PF') THEN 'PR' " + 
    			" 		when LTRIM(RTRIM(mbrtyp)) in ('SP') THEN 'SE' " + 
    			" 		when LTRIM(RTRIM(mbrtyp)) in ('TR') THEN 'TR' " + 
    			" 		when LTRIM(RTRIM(mbrtyp)) in ('AE') THEN 'AE' " + 
    			" 		when LTRIM(RTRIM(mbrtyp)) in ('L') THEN 'L' " + 
    			" 		when COALESCE(MBRTYP,'') = '' THEN 'UK' " + 
    			" 		else LTRIM(RTRIM(mbrtyp)) END " + 
    			"      , Ssn " + //5
    			"      , EntPtyFstNa " + 
    			"      , EntPtyLstNa " + 
    			"      , SocCde " + 
    			"      , EntShr as EntShr " + 
    			"      , CreTs " + //10
    			"      , CreUid " + 
    			"      , UpdTs " + 
    			"      , UpdUid " + 
    			"      , SubSeqNr " + 
    			"      , CAE " + //15
    			"      , LTRIM(RTRIM(ipi)) as IPI " + 
    			"      , TIS " + 
    			"      , LTRIM(RTRIM(SubmitterNumber)) as SubmitterNumber " + 
    			"  FROM  dbo . tblErrEntPty  "
    			+ " where PgmCde between "+minProgramCode+" and " +maxprogramCode ; */
    	
    	 List<MusicInterestedParty> itemList = new ArrayList<>();
    	 
    	 try ( final PreparedStatement pstmt = con.prepareStatement(sql);
       			 final ResultSet rs = pstmt.executeQuery();) {
    		
    		MusicInterestedParty muItem = null;
    		while(rs.next()) {
    			muItem = new MusicInterestedParty();

    			muItem.setProgramCode(rs.getLong(1));
    			muItem.setSequenceNumber(rs.getString(2));
    			muItem.setRole(rs.getString(4));
    			muItem.setFirstName(rs.getString(6));
    			muItem.setLastNameOrCompanyName(rs.getString(7));
    			muItem.setSocietyCode(rs.getString(8));
    			muItem.setShare(rs.getString(9));
    			muItem.setPartyCreateDateTime(rs.getString(10));
    			muItem.setPartyCreateId(rs.getString(11));
    			muItem.setPartyUpdateDateTime(rs.getString(12));
    			muItem.setPartyUpdateId(rs.getString(13));
    			muItem.setIpiNumber(rs.getString(16));
    			muItem.setSubmitterIPNumber(rs.getString(18));
    			itemList.add(muItem);
    			
    		}
    		
    	}
    	catch(Exception e) {
    		LOG.error(e);
    	}
    	/* LOG.info(" Done Selecting data for Int Parties"); */
    	String sql1 = "insert into  cue2 . mus_int_pty  (" + 
    			" pgm_cde ," + 
    			" first_name  ," + 
    			" mid_name  ," + 
    			" last_name  ," + 
    			" role  ," + //5
    			" shr  ," + 
    			" soc_cde  ," + 
    			" ipi_nr  ," + 
    			" seq_nr  ," + 
    			" pty_id  ," + //10
    			" cre_id  ," + 
    			" cre_dt  ," + 
    			" upd_id  ," + 
    			" upd_dt  ," +
    			" subm_ip_nr  " +
    			") " +
    			"values ( " + 
    			"?,?,?,?,?, " + 
    			"?,?,?,?,?, " +  
    			"?,?,?,?,? " +
    			")";
    	try ( final PreparedStatement pstmt1 = pgCon.prepareStatement(sql1);) {
    		int rowNumInt = 0;
    		for(MusicInterestedParty muItem : itemList) {
    			rowNumInt ++;
    			pstmt1.setLong(1, muItem.getProgramCode());
    			pstmt1.setString(2, StringUtils.isBlank(muItem.getFirstName())?null:muItem.getFirstName().trim());
    			pstmt1.setString(3, null);
    			pstmt1.setString(4, StringUtils.isBlank(muItem.getLastNameOrCompanyName())?null:muItem.getLastNameOrCompanyName().trim());
    			pstmt1.setString(5, StringUtils.isBlank(muItem.getRole())?null:muItem.getRole().trim());
    			pstmt1.setDouble(6, Double.parseDouble(muItem.getShare()));
    			pstmt1.setObject(7, StringUtils.isBlank(muItem.getSocietyCode())?null:NumberUtils.toInt(muItem.getSocietyCode()));
    			pstmt1.setObject(8, (StringUtils.isBlank(muItem.getIpiNumber()) || !StringUtils.isNumeric(muItem.getIpiNumber()))?null:NumberUtils.toLong(muItem.getIpiNumber()));
    			pstmt1.setInt(9, Integer.parseInt(muItem.getSequenceNumber()));
    			pstmt1.setObject(10, null);
    			pstmt1.setString(11, StringUtils.isBlank(muItem.getPartyCreateId())?null:muItem.getPartyCreateId().trim());
    			pstmt1.setObject(12, StringUtils.isBlank(muItem.getPartyCreateDateTime())?null:java.sql.Timestamp.valueOf(muItem.getPartyCreateDateTime()));
    			pstmt1.setString(13, StringUtils.isBlank(muItem.getPartyUpdateId())?null:muItem.getPartyUpdateId().trim());
    			pstmt1.setObject(14, StringUtils.isBlank(muItem.getPartyUpdateDateTime())?null:java.sql.Timestamp.valueOf(muItem.getPartyUpdateDateTime()));
    			pstmt1.setObject(15, (StringUtils.isBlank(muItem.getSubmitterIPNumber()) || !StringUtils.isNumeric(muItem.getSubmitterIPNumber()))?null:NumberUtils.toLong(muItem.getSubmitterIPNumber()));
    			pstmt1.addBatch();
    			
    			if(rowNumInt % 44000 == 0  || rowNumInt == itemList.size()) { 
					pstmt1.executeBatch();
					pstmt1.clearBatch();
					LOG.info(" Updated Batch for Int Parties: " + rowNumInt);
				}
				
				
    		}
    		//pstmt1.executeBatch();
    		/* LOG.info(" Insert Completed for Int Parties: " + rowNum); */
    	}catch(java.sql.BatchUpdateException e) {
    		LOG.error("ERROR IN Int Parties Insert: " + e.getNextException());
    	}catch(Exception e) {
    		LOG.error(e);
    	}
    	
    	return count;
    }
    

    
    

    public int insertCueHeader(Entry<Integer, Integer> entry, Connection con, Connection pgCon, Boolean incremental, List<String> programCodesList) throws SQLException {
    	LOG.info(" Starting insert CueHeader " + entry + " Incremental : " + incremental);
    	if(incremental) {
        	LOG.info("Program Codes '" + programCodesList.stream()
                    .collect(Collectors.joining(","))+"'");
    	}
    	int minProgramCode = -1;
        int maxprogramCode = -1; 
        if(!incremental) {
        	 minProgramCode = entry.getKey();
             maxprogramCode = entry.getValue();
        }
    	int count = 0;
    	

    	
    	String sql =  ShowQueryConstants.SELECT_CUE_SHT_HDR_1;
        if(!incremental) {
    		sql += " between "+minProgramCode+" and " +maxprogramCode;
    	}
    	else {
    		sql += " in ("+programCodesList.stream()
            .collect(Collectors.joining(","))+") ";

    		//sql += " in (-99999) ";
    	}
    	sql += ShowQueryConstants.SELECT_CUE_SHT_HDR_2;
        if(!incremental) {
    		sql += " between "+minProgramCode+" and " +maxprogramCode;
    	}
    	else {
    		sql += " in ("+programCodesList.stream()
            .collect(Collectors.joining(","))+") ";

    		//sql += " in (-99999) ";
    	}

    	sql += ShowQueryConstants.SELECT_CUE_SHT_HDR_3;
    	/*
    	String sql =  
    			
    			"SELECT  a.Action    " + 
    			"    			      , a.ActTs    " + 
    			"    			      , a.ActUid    " + 
    			"    			      , a.pgmcde    " + 
    			"    			      , a.SerCde   " + //5 
    			"    			      , a.Productyontype    " + 
    			"    			      , REPLACE(REPLACE(a.productioncategorycode, CHAR(13), ''), CHAR(10), '') as productioncategorycode    " + 
    			"    			      , a.productiontitle    " + 
    			"    			      , a.episodetitle    " + 
    			"    			      , a.episodenumber   " + //--10 " + 
    			"    			      , a.productionlanguageoriginal    " + 
    			"    			      , a.Episodelanguageoriginal    " + 
    			"    			      , a.productiondurationhr    " + 
    			"    			      , a.productiondurationmm    " + 
    			"    			      , (productiondurationhr * 3600 + productiondurationmm * 60 + productiondurationss) as dur   " + //--15  " + 
    			"    			      , a.totalmusicdurHh    " + 
    			"    			      , a.totalmusicdurmm    " + 
    			"    			      , (totalmusicdurHh * 3600 + totalmusicdurmm * 60 + totalmusicdurss) as musdur   " + 
    			"    			      , a.postdurationhr    " + 
    			"    			      , a.postdurationmm   " + //--20 " + 
    			"    			      , (postdurationhr * 3600 + postdurationmm * 60 + postdurationss) as postdur   " + 
    			"    			      , a.versiontypecde    " + 
    			"    			      , a.versioncomment    " + 
    			"    			      , a.versionterritory    " + 
    			"    			      , a.yearofproduction   " + //--25 " + 
    			"    			      , a.countryofproduction    " + 
    			"    			      , a.originalairdt    " + 
    			"    			      , a.avidentifier    " + 
    			"    			      , a.Avidentifiersource    " + 
    			"    			      , a.productionidentifiersource   " + //--30  " + 
    			"    			      , a.productionidentifier    " + 
    			"    			      , a.avinterestedpartyrole    " + 
    			"    			      , a.nameofavinterestedparty    " + 
    			"    			      , a.networkstation    " + 
    			"    			      , a.territoryoffirstbroadcast   " + //-- 35 " + 
    			"    			      , a.directlicenseind    " + 
    			"    			      , a.rotationflag    " + 
    			"    			      , a.umbrellaindicator    " + 
    			"    			      , a.microFilmreel    " + 
    			"    			      , a.microfilmframe   " + //--40 " + 
    			"    			      , a.asctheme    " + 
    			"    			      , a.programfirstdistribution    " + 
    			"    			      , a.expmusiccount    " + 
    			"    			      , a.totalmusiccount    " + 
    			"    			      , a.programcompleteind   " + //--45 " + 
    			"    			      , a.AVFix    " + 
    			"    			      , a.alternateCueSheetType    " + 
    			"    			      , a.rotationalind    " + 
    			"    			      , a.verificationindicator    " + 
    			"    			      , a.verificationuserid   " + //--50 " + 
    			"    			      , a.verficationdate    " + 
    			"    			      , a.SpdRptInd    " + 
    			"    			      , a.SpdCalcMusInd    " + 
    			"    			      , a.SpdRptMusInd    " + 
    			"    			      , a.SpdCalcSplBmiInd   " + //--55 " + 
    			"    			      , a.SpdRptSplBmiInd    " + 
    			"    			      , a.SpdCalcSplSscInd    " + 
    			"    			      , a.SpdRptSplSscInd    " + 
    			"    			      , a.SpdCalcSplOthInd    " + 
    			"    			      , a.SpdRptSplOthInd   " + //--60 " + 
    			"    			      , a.SpdUpdTs    " + 
    			"    			      , a.distyp    " + 
    			"    			      , a.analysisind    " + 
    			"    			      , a.analysisdatetime    " + 
    			"    			      , a.analysispdateid   " + //--65 " + 
    			"    			      , a.titlecodecompletion    " + 
    			"    			      , a.createid    " + 
    			"    			      , convert(varchar, createdatetime, 121) as createdatetime    " + 
    			"    			      , a.updateid    " + 
    			"    			      , convert(varchar, updatedatetime, 121) as updatedatetime   " + //--70 " + 
    			"    			      , a.avicode    " + 
    			"    			      , a.deleteflag    " + 
    			"    			      , a.umbrellaelements    " + 
    			"    			      , a.datasource    " + 
    			"    			      , a.CueSheetStatus   " + //--75 " + 
    			"    			      , a.errorStatus    " + 
    			"    			      , a.delegationstatus    " + 
    			"    			      , a.matchreviewstatus    " + 
    			"    			      , a.codingcomplete    " + 
    			"    			      , a.prioritylevel   " + //--80 " + 
    			"    			      , a.attachmentindicator    " + 
    			"    			      , a.cuesheetorigin    " + 
    			"    			      , a.mediatype    " + 
    			"    			      , a.assigneduser    " + 
    			"    			      , a.Status   " + //--85 " + 
    			"    			      , a.seriesmastind    " + 
    			"    			      , a.seriesrotationalind    " + 
    			"    			      , a.seriesreportind    " + 
    			"    			      , a.seriescalcmultipler    " + 
    			"    			      , a.seriesreportedmultipler  " + // --90 " + 
    			"    			      , a.seasonnumber    " + 
    			"    			      , a.requeststatus    " + 
    			"    			      , a.duplicateIndicator    " + 
    			"    			      , a.duplicateReasonCode    " + 
    			"    			      , a.seriesPreviousName   " + //--95 " + 
    			"    			      , a.programPreviousName    " + 
    			"    			      , a.seriesCategory    " + 
    			"    			      , a.seriesVersion    " + 
    			"    			      , a.seriesDuration    " + 
    			"    			      , a.submitterSeriesCode   " + //--100 " + 
    			"    			      , a.masterNumber    " + 
    			"    			      , a.partNumber    " + 
    			"    			      , a.animationIndicator    " + 
    			"    			      , a.runtimeDuration    " + 
    			"    			      , a.masterfromDate   " + //--105 " + 
    			"    			      , a.masterToDate    " + 
    			"    			      , a.versionNumber    " + 
    			"    			      , a.revisionDate    " + 
    			"    			      , a.submitterProgramCode    " + 
    			"    			      , a.requestorProgramCode   " + //--110 " + 
    			"    			      , a.seriesIdentifier    " + 
    			"    			      , a.originalProgramCode    " + 
    			"    			      , a.productionTitleMatchpercent    " + 
    			"    			      , a.episodeTitleMatchpercent    " + 
    			"    			      , a.processStatus   " + //--115 " + 
    			"    			      , a.syndicatedNumber    " + 
    			"    			      , case when a.format = 'Nonelectronic' then 'manual' else a.format end as format   " + //--117, " + 
    			"					  , b.creatorId as cinfo_creatorid " + 
    			"					  , b.cuesheetclassuficationtype as cinfo_class " + 
    			"					  , b.CueSheetPreparedBy as cinfo_preparedby " + //120
    			"					  , b.Filecreationdate as cinfo_filecreationdt " + 
    			"					  , b.filename as  cinfo_filename " + 
    			"					  , b.roleofcuesheetprovider as cinfo_role " + 
    			"					  , b.Cuesheetprovider as Cuesheetprovider " + 
    			"    			  FROM  (select * from DM_Program_Series x where x.pgmcde between "+minProgramCode+" and " +maxprogramCode +") a " + 
    			"				  	left outer join ( " + 
    			"    					select * from ( " + 
    			"        					select *, row_number() over ( " + 
    			"            					partition by programcode " + 
    			"            					order by filecreationdate desc " + 
    			"        					) as row_num " + 
    			"        				from DM_Cuesht_Info where  programcode between "+minProgramCode+" and " +maxprogramCode  + 
    			"    					) as ordered_cue_info " + 
    			"    				where ordered_cue_info.row_num = 1 " + 
    			" ) as b " + 
    			" on a.pgmcde = b.programcode";
    	*/
    	 List<Production> itemList = new ArrayList<>();
    	 try ( final PreparedStatement pstmt = con.prepareStatement(sql);
       			 final ResultSet rs = pstmt.executeQuery();) {
    		
    		Production muItem = null;
    		while(rs.next()) {
    			muItem = new Production();
    			muItem.setProgramAction(rs.getString(1));
    			muItem.setProgramCode(rs.getString(4));
    			muItem.setSeriesCode(rs.getString(5));
    			
    			muItem.setProductionType(rs.getString(6));
    			muItem.setProductionCategoryCode(rs.getString(7));
    			muItem.setProductionTitle(rs.getString(8));
    			muItem.setEpisodeTitle(rs.getString(9));
    			muItem.setEpisodeNumber(rs.getString(10));
    			muItem.setProductionLanguageOriginal(rs.getString(11));
    			muItem.setEpisodeLanguageOriginal(rs.getString(12));
    			muItem.setProductionDuration(rs.getString(15));
    			muItem.setTotalMusicDuration(rs.getString(18));
    			
    			muItem.setPostDuration(rs.getString(21));
    			muItem.setVersionTypeCode(rs.getString(22));
    			muItem.setVersionComment(rs.getString(23));
    			muItem.setVersionTerritory(rs.getString(24));
    			muItem.setYearOfProduction(rs.getString(25));
    			
    			muItem.setCountryOfProduction(rs.getString(26));
    			muItem.setOriginalAirDate(rs.getString(27));
    			muItem.setAvIdentifier(rs.getString(28));
    			muItem.setAvIdentifierSource(rs.getString(29));
    			muItem.setProductionIdentifierSource(rs.getString(30));
    			
    			muItem.setProductionIdentifier(rs.getString(31));
    			muItem.setNetworkStation(rs.getString(34));
    			muItem.setTerritoryOfFirstBroadcast(rs.getString(35));
    			
    			muItem.setDirectLicenseIndicator(rs.getString(36));
    			muItem.setProgramRotationalIndicator(rs.getString(37));
    			muItem.setUmbrellaIndicator(rs.getString(38));
    			muItem.setMicroFilmFrame(rs.getString(40));
    			muItem.setMicroFilmReel(rs.getString(39));
    			
    			muItem.setAscapTheme(rs.getString(41));
    			muItem.setProgramFirstDistribution(rs.getString(42));
    			muItem.setExpectedMusicCount(rs.getString(43));
    			muItem.setTotalMusicCount(rs.getString(44));
    			muItem.setProgramCompleteIndicator(rs.getString(45));
    			
    			muItem.setAlternateCueSheetType(rs.getString(47));
    			muItem.setProgramRotationalIndicator(rs.getString(48));
    			muItem.setVerificationCompleteIndicator(rs.getString(49));
    			muItem.setVerificationUserId(rs.getString(50));
    			
    			muItem.setVerificationDate(rs.getString(51));
    			muItem.setSpdbProgramReportIndicator(rs.getString(52));
    			muItem.setSpdbCalculatedMusicIndicator(rs.getString(53));
    			muItem.setSpdbReportMusicIndicator(rs.getString(54));
    			muItem.setSpdbCalculatedSplitWorkBmiIndicator(rs.getString(55));
    			
    			muItem.setSpdbReportedSplitWorkBmiIndicator(rs.getString(56));
    			muItem.setSpdbCalculatedSplitWorkSesac(rs.getString(57));
    			muItem.setSpdbReportedSplitWorkSesacIndicator(rs.getString(58));
    			muItem.setSpdbCalculatedSplitWorkOtherIndicator(rs.getString(59));
    			muItem.setSpdbCalculatedReportedSplitWorkOtherIndicator(rs.getString(60));
    			
    			muItem.setSpdbUpdateDateTime(rs.getString(61));
    			muItem.setDistributionType(rs.getString(62));
    			muItem.setAnalysisIndicator(rs.getString(63));
    			muItem.setAnalysisDateTime(rs.getString(64));
    			muItem.setAnalysisUserId(rs.getString(65));
    			
    			muItem.setCodingComplete(rs.getString(66));
    			muItem.setShowCreateId(rs.getString(67));
    			muItem.setShowCreateDateTime(rs.getString(68));
    			muItem.setShowUpdateId(rs.getString(69));
    			muItem.setShowUpdateDateTime(rs.getString(70));
    			
    			muItem.setAviCode(rs.getString(71));
    			muItem.setDeleteFlag(rs.getString(72));
    			muItem.setDataSource(rs.getString(74));
    			muItem.setCuesheetStatus(rs.getString(75));
    			
    			muItem.setErrorStatus(rs.getString(76));
    			muItem.setDelegationStatus(rs.getString(77));
    			muItem.setMatchReviewStatus(rs.getString(78));
    			muItem.setCodingComplete(rs.getString(79));
    			muItem.setPriorityLevel(rs.getString(80));
    			
    			muItem.setAttachmentIndicator(rs.getString(81));
    			muItem.setCuesheetOrigin(rs.getString(82));
    			muItem.setMediaType(rs.getString(83));
    			muItem.setAssignedUser(rs.getString(84));
    			
    			
    			muItem.setSeriesMasterIndicator(rs.getString(86));
    			muItem.setSeriesRotationalIndicator(rs.getString(87));
    			muItem.setSpdbSeriesReportIndicator(rs.getString(88));
    			muItem.setSpdbseriesCalculatedMultipler(rs.getString(89));
    			muItem.setSpdbSeriesReportedMultipler(rs.getString(90));
    			
    			muItem.setSeasonNumber(rs.getString(91));
    			muItem.setRequestStatus(rs.getString(92));
    			muItem.setDuplicateIndicator(rs.getString(93));
    			muItem.setDuplicateReasonCode(rs.getString(94));
    			muItem.setSeriesPreviousName(rs.getString(95));
    			
    			muItem.setProgramPreviousName(rs.getString(96));
    			muItem.setSubmitterSeriesCode(rs.getString(100));
    			
    			muItem.setPartNumber(rs.getString(102));
    			muItem.setAnimationIndicator(rs.getString(103));
    			muItem.setMasterFromDate(rs.getString(105));
    			

    			muItem.setMasterToDate(rs.getString(106));
    			muItem.setVersionNumber(rs.getString(107));
    			muItem.setRevisionDate(rs.getString(108));
    			muItem.setSubmitterProgramCode(rs.getString(109));
    			
    			muItem.setOriginalProgramCode(rs.getString(112));
    			muItem.setProductionTitleMatchScore(rs.getString(113));
    			muItem.setEpisodeTitleMatchScore(rs.getString(114));
    			
    			muItem.setSyndicatedNumber(rs.getString(116));
    			muItem.setFormat(rs.getString(117));
    			muItem.setCreatorId(rs.getString(118));
    			muItem.setCuesheetClassificationType(rs.getString(119));
    			muItem.setCuesheetPreparedBy(rs.getString(120));
    			muItem.setFileCreationDate(rs.getString(121));
    			muItem.setFileName(rs.getString(122));
    			muItem.setRoleOfCuesheetProvider(rs.getString(123));
    			muItem.setCuesheetProvider(rs.getString(124));
    			
    			itemList.add(muItem);
    			
    		}
    		
    	}
    	catch(Exception e) {
    		LOG.error(e);
    	}
    	finally {
    	}
    	/* LOG.info(" Done Selecting data for Production"); */
    	
    	String sql1 = "insert into  cue2 . cue_sht_hdr  ( " + 
    			" file_id  , " + 
    			" ser_cde  , " + 
    			" pgm_cde  , " + 
    			" prod_ttl  , " + 
    			" ep_ttl  , " +  //5
    			" pgm_act_orig  , " + 
    			" ser_act_orig  , " + 
    			" anlys_dt , " + 
    			" anlys_ind  , " + 
    			" anlys_user_id  , " + //10
    			" asc_thm  , " + 
    			" asgd_user  , " + 
    			" atch_ind ,  " + 
    			" avi_cde  , " + 
    			" av_id  ," + //15
    			" av_id_src,   " + 
    			" prod_origin ,  " + 
    			" coding_cmplt   , " + 
    			" cntry_of_prod   , " + 
    			" cue_mch_cmplt_dt  , " + //20 
    			" cue_sht_orig , " +  
    			" cue_sht_stat  , " + 
    			" data_src  , " + 
    			" deleg_stat  , " + 
    			" dir_lic_ind   , " + //25
    			" dist_typ  , " + 
    			" ep_lng  , " + 
    			" ep_nr  , " + 
    			" err_stat , " + 
    			" exp_mus_cnt   , " + //30
    			" mch_rev_stat , " + 
    			" prod_ttl_mch_scr   , " + 
    			" ep_ttl_mch_scr   , " + 
    			" media_typ  , " + 
    			" mcr_film_fr   , " + //40
    			" mcr_film_rl   , " + 
    			" ntwrk_sta   , " + 
    			" orig_air_dt  , " + 
    			" orig_pgm_cde   , " + 
    			" post_dur   , " + 
    			" pri_lvl  , " + 
    			" prod_cat_cde  , " + 
    			" prod_dur   , " + 
    			" pgm_prod_id  , " + 
    			" pgm_prod_id_src  , " + 
    			" prod_lng  , " + //55
    			" prod_typ ,   " + 
    			" pgm_cmplt_ind   , " + 
    			" dup_ind  ,  " + //60
    			" dup_cde   , " + 
    			" syn_nr   , " + 
    			" pgm_first_dist  , " + 
    			" pgm_rot_ind   , " +//65 
    			" req_stat  , " + 
    			" sea_nr   , " + 
    			" spdb_ser_calc_mlt , " + 
    			" ser_mstr_ind,    " +
    			" ser_rot_ind   , " + 
    			" spdb_ser_rpt_mlt  , " + 
    			" spdb_ser_rpt_ind   , " + 
    			" spdb_calc_mus_ind  , " + 
    			" spdb_calc_spl_wrk_bmi_ind   , " + //75
    			" spdb_calc_spl_wrk_ssc   , " + 
    			" spdb_calc_spl_wrk_oth_ind   , " + 
    			" spdb_pgm_rpt_ind , " + 
    			" spdb_rpt_spl_wrk_bmi_ind   , " + 
    			" spdb_rpt_spl_wrk_ssc_ind   , " + //80
    			" spdb_rpt_spl_wrk_oth_ind  , " + 
    			" spdb_rpt_mus_ind   , " + 
    			" spdb_upd_dt , " + 
    			" subm_pgm_cde  , " + 
    			" subm_series_code,   " + //85
    			" terr_of_first_brdcst , " + 
    			" tot_mus_cnt   , " + 
    			" tot_mus_dur   , " + 
    			" umb_ind  ,  " + 
    			" vrfy_cmplt_ind   , " + 
    			" vrfy_user_id  , " + 
    			" ver_cmts   , " + 
    			" ver_typ , " + //95
    			" yr_of_prod   , " + 
    			" ser_prev_name   , " + 
    			" pgm_prev_name   , " + 
    			" mstr_from_dt , " + 
    			" mstr_to_dt, " + //105
    			" alt_cue_sht_typ  , " + 
    			" fmt_ver  , " + 
    			" revn_dt  , " + 
    			" anm_ind,    " + 
    			" part_nr   , " + //115
    			" proc_stat  , " + 
    			" subm_cre_id   , " + 
    			" cue_sht_class , " + 
    			" cue_sht_prep_by   , " + //120 
    			" cue_sht_subm   , " + 
    			" file_cre_dt  , " + 
    			" role_of_cue_sht_prov   , " + 
    			" orig_ser_cde   , " + 
    			" ser_cat  , " + 
    			" ser_ver  , " + 
    			" mstr_nr   , " + 
    			" rt_dur,    " + //135
    			" req_pgm_cde   , " + 
    			" ser_prod_id  , " + 
    			" vrfy_dt  , " + //140
    			" terr_of_use  , " + 
    			" cue_sht_revn   , " + 
    			" del_flg   , " + 
    			" cre_id,  " + //145
    			" cre_dt,  " + 
    			" upd_id,  " + 
    			" upd_dt,  " + 
    			" fmt  " + 
    			")" +
    	
    			"values ( " + 
    			"?,?,?,?,?, " + 
    			"?,?,?,?,?, " + 
    			"?,?,?,?,?, " + 
    			"?,?,?,?,?, " + //20
    			"?,?,?,?,?, " + 
    			"?,?,?,?,?, " + 
    			"?,?,?,?,?, " + 
    			"?,?,?,?,?, " + //40
    			"?,?,?,?,?, " + 
    			"?,?,?,?,?, " + 
    			"?,?,?,?,?, " + 
    			"?,?,?,?,?, " + //60
    			"?,?,?,?,?, " + 
    			"?,?,?,?,?, " + 
    			"?,?,?,?,?, " + 
    			"?,?,?,?,?, " + //80
    			"?,?,?,?,?, " + 
    			"?,?,?,?,?, " + 
    			"?,?,?,?,?, " + 
    			"?,?,?,?,?, " + //100
    			"?,?,?,?,?, " + 
    			"?,?,?,?,?, " + 
    			"?,?,?)";
    	try ( final PreparedStatement pstmt1 = pgCon.prepareStatement(sql1);) {
    		int rowNum = 0;
    		for(Production muItem : itemList) {
    			rowNum ++;
    			
    			pstmt1.setLong(1, 0);
    			pstmt1.setLong(2, StringUtils.isBlank(muItem.getSeriesCode())?0:Long.parseLong(muItem.getSeriesCode()));
    			pstmt1.setLong(3, Long.parseLong(muItem.getProgramCode()));
    			pstmt1.setString(4, StringUtils.isBlank(muItem.getProductionTitle())?null:muItem.getProductionTitle().trim());
    			pstmt1.setString(5, StringUtils.isBlank(muItem.getEpisodeTitle())?null:muItem.getEpisodeTitle().trim());
    			pstmt1.setString(6, null);
    			pstmt1.setString(7, null);
    			pstmt1.setObject(8, StringUtils.isBlank(muItem.getAnalysisDateTime())?null:java.sql.Timestamp.valueOf(muItem.getAnalysisDateTime()));
    			pstmt1.setString(9, muItem.getAnalysisIndicator());
    			pstmt1.setString(10, muItem.getAnalysisUserId());
    			pstmt1.setString(11, muItem.getAscapTheme());
    			pstmt1.setString(12, muItem.getAssignedUser());
    			pstmt1.setString(13, muItem.getAttachmentIndicator());
    			pstmt1.setString(14, muItem.getAviCode());
    			pstmt1.setString(15, null);
    			pstmt1.setString(16, null);
    			pstmt1.setString(17, muItem.getProgramOrigin());
    			pstmt1.setString(18, muItem.getCodingComplete());
    			pstmt1.setObject(19, StringUtils.isBlank(muItem.getCountryOfProduction())?null:NumberUtils.toInt(muItem.getCountryOfProduction()));
    			pstmt1.setObject(20, null);
    			pstmt1.setString(21, muItem.getCuesheetOrigin());
    			pstmt1.setString(22, muItem.getCuesheetStatus());
    			pstmt1.setString(23, muItem.getDataSource());
    			pstmt1.setString(24, muItem.getDelegationStatus());
    			pstmt1.setString(25, muItem.getDirectLicenseIndicator());
    			pstmt1.setString(26, muItem.getDistributionType());
    			pstmt1.setString(27,StringUtils.isBlank(muItem.getEpisodeLanguageOriginal())?null:muItem.getEpisodeLanguageOriginal().trim().substring(0,2));
    			pstmt1.setString(28, muItem.getEpisodeNumber());
    			pstmt1.setInt(29, StringUtils.isBlank(muItem.getErrorStatus())?0:Integer.parseInt(muItem.getErrorStatus()));
    			pstmt1.setInt(30, StringUtils.isBlank(muItem.getExpectedMusicCount())?0:Integer.parseInt(muItem.getExpectedMusicCount()));
	   			pstmt1.setString(31, muItem.getMatchReviewStatus());
				pstmt1.setObject(32, StringUtils.isBlank(muItem.getProductionTitleMatchScore())?null:NumberUtils.toInt(muItem.getProductionTitleMatchScore()));
				pstmt1.setObject(33, StringUtils.isBlank(muItem.getEpisodeTitleMatchScore())?null:NumberUtils.toInt(muItem.getEpisodeTitleMatchScore()));
				pstmt1.setString(34, muItem.getMediaType());
				pstmt1.setInt(35, StringUtils.isBlank(muItem.getMicroFilmFrame())?0:Integer.parseInt(muItem.getMicroFilmFrame()));
				pstmt1.setInt(36,StringUtils.isBlank(muItem.getMicroFilmReel())?0:Integer.parseInt(muItem.getMicroFilmReel()));
	   			pstmt1.setString(37, muItem.getNetworkStation());
	   			pstmt1.setObject(38, (!StringUtils.isBlank(muItem.getOriginalAirDate()))&&dateUtilsYMD.isValid(muItem.getOriginalAirDate())?java.sql.Date.valueOf(muItem.getOriginalAirDate()):null);
	   			pstmt1.setLong(39, StringUtils.isBlank(muItem.getOriginalProgramCode())?0:Long.parseLong(muItem.getOriginalProgramCode()));
	   			pstmt1.setObject(40, StringUtils.isBlank(muItem.getPostDuration())?null:NumberUtils.toInt(muItem.getPostDuration()));
				pstmt1.setString(41, muItem.getPriorityLevel());
				pstmt1.setString(42, muItem.getProductionCategoryCode());
				pstmt1.setObject(43, StringUtils.isBlank(muItem.getProductionDuration())?null:NumberUtils.toInt(muItem.getProductionDuration()));
	   			pstmt1.setString(44, muItem.getProductionIdentifier());
	   			pstmt1.setString(45, muItem.getProductionIdentifierSource());
	   			pstmt1.setString(46, muItem.getProductionLanguageOriginal());
				pstmt1.setString(47, muItem.getProductionType());
				pstmt1.setString(48, muItem.getProgramCompleteIndicator());
				pstmt1.setString(49, muItem.getDuplicateIndicator());
				pstmt1.setObject(50, StringUtils.isBlank(muItem.getDuplicateReasonCode())?null:NumberUtils.toInt(muItem.getDuplicateReasonCode()));
	   			pstmt1.setString(51, muItem.getSyndicatedNumber());
	   			pstmt1.setObject(52, (!StringUtils.isBlank(muItem.getProgramFirstDistribution()))&&dateUtilsYMD.isValid(muItem.getProgramFirstDistribution())?java.sql.Date.valueOf(muItem.getProgramFirstDistribution()):null);
	   			pstmt1.setString(53, muItem.getProgramRotationalIndicator());
	   			pstmt1.setString(54, muItem.getRequestStatus());
				pstmt1.setObject(55, null);
				pstmt1.setDouble(56, StringUtils.isBlank(muItem.getSpdbseriesCalculatedMultipler())?0:Double.parseDouble(muItem.getSpdbseriesCalculatedMultipler()));
				pstmt1.setString(57, muItem.getSeriesMasterIndicator());
				pstmt1.setString(58, muItem.getSeriesRotationalIndicator());
	   			pstmt1.setDouble(59, StringUtils.isBlank(muItem.getSpdbSeriesReportedMultipler())?0:Double.parseDouble(muItem.getSpdbSeriesReportedMultipler()));
	   			pstmt1.setString(60, muItem.getSpdbSeriesReportIndicator());
	   			pstmt1.setString(61, muItem.getSpdbCalculatedMusicIndicator());
	   			pstmt1.setString(62, muItem.getSpdbCalculatedSplitWorkBmiIndicator());
	   			pstmt1.setString(63, muItem.getSpdbCalculatedSplitWorkSesac());
				pstmt1.setString(64, muItem.getSpdbCalculatedSplitWorkOtherIndicator());
				pstmt1.setString(65, muItem.getSpdbProgramReportIndicator());
				pstmt1.setString(66, muItem.getSpdbReportedSplitWorkBmiIndicator());
				pstmt1.setString(67, muItem.getSpdbReportedSplitWorkSesacIndicator());
				pstmt1.setString(68, muItem.getSpdbCalculatedSplitWorkOtherIndicator());
	   			pstmt1.setString(69, muItem.getSpdbReportMusicIndicator());
	   			pstmt1.setObject(70, StringUtils.isBlank(muItem.getSpdbUpdateDateTime())?null:java.sql.Timestamp.valueOf(muItem.getSpdbUpdateDateTime())); 
	   			pstmt1.setString(71, muItem.getSubmitterProgramCode());
	   			pstmt1.setString(72, muItem.getSubmitterSeriesCode());
	   			pstmt1.setObject(73, StringUtils.isBlank(muItem.getTerritoryOfFirstBroadcast())?null:NumberUtils.toInt(muItem.getTerritoryOfFirstBroadcast()));
	   			//pstmt1.setString(73, muItem.getTerritoryOfFirstBroadcast());
				pstmt1.setInt(74, StringUtils.isBlank(muItem.getTotalMusicCount())?0:Integer.parseInt(muItem.getTotalMusicCount()));
				pstmt1.setObject(75, StringUtils.isBlank(muItem.getTotalMusicDuration())?null:NumberUtils.toInt(muItem.getTotalMusicDuration()));
				pstmt1.setString(76, muItem.getUmbrellaIndicator());
	   			pstmt1.setString(77, muItem.getVerificationCompleteIndicator());
	   			pstmt1.setString(78, muItem.getVerificationUserId());
	   			pstmt1.setString(79, muItem.getVersionComment());
	   			pstmt1.setString(80, muItem.getVersionTypeCode());
				pstmt1.setInt(81, StringUtils.isBlank(muItem.getYearOfProduction())?0:Integer.parseInt(muItem.getYearOfProduction()));
				pstmt1.setString(82, muItem.getSeriesPreviousName());
				pstmt1.setString(83, muItem.getProgramPreviousName());
	   			pstmt1.setObject(84,null);
	   			pstmt1.setObject(85, null);
				pstmt1.setString(86, muItem.getAlternateCueSheetType());
				pstmt1.setObject(87, null);
				pstmt1.setObject(88, null);
	   			pstmt1.setObject(89, muItem.getAnimationIndicator());
	   			pstmt1.setInt(90, StringUtils.isBlank(muItem.getPartNumber())?0:Integer.parseInt(muItem.getPartNumber()));
				pstmt1.setString(91, "BUSVLD");
				pstmt1.setObject(92, StringUtils.isBlank(muItem.getCreatorId())?null:Integer.parseInt(muItem.getCreatorId()));
				pstmt1.setString(93, StringUtils.isBlank(muItem.getCuesheetClassificationType())?"NEW":muItem.getCuesheetClassificationType());
				pstmt1.setString(94, muItem.getCuesheetPreparedBy());
	   			pstmt1.setString(95, (!StringUtils.isBlank(muItem.getCuesheetProvider()))?muItem.getCuesheetProvider():null);
	   			pstmt1.setObject(96, (!StringUtils.isBlank(muItem.getFileCreationDate()))&&dateUtilsYMDHMS.isValid(muItem.getFileCreationDate())?java.sql.Timestamp.valueOf(muItem.getFileCreationDate()):null);
	   			pstmt1.setString(97, muItem.getRoleOfCuesheetProvider());
				pstmt1.setObject(98, null);
				pstmt1.setString(99, null);
				pstmt1.setString(100, null);
	   			pstmt1.setObject(101, null);
	   			pstmt1.setObject(102, null);
				pstmt1.setObject(103, null);
				pstmt1.setString(104, null);
				pstmt1.setObject(105, StringUtils.isBlank(muItem.getVerificationDate())?null:java.sql.Timestamp.valueOf(muItem.getVerificationDate()));
				pstmt1.setString(106, null);
	   			pstmt1.setObject(107, null);
	   			pstmt1.setString(108, StringUtils.isBlank(muItem.getDeleteFlag())?null:muItem.getDeleteFlag().trim());
	   			pstmt1.setString(109, StringUtils.isBlank(muItem.getShowCreateId())?null:muItem.getShowCreateId().trim());
	   			pstmt1.setObject(110, StringUtils.isBlank(muItem.getShowCreateDateTime())?null:java.sql.Timestamp.valueOf(muItem.getShowCreateDateTime()));
	   			pstmt1.setString(111, StringUtils.isBlank(muItem.getShowUpdateId())?null:muItem.getShowUpdateId().trim());
	   			pstmt1.setObject(112, StringUtils.isBlank(muItem.getShowUpdateDateTime())?null:java.sql.Timestamp.valueOf(muItem.getShowUpdateDateTime()));
	   			pstmt1.setString(113, StringUtils.isBlank(muItem.getFormat())?null:muItem.getFormat().toLowerCase().trim());
    			
    			pstmt1.addBatch();
    			
    			if(rowNum % 100000 == 0 ) { 
					pstmt1.executeBatch();
					pstmt1.clearBatch();
					LOG.info("Updated Batchfor CueHeader: " + rowNum);
				} 
    		}
    		pstmt1.executeBatch();
    		/* LOG.info("Insert Completed for CueHeader: " + rowNum); */
    	}
    	catch(java.sql.BatchUpdateException e) {

    		LOG.error("ERROR IN Hedear Insert: " + e.getNextException());
    	}
    	catch(Exception e) {
    		LOG.error(e);
    	}
    	finally {
    	}
    	
    	return count;
    }
    
    public int insertAltTitles(Entry<Integer, Integer> entry, Connection con, Connection pgCon, Boolean incremental, List<String> programCodesList) throws SQLException {
    	/* LOG.info(" Starting insertAltTitles " + entry); */
    	int minProgramCode = -1;
        int maxprogramCode = -1; 
        if(!incremental) {
        	 minProgramCode = entry.getKey();
             maxprogramCode = entry.getValue();
        }
    	int count = 0;
    	
    	String sql =  ShowQueryConstants.SELECT_ALT_TTL_1;
        if(!incremental) {
    		sql += " between "+minProgramCode+" and " +maxprogramCode;
    	}
    	else {
    		sql += " in ("+programCodesList.stream()
            .collect(Collectors.joining(","))+") ";
    	}
    	sql += ShowQueryConstants.SELECT_ALT_TTL_2;
        if(!incremental) {
    		sql += " between "+minProgramCode+" and " +maxprogramCode;
    	}
    	else {
    		sql += " in ("+programCodesList.stream()
            .collect(Collectors.joining(","))+") ";
    	}

    	sql += ShowQueryConstants.SELECT_ALT_TTL_3;
    	/*
    	String sql =   
    			" SELECT A.akacde, A.akatyp, B.Productyontype, " + 
    			" A.akana, ltrim(rtrim(A.lngcde)), " + 
    			" B.PGMCDE  AS PGMCDE, " + 
    			" B.SerCde  AS SERCDE, " + 
    			" creuid, crets, upduid, updts " +
    			" FROM DM_TblSPuAKa A, " + 
    			" DM_Program_Series B " + 
    			" WHERE a.akacde = b.pgmcde " + 
    			" and a.akaTyp = 'P' " + 
    			" AND B.PGMCDE BETWEEN  "+minProgramCode+" AND  " + maxprogramCode +  
    			" UNION " + 
    			" select * from ( "+
    			" SELECT A.akacde AS akacde, A.akatyp, B.Productyontype, " + 
    			" A.akana, ltrim(rtrim(A.lngcde)) as lng, " + 
    			" CASE WHEN B.Productyontype = 'S' THEN 0 ELSE 0 END AS PGMCDE, " + 
    			" B.SerCde  AS SERCDE, " + 
    			" creuid, crets, upduid, updts " +
    			" FROM DM_TblSPuAKa A, " + 
    			" DM_Program_Series B " + 
    			" WHERE a.akacde = b.SERcde " + 
    			" and a.akaTyp = 'S' " + 
    			" AND B.PGMCDE  BETWEEN  "+minProgramCode+" AND "+maxprogramCode +
    			" ) outer1 group by akacde,  akatyp, Productyontype, akana, lng, PGMCDE, SERCDE, creuid, crets, upduid, updts  "; */
    	 List<AlternateProductionTitle> itemList = new ArrayList<>();
    	 try ( final PreparedStatement pstmt = con.prepareStatement(sql);
       			 final ResultSet rs = pstmt.executeQuery();) {
    		
    		AlternateProductionTitle muItem = null;
    		while(rs.next()) {
    			muItem = new AlternateProductionTitle();

    			muItem.setProgramCode(rs.getLong(6));
    			muItem.setSeriesCode(rs.getLong(7));
    			muItem.setProductionTitle(rs.getString(4));
    			muItem.setAlternateLanguage(rs.getString(5));
    			muItem.setTitleCreateId(rs.getString(8));
    			muItem.setTitleCreateDateTime(rs.getString(9));
    			muItem.setTitleUpdateId(rs.getString(10));
    			muItem.setTitleUpdateDateTime(rs.getString(11));
    			itemList.add(muItem);
    			
    		}
    		
    	}
    	catch(Exception e) {
    		LOG.error(e);
    	}
    	finally {
    	}
    	/* LOG.info(" Done Selecting data for insertAltTitles"); */
    	String sql1 = " INSERT INTO  cue2 . alt_ttl  (" + 
    			" pgm_cde ," + 
    			" ser_cde ," + 
    			" ser_act_orig ," + 
    			" pgm_act_orig ," + 
    			" prod_ttl ," + //5
    			" alt_lng ," + 
    			" cre_id, cre_dt, upd_id, upd_dt " + 
    			" )" +//15
    			"values ( " + 
    			"?,?,?,?,?, " + 
    			"?,?,?,?,? " +
    			")";
    	try ( final PreparedStatement pstmt1 = pgCon.prepareStatement(sql1);) {
    		int rowNum = 0;
    		for(AlternateProductionTitle muItem : itemList) {
    			rowNum ++;
    			pstmt1.setLong(1, muItem.getProgramCode());
    			pstmt1.setLong(2, muItem.getSeriesCode());
    			pstmt1.setString(3, null);
    			pstmt1.setString(4, null);
    			pstmt1.setString(5, StringUtils.isBlank(muItem.getProductionTitle())?null:muItem.getProductionTitle().trim());
    			pstmt1.setString(6, StringUtils.isBlank(muItem.getAlternateLanguage())?null: muItem.getAlternateLanguage().substring(0,2));
    			pstmt1.setString(7, StringUtils.isBlank(muItem.getTitleCreateId())?null:muItem.getTitleCreateId().trim());
    			pstmt1.setObject(8, StringUtils.isBlank(muItem.getTitleCreateDateTime())?null:java.sql.Timestamp.valueOf(muItem.getTitleCreateDateTime()));
    			pstmt1.setString(9, StringUtils.isBlank(muItem.getTitleUpdateId())?null:muItem.getTitleUpdateId().trim());
    			pstmt1.setObject(10, StringUtils.isBlank(muItem.getTitleUpdateDateTime())?null:java.sql.Timestamp.valueOf(muItem.getTitleUpdateDateTime()));
    			
    			pstmt1.addBatch();
    			
    			if(rowNum % 30000 == 0 || rowNum == itemList.size()) { 
					pstmt1.executeBatch();
					pstmt1.clearBatch();
					LOG.info("Updated Batch for Alt Titiles: " + rowNum);
				}
				
				
    		}
    		//pstmt1.executeBatch();
    		/* LOG.info(" Insert Completed for Alt Titiles: " + rowNum); */
    	} catch(Exception e) {
    		LOG.error(e);
    	}
    	
    	return count;
    }
    
    
    public int insertAVParties(Entry<Integer, Integer> entry, Connection con, Connection pgCon, Boolean incremental, List<String> programCodesList) throws SQLException {
    	/* LOG.info(" Starting insertAVParties " + entry); */

    	int minProgramCode = -1;
        int maxprogramCode = -1; 
        if(!incremental) {
        	 minProgramCode = entry.getKey();
             maxprogramCode = entry.getValue();
        }
    	int count = 0;
    	
    	String sql = ShowQueryConstants.SELECT_AV_PTY_1;
        if(!incremental) {
    		sql += " between "+minProgramCode+" and  "+maxprogramCode;
    	}
    	else {
    		sql += " in ("+programCodesList.stream()
            .collect(Collectors.joining(","))+") ";
    	}
    	/*
    	String sql = "select distinct pgmcde, role , ltrim(rtrim(DirActFstNA)), ltrim(rtrim(DirActLstNA)), creuid, crets, upduid, updts from DM_DIRaCT " + 
    			"	where pgmcde between "+minProgramCode+" and  "+maxprogramCode;
    	*/
    	
    	 List<AVInterestedParty> itemList = new ArrayList<>();
    	 try ( final PreparedStatement pstmt = con.prepareStatement(sql);
       			 final ResultSet rs = pstmt.executeQuery();) {
    		
    		AVInterestedParty item = null;
    		while(rs.next()) {
    			item = new AVInterestedParty();
    			item.setRole(rs.getString(2));
    			item.setProgramCode(rs.getLong(1));
    			item.setFirstName(rs.getString(3));
    			item.setLastNameOrCompanyName(rs.getString(4));
    			item.setAvCreateId(rs.getString(5));
    			item.setAvCreateDateTime(rs.getString(6));
    			item.setAvUpdateId(rs.getString(7));
    			item.setAvUpdateDateTime(rs.getString(8));
    			itemList.add(item);
    			
    		}
    		
    	}
    	catch(Exception e) {
    		System.err.println(e);
    	}
    	finally {
    	}
    	/* LOG.info(" Done Selecting data for AV Parties"); */
    	String sqlPg =  
    			" insert into cue2 . av_int_pty (" + 
    			" pgm_cde  ," + 
    			" av_int_pty_act_orig  ," + 
    			" first_name  ," + 
    			" last_name  ," + 
    			" role ," + 
    			" cre_id, cre_dt, upd_id, upd_dt ) " + 
    			" values (?,?,?,?,?,"
    			+ "?,?,?,?)  ";
    	try ( final PreparedStatement pstmt1 = pgCon.prepareStatement(sqlPg);) {
    		int rowNumAV = 0;
    		for(AVInterestedParty item : itemList) {
    			rowNumAV ++;
    			pstmt1.setLong(1, item.getProgramCode());
    			pstmt1.setString(2, null);
    			pstmt1.setString(3,item.getFirstName());
    			pstmt1.setString(4, item.getLastNameOrCompanyName());
    			pstmt1.setString(5, item.getRole());
    			pstmt1.setString(6, StringUtils.isBlank(item.getAvCreateId())?null:item.getAvCreateId().trim());
    			pstmt1.setObject(7, StringUtils.isBlank(item.getAvCreateDateTime())?null:java.sql.Timestamp.valueOf(item.getAvCreateDateTime()));
    			pstmt1.setString(8, StringUtils.isBlank(item.getAvUpdateId())?null:item.getAvUpdateId().trim());
    			pstmt1.setObject(9, StringUtils.isBlank(item.getAvUpdateDateTime())?null:java.sql.Timestamp.valueOf(item.getAvUpdateDateTime()));
    			
    			pstmt1.addBatch();
    			
    			if(rowNumAV % 40000 == 0 || rowNumAV == itemList.size()) { 
					pstmt1.executeBatch();
					pstmt1.clearBatch();
					LOG.info("Updated Batch for AVParties: " + rowNumAV);
				}
				
    		}
    		//pstmt1.executeBatch();
    		/* LOG.info("Insert Completed for AVParties: " + rowNum); */
    	}catch(java.sql.BatchUpdateException e) {
    		LOG.error("ERROR IN AVParties Insert: " + e.getNextException());
    	}catch(Exception e) {
    		LOG.error(e);
    	}
    	
    	return count;
    }
    
    
    
    public int insertErrors(Entry<Integer, Integer> entry, Connection con, Connection pgCon, Boolean incremental, List<String> programCodesList) throws SQLException {
    	/* LOG.info("Starting insertErrors " + entry); */

    	int minProgramCode = -1;
        int maxprogramCode = -1; 
        if(!incremental) {
        	 minProgramCode = entry.getKey();
             maxprogramCode = entry.getValue();
        }
    	int count = 0;
    	
    	String sql = ShowQueryConstants.SELECT_CUE_ERR_1;
        if(!incremental) {
    		sql += " between "+minProgramCode+" and  "+maxprogramCode;
    	}
    	else {
    		sql += " in ("+programCodesList.stream()
            .collect(Collectors.joining(","))+") ";
    	}
    	/*
    	
    	String sql = "select PgmCde, SeqNr, ERRORCODE, crets, creuid, updts, upduid FROM VW_program_errors a  " + 
    			"			where a.pgmcde between "+minProgramCode+" and  "+maxprogramCode;
    	*/
    	
    	 List<ProgramError> itemList = new ArrayList<>();
    	 try ( final PreparedStatement pstmt = con.prepareStatement(sql);
       			 final ResultSet rs = pstmt.executeQuery();) {
    		
    		ProgramError item = null;
    		while(rs.next()) {
    			item = new ProgramError();
    			item.setProgramCode(rs.getString(1));
    			item.setSequenceNumber(rs.getString(2));
    			item.setErrorCode(rs.getString(3));
    			item.setShowCreateDateTime(rs.getString(4));
    			item.setShowCreateUserId(rs.getString(5));
    			item.setShowUpdateDateTime(rs.getString(6));
    			item.setShowUpdateUserId(rs.getString(7));
    			
    			itemList.add(item);
    			
    		}
    		
    	}
    	catch(Exception e) {
    		LOG.error(e);
    	}
    	/* LOG.info(" Done Selecting data for Errors"); */
    	String sqlPg =  
    			"insert into cue2.cue_sht_err ( " + 
    			"pgm_cde, " + 
    			"seq_nr , " + 
    			"err_cde , " + 
    			"cre_id, " + 
    			"cre_dt, " + 
    			"upd_id, " + 
    			"upd_dt " + 
    			") values (?,?,?,?,?,?,?) on conflict on constraint unique_id_errcode do nothing ";
    	 try ( final PreparedStatement pstmt1 = pgCon.prepareStatement(sqlPg);) {
    		int rowNum = 0;
    		for(ProgramError item : itemList) {
    			rowNum ++;
    			pstmt1.setLong(1, Long.parseLong(item.getProgramCode()));
    			pstmt1.setObject(2, StringUtils.isBlank(item.getSequenceNumber())?null:NumberUtils.toInt(item.getSequenceNumber()));
    			pstmt1.setObject(3, StringUtils.isBlank(item.getErrorCode())?null:NumberUtils.toInt(item.getErrorCode()));
    			pstmt1.setString(4, item.getShowCreateUserId());
    			pstmt1.setObject(5, StringUtils.isBlank(item.getShowCreateDateTime())?null:java.sql.Timestamp.valueOf(item.getShowCreateDateTime()));
    			pstmt1.setString(6, item.getShowUpdateUserId());
    			pstmt1.setObject(7, StringUtils.isBlank(item.getShowUpdateDateTime())?null:java.sql.Timestamp.valueOf(item.getShowUpdateDateTime()));
    			
    			pstmt1.addBatch();
    			
    			if(rowNum % 45000 == 0 || rowNum == itemList.size()) { 
					pstmt1.executeBatch();
					pstmt1.clearBatch();
					LOG.info(" Updated Batch for Errors: " + rowNum);
				}
				
    		}
    		//pstmt1.executeBatch();
    	}catch(java.sql.BatchUpdateException e) {
    		LOG.error("ERROR IN cue errors Insert: " + e.getNextException());
    	}
    	catch(Exception e) {
    		LOG.error(e);
    	}
    	
    	return count;
    }

    }

