package com.ascap.cue2.cuesheet.showdataimporter.constants;

public  class ShowQueryConstants {

	public final static String DELETE_INC_STG_TABLE = "update DM_Incremental_PgmCde set status = 'COMPLETE', updatedatetime = getdate() where incremental_id <= ? and status = 'INPROGRESS' " ;
	
	public final static String GET_MUSIC_CONTENT = "SELECT "
		//	+ " -- Music_Content_Item DM_MusCtnUniverse  --- "
		    + " music.programCode as music_programCode "
			+ " ,music.action as cueAction "  //action & action timestamp and action user id are only for hist table. cueAction si different
		//	+ "   -- ,music.actiontimestamp " //not needed
		//	+ "   -- ,music.actionuserid " //not needed
			+ ",music.sequnecenumber as sequenceNumber"
			+ ",music.musictitle "
            + ",RIGHT('0' +cast(music.MusicdurationHH as varchar(10)),2) + "
            + "  RIGHT('0' +cast(music.musicdurationmm as varchar(10)),2) + "
            + "  RIGHT('0' +cast(music.musicdurationss as varchar(10)),2) as musicDuration "
            + ",RIGHT('0' +cast(music.starttimehh as varchar(10)),2) + "
            + "   RIGHT('0' +cast(music.starttimemm as varchar(10)),2) + "
            + "   RIGHT('0' +cast(music.starttimess as varchar(10)),2) as startTime "
			+ ",music.iswc "
			+ ",music.ISRC "
			+ ",music.musicusagecode "
			+ ",music.AscapWorkId "
			+ ",music.programoccurance as programOccurrence "
			+ ",music.performancecode "
			+ ",music.usetyp as useType "
			+ ",music.MusLibId as musicLibraryIndicator "
			+ ",music.AmbMusInd as ambientMusicIndicator "
			+ ",music.createdatetime as showCreateDateTime "
			+ ",music.createid as showCreateId "
			+ ",music.updateid as showUpdateId "
			+ ",music.updatedattime as showUpdateDateTime "
			+ ",music.matchuserid "
			+ ",music.matchdattime as matchDateTime "
		//	+ ",music.[source] " Not needed
			+ ",music.cuematchtype "
			+ ",music.directsourceind as directSourceIndicator "
			+ ",music.publicDomainIndicator as publicDomainIndicator "
			+ ",music.publicDomainTerritory "
			+ ",music.submitterWorkID as submitterWorkId"
		   +  ",party.pgmcde  as party_programcode "
			+ ",party.seqnr as party_sequenceNumber "
			+ ",party.memebertypecode "
			+ ",party.societycode "
		//	+ ",party.nameofmusicintrestedparty as nameOfMusicInterestedParty "
            + ",ltrim(rtrim(party.firstName)) as music_firstName "
            + ",ltrim(rtrim(party.lastName)) as music_lastName "
			+ ",ltrim(rtrim(party.ipinumber)) as ipiNumber "
			+ ",ltrim(rtrim(party.musicintrestedpartyshare)) as musicInterestedPartyShare "
			+ ",ltrim(rtrim(party.MusicPerformer)) as  MusicPerformer "
			//+ ",party.submitterIpNumber as  " // need to be added in avro
			//Music errors
			+ " ,music_error.pgmCde  as music_error_programCode"
			+ " ,music_error.SeqNr   as music_error_sequenceNumber"
			+ " ,LTRIM(RTRIM(CAST(RvsTyp as VARCHAR)))     as revisionType"
			 //	"      --,ExcTtlCde  as" + not needed
			 //	"     --,ExcUpdInd  as" + not needed
			//	"      --,AtrVal     as" + not needed
			+ "  ,errorcode     as errorCode"
			+ "  ,LTRIM(RTRIM(CAST(errromessage as VARCHAR)))     as errorMessage"
			+ "  ,LTRIM(RTRIM(CAST(Cue2ErrorSeverity as VARCHAR)))      as errorSeverity "
			+ "  ,CreTs      as showCreateDateTime"
			+ "  ,LTRIM(RTRIM(CAST(CreUid  as VARCHAR)))     as showCreateUserId"
			+ "  ,UpdTs      as showUpdateDateTime"
			+ "  ,LTRIM(RTRIM(CAST(UpdUid  as VARCHAR)))     as showUpdateUserId"
			+ "     from  (Select * from DM_MusCtnUniverse where  programcode between  convert(int,?) and convert(int,?) ) music "
			+ "     left join (select distinct a.pgmcde, a.seqnr ,LTRIM(RTRIM(CAST(b.Cue2MusicInterestedPartyRoleCode as VARCHAR))) memebertypecode ,a.SocCde societycode "
			+          ", ltrim(rtrim(CAST(a.EntPtyFstNa as VARCHAR))) firstName, ltrim(rtrim(CAST(a.EntPtyLstNa as VARCHAR))) lastName "
			+          ", LTRIM(RTRIM(CAST(a.IPI as VARCHAR))) ipinumber , RIGHT('0000' +cast(convert(int, a.EntShr*100) AS VARCHAR(5)),5) as musicintrestedpartyshare , case when isnull(LTRIM(RTRIM(CAST(a.MbrTyp as VARCHAR))) , 'X') ='PF' then ltrim(rtrim(CAST(a.EntPtyFstNa as VARCHAR)))+' '+ltrim(rtrim(CAST(a.EntPtyLstNa as VARCHAR))) "
		    +          " else null end as MusicPerformer "
			+          ",LTRIM(RTRIM(CAST(a.SubmitterNumber as VARCHAR))) submitterIpNumber "
			+          "from tblErrEntPty a  left join show_cue2_musicusintrestedparty_lookup b on a.MbrTyp = b.ShowMbrTypCode where a.pgmcde between  convert(int,?) and convert(int,?))" +
            "                 party on music.programcode = party.pgmcde and music.sequnecenumber = party.seqnr "
			+ "          left join (Select distinct  PgmCde, SeqNr, RvsTyp, errorcode, errromessage,Cue2ErrorSeverity,CreTs, CreUid, UpdTs, UpdUid  from VW_program_errors where seqnr != 0 and pgmcde between  convert(int,?) and convert(int,?)) music_error on  music_error.pgmCde =  music.programcode  and music_error.SeqNr = music.sequnecenumber"
	        + " order by music.programcode, music.sequnecenumber, party.pgmcde, party.seqnr, music_error.pgmCde, music_error.SeqNr ";
			

	public final static String NTILE_ON_PROGRAM_CODE_QUERY= "SELECT MIN(MINID) min_pgmcde," +
			"MAX(MAXID)  max_pgmcde, N FROM " +
			"(" +
			"SELECT  " +
			"       MIN(pgmcde) AS MINID, " +
			"MAX(pgmcde) AS MAXID, " +
			"       ntile(?) OVER ( ORDER BY pgmcde ) AS N " +
			"   FROM " +
			"       dbo.DM_Program_Series production  where [status] = 'PENDING' and pgmcde > 0 and pgmcde  < ? " +
			"GROUP BY pgmcde)" +
			"AS T " +
			"GROUP BY N " +
			"ORDER BY N";
	
	public final static String NTILE_COUNT_QUERY= "SELECT (COUNT(*) / 10000) + 1 as CNT " +
			"   FROM " +
			"       dbo.DM_Program_Series production  where [status] = 'PENDING' and pgmcde > 0 and pgmcde  < ? ";


	public final static String UPDATE_PROGRAM_STATUS = "UPDATE DM_Program_Series set [status] = ? where pgmcde = ?   ";

	public final static  String GET_FORMAT = " Select pgmcde, format, processStatus from DM_Program_Series  where pgmcde between convert(int,?) and convert(int,?) ";

	public final static String GET_PRODUCTION_QUERY = "  SELECT       "
			+ "   production.pgmcde                   as programCode  "
			+ " ,production.action  as               programAction  "
			+ " ,production.alternateCueSheetType    as alternateCueSheetType "
			+ " ,production.analysisdatetime         as analysisDateTime  "
			+ " ,production.analysisind              as analysisIndicator  "
			+ " ,production.analysispdateid          as analysisUserId  "
			+ " ,production.animationIndicator       as animationIndicator  "
			+ " ,production.asctheme                 as ascapTheme  "
			+ " ,production.assigneduser             as assignedUser  "
			+ " ,production.attachmentindicator      as attachmentIndicator  "
			+ " ,production.avicode                  as aviCode   "
			+ " ,production.avidentifier             as avIdentifier  "
			+ " ,production.Avidentifiersource       as avIdentifierSource  "
			+ " ,production.codingcomplete           as codingComplete  "
			+ " ,production.countryofproduction      as countryOfProduction  "
			+ " ,production.createdatetime           as showCreateDateTime  "
			+ " ,production.createid                 as showCreateId  "
			+ " ,production.cuesheetorigin           as cuesheetOrigin  "
			+ " ,production.CueSheetStatus           as cuesheetStatus  "
			+ " ,production.datasource               as dataSource  "
			+ " ,production.delegationstatus         as delegationStatus  "
			+ " ,production.deleteflag               as deleteFlag  "
			+ " ,production.directlicenseind         as directLicenseIndicator  "
			+ " ,production.distyp                   as distributionType  "
			+ " ,production.duplicateIndicator       as duplicateIndicator  "
			+ " ,production.duplicateReasonCode      as duplicateReasonCode  "
			+ " ,production.Episodelanguageoriginal  as episodeLanguageOriginal  "
			+ " ,production.episodenumber            as episodeNumber  "
			+ " ,production.episodetitle             as episodeTitle  "
			+ " ,production.episodeTitleMatchpercent as episodeTitleMatchScore  "
			+ " ,production.errorStatus              as errorStatus  "
			+ " ,production.expmusiccount            as expectedMusicCount  "
			+ " ,production.masterfromDate           as masterFromDate  "
		//	+ " ,production.masterNumber             as masterNumber  " No mapping in AVRO
			+ " ,production.masterToDate             as masterToDate  "
			+ " ,production.matchreviewstatus        as matchReviewStatus  "
			+ " ,production.mediatype                as mediaType  "
			+ " ,production.microfilmframe           as microFilmFrame   "
			+ " ,production.microFilmreel            as microFilmReel  "
			+ " ,production.networkstation           as networkStation  "
			+ " ,production.originalairdt            as originalAirDate  "
			+ " ,production.originalProgramCode      as originalProgramCode  "
			+ " ,production.partNumber               as partNumber  "
			+ " ,RIGHT('0' +cast(production.postdurationhr as varchar(10)) ,2)  "
			+ " + RIGHT('0' +cast(production.postDurationmm as varchar(10)) ,2)  "
			+ " + RIGHT('0' +cast(production.postDurationss as varchar(10)) ,2)    as  postDuration  "
			+ " ,production.prioritylevel            as  priorityLevel  "
			+ " ,production.productioncategorycode   as  productionCategoryCode  "
			+ " ,RIGHT('0' +cast(production.productionDurationhr as varchar(10)) ,2)  "
			+ " + RIGHT('0' +cast(production.productionDurationmm as varchar(10)) ,2)   "
			+ " + RIGHT('0' +cast(production.productionDurationss as varchar(10)) ,2) as  productionDuration  "
			+ " ,production.productionidentifier     as  productionIdentifier  "
			+ " ,production.productionidentifiersource as  productionIdentifierSource  "
			+ " ,production.productionlanguageoriginal as  productionLanguageOriginal  "
			+ " ,production.productiontitle             as  productionTitle  "
			+ " ,production.productionTitleMatchpercent as productionTitleMatchScore  "
			+ " ,production.Productyontype              as  productionType  "
			+ " ,production.programcompleteind          as  programCompleteIndicator  "
			+ " ,production.programfirstdistribution    as  programFirstDistribution  "
			+ " ,production.programPreviousName         as programPreviousName  "
			+ " ,production.requeststatus               as  requestStatus  "
			+ " ,production.revisionDate                as revisionDate  "
			+ " ,rotationalind               as programRotationalIndicator " //tbl program "
			//+ " ,production.runtimeDuration             as prod_runtimeDuration  " No mapping in avro
			+ " ,production.seasonnumber                as  seasonNumber  "
			+ " ,production.SerCde                      as  seriesCode  "
			+ " ,production.seriescalcmultipler         as  spdbseriesCalculatedMultipler  "
			+ " ,production.seriesmastind             as  seriesMasterIndicator  "
			+ " ,production.seriesPreviousName        as  seriesPreviousName  "
			+ " ,production.seriesreportedmultipler   as  spdbSeriesReportedMultipler  "
			+ " ,production.seriesreportind           as  spdbSeriesReportIndicator  "
			+ " ,production.seriesrotationalind       as  seriesRotationalIndicator  "
			+ " ,production.SpdCalcMusInd             as  spdbCalculatedMusicIndicator  "
			+ " ,production.SpdCalcSplBmiInd          as  spdbCalculatedSplitWorkBmiIndicator  "
			+ " ,production.SpdCalcSplOthInd          as  spdbCalculatedSplitWorkOtherIndicator  "
			+ " ,production.SpdCalcSplSscInd          as  spdbCalculatedSplitWorkSesac  "
			+ " ,production.SpdRptInd                 as  spdbProgramReportIndicator  "
			+ " ,production.SpdRptMusInd              as  spdbReportMusicIndicator  "
			+ " ,production.SpdRptSplBmiInd           as  spdbReportedSplitWorkBmiIndicator  "
			+ " ,production.SpdRptSplOthInd           as  spdbCalculatedReportedSplitWorkOtherIndicator  "
			+ " ,production.SpdRptSplSscInd           as  spdbReportedSplitWorkSesacIndicator  "
			+ " ,production.SpdUpdTs                  as  spdbUpdateDateTime  "
			//+ " ,production.Status                    as  cuesheetStatus2  "
			+ " ,production.submitterProgramCode      as  submitterProgramCode  "
			+ " ,production.submitterSeriesCode       as  submitterSeriesCode  "
			+ " ,production.territoryoffirstbroadcast as  territoryOfFirstBroadcast  "
			+ " ,production.totalmusiccount           as  totalMusicCount  "
			+ " ,RIGHT('0' +cast(production.totalmusicdurHh as varchar(10)) ,2)   "
			+  " + RIGHT('0' +cast(production.totalmusicdurmm as varchar(10)) ,2)     "
			+  " + RIGHT('0' +cast(production.totalmusicdurss as varchar(10)) ,2)   as totalMusicDuration  "
			+ " ,production.umbrellaelements          as umbrellaElements  "
			+ " ,production.umbrellaindicator         as umbrellaIndicator  "
			+ " ,production.updatedatetime            as showUpdateDateTime  "
			+ " ,production.updateid                  as showUpdateId  "
			+ " ,production.verificationindicator     as verificationCompleteIndicator  "
			+ " ,production.verificationuserid        as verificationUserId  "
			+ " ,production.versioncomment            as versionComment  "
			+ " ,production.versionNumber             as versionNumber  "
			+ " ,production.versionterritory          as versionTerritory  "
			+ " ,production.versiontypecde            as versionTypeCde  "
			+ " ,production.yearofproduction          as yearOfProduction  "
			+ " ,production.syndicatedNumber          as syndicatedNumber  "
			+ " ,cueInfo.programcode                   as cuesheet_info_programcode  "
			+ " ,cueInfo.cuesheetclassuficationtype   as cuesheetClassificationType  "
			+ " ,cueInfo.Cuesheetprovider             as cuesheetProvider   "
			+ " ,cueInfo.CueSheetPreparedBy           as cuesheetPreparedBy  "
			+ " ,cueInfo.roleofcuesheetprovider       as roleOfCuesheetProvider  "
			+ " ,cueInfo.Filecreationdate             as fileCreationDate  "
			+ " ,cueInfo.[filename]                   as fileName  "
			+ " ,cueInfo.creatorId                    as creatorId  "
			+ " , spuAka.akaTyp  "
			+ " , spuAka.akacde  "
			+ " , prod_title = case when spuAka.akaTyp =  'P' then spuAka.akana else null end  "
			+ " , prod_lang = case when spuAka.akaTyp =  'P' then spuAka.lngcde else null end  "
			+ " , ep_title = case when spuAka.akaTyp =  'S' then spuAka.akana else null end  "
			+ " , ep_lang = case when spuAka.akaTyp =  'S' then spuAka.lngcde else null end  "
			+ " ,errors.PgmCde             as pgm_error_programCode  "
			//--      + " ,SeqNr              as not needed "
			+ " ,LTRIM(RTRIM(CAST(RvsTyp as VARCHAR)))             as revisionType  "
			//--    + " ,ExcTtlCde          as not needed "
			//--    + " ,ExcUpdInd          as not needed "
			//--    + " ,Series_name        as not needed "
			+ " ,errorcode   as errorCode"
			+ " ,LTRIM(RTRIM(CAST(errromessage as VARCHAR)))        as errorMessage  "
			+ " ,LTRIM(RTRIM(CAST(Cue2ErrorSeverity  as VARCHAR)))  as errorSeverity  "
			+ " ,errors.CreTs              as err_showCreateDateTime  "
			+ " ,LTRIM(RTRIM(CAST(errors.CreUid  as VARCHAR)))             as err_showCreateUserId  "
			+ " ,errors.UpdTs              as err_showUpdateDateTime  "
			+ " ,LTRIM(RTRIM(CAST(errors.UpdUid as VARCHAR)))              as err_showUpdateUserId  "
			//avInterestedParty from tblDirAct "
			+ " ,avInterestedParty.role as av_role  "
			+ " ,ltrim(rtrim(cast(avInterestedParty.DirActFstNA as varchar))) as av_firstName  "
			+ " ,ltrim(rtrim(cast(avInterestedParty.DirActLstNA as varchar))) as av_lastName  "
			+ " ,avInterestedParty.pgmcde as av_programCode  "
           + "	from (Select * from DM_Program_Series  where pgmcde between convert(int ,?)  	and convert(int ,?) ) production  "
			+ "	left join  (Select distinct programcode, cuesheetclassuficationtype, Cuesheetprovider, "
			+ " CueSheetPreparedBy, roleofcuesheetprovider , Filecreationdate , [filename] , creatorId from DM_Cuesht_Info "
			+ " where  programcode between convert(int ,?)  	and convert(int ,?) )  cueInfo  on  cueInfo.programcode = production.pgmcde  "
			+ "	left join (select distinct pgmcde , case when DirActTyp = 'A' then 'AC'  "
			+ "	when DirActTyp = 'D' then 'RE' else null end as role , DirActFstNA, DirActLstNA from tblDirAct "
			+ " where pgmcde between convert(int ,?)  	and convert(int ,?)  )avInterestedParty  on avInterestedParty.pgmcde = production.pgmcde  "
			+ "	left join ( Select distinct akacde, ltrim(rtrim(akana)) akana, akatyp, lngcde from DM_TblSPuAKa "
			+ " where akacde between convert(int ,?)  	and convert(int ,?)   ) spuAka on spuAka.akacde  = production.pgmcde  "
			+ "	left join (Select distinct  PgmCde, RvsTyp, errorcode, errromessage,Cue2ErrorSeverity,CreTs, CreUid, UpdTs, UpdUid from  VW_program_errors where seqnr = 0 "
			+ " and pgmcde between convert(int ,?)  	and convert(int ,?) ) errors on errors.pgmcde = production.pgmcde   "
		    + "  order by production.pgmcde, cueInfo.programcode , akacde  , errors.pgmcde  ";

	
	public static final String SELECT_UMB_1 =  "select UmbCde, PgmCde, convert(varchar, CreTs, 121) as CreTs, CreUid, convert(varchar, UpdTs, 121) as UpdTs "+
			  ", UpdUid from tblUmbSpec where UmbCde ";
/*
	public static final String UMB_SELECT_2 = 
				" BETWEEN "+minProgramCode+" AND "+maxprogramCode;
				*/
	
	
	public static final String SELECT_MUS_CONTENT_1 = 
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
			+ " where programcode "; // between "+minProgramCode+" and " +maxprogramCode ;
	
	public static final String SELECT_INT_PTY_1 = 
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
	    			+ " where PgmCde  ";//between +minProgramCode+" and " +maxprogramCode ;
	    	
	public static final String SELECT_ALT_TTL_1 =
			" SELECT A.akacde, A.akatyp, B.Productyontype, " + 
	    			" A.akana, ltrim(rtrim(A.lngcde)), " + 
	    			" B.PGMCDE  AS PGMCDE, " + 
	    			" B.SerCde  AS SERCDE, " + 
	    			" creuid, crets, upduid, updts " +
	    			" FROM DM_TblSPuAKa A, " + 
	    			" DM_Program_Series B " + 
	    			" WHERE a.akacde = b.pgmcde " + 
	    			" and a.akaTyp = 'P' " + 
	    			" AND B.PGMCDE ";// BETWEEN  "+minProgramCode+" AND  " + maxprogramCode +  
	public static final String SELECT_ALT_TTL_2 =
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
	    			" AND B.PGMCDE ";// BETWEEN  "+minProgramCode+" AND "+maxprogramCode +
	public static final String SELECT_ALT_TTL_3 =
			" ) outer1 group by akacde,  akatyp, Productyontype, akana, lng, PGMCDE, SERCDE, creuid, crets, upduid, updts  ";
	    	 
 
	public static final String SELECT_AV_PTY_1 =
			"select distinct pgmcde, role , ltrim(rtrim(DirActFstNA)), ltrim(rtrim(DirActLstNA)), creuid, crets, upduid, updts from DM_DIRaCT " + 
	    			"	where pgmcde ";//between "+minProgramCode+" and  "+maxprogramCode;
	    	
	public static final String SELECT_CUE_ERR_1 =
			"select PgmCde, SeqNr, ERRORCODE, crets, creuid, updts, upduid FROM VW_program_errors a  " + 
	    			"			where a.pgmcde ";//between "+minProgramCode+" and  "+maxprogramCode;
	    	
	public static final String SELECT_CUE_SHT_HDR_1 =
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
	    			"    			  FROM  (select * from DM_Program_Series x where x.pgmcde ";// between "+minProgramCode+" and " +maxprogramCode +
	public static final String SELECT_CUE_SHT_HDR_2 =
	") a " + 
	    			"				  	left outer join ( " + 
	    			"    					select * from ( " + 
	    			"        					select *, row_number() over ( " + 
	    			"            					partition by programcode " + 
	    			"            					order by filecreationdate desc " + 
	    			"        					) as row_num " + 
	    			"        				from DM_Cuesht_Info where  programcode ";//between "+minProgramCode+" and " +maxprogramCode  + 
	public static final String SELECT_CUE_SHT_HDR_3 =   			
	"    					) as ordered_cue_info " + 
	    			"    				where ordered_cue_info.row_num = 1 " + 
	    			" ) as b " + 
	    			" on a.pgmcde = b.programcode";
}
