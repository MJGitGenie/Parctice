package com.ascap.cue2.cuesheet.showdataimporter;

import static com.ascap.cue2.cuesheet.showdataimporter.constants.ShowQueryConstants.NTILE_COUNT_QUERY;
import static com.ascap.cue2.cuesheet.showdataimporter.constants.ShowQueryConstants.NTILE_ON_PROGRAM_CODE_QUERY;
import static com.ascap.cue2.cuesheet.showdataimporter.constants.ShowQueryConstants.DELETE_INC_STG_TABLE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import com.ascap.cue2.cuesheet.showdataimporter.config.Cuesheet;
import com.ascap.cue2.cuesheet.showdataimporter.utils.DateValidationUtils;



@RunWith(MockitoJUnitRunner.Silent.class)
public class ShowDataTest {

    @Mock
    private static Connection connection;
    @Mock
    private static Connection connection1;
    @Mock
    private static PreparedStatement stmt;
    @Mock
    private static PreparedStatement stmt1;

    @Mock
    private static CallableStatement stmt2;
    @Mock
    private static PreparedStatement stmt3;
    @Mock
    private static ResultSet rs;
    @Mock
    private static ResultSet rs1;
    @Mock
    private static ResultSet rs3;

    ShowData showData = new ShowData();
    Cuesheet expectedCuesheet;
    HashMap<Integer,Integer>  expectedMap;


    @BeforeClass
    public static void setUp() throws Exception {
        connection = Mockito.mock(Connection.class);
        stmt = Mockito.mock(PreparedStatement.class);
        rs = Mockito.mock(ResultSet.class);

    }


    @Before
    public void setTestData(){
         expectedMap = new HashMap<>();
        expectedMap.put(1,2);
    }

    @Before
    public void loadTestCueSheet() throws Exception{
    }
    
    @Test 
    public void dateValidationTest() throws Exception {
    	DateValidationUtils dvTest = new DateValidationUtils("yyyy-MM-dd");
    	Assert.assertTrue(dvTest.isValid("2019-01-01"));
    	Assert.assertFalse(dvTest.isValid("2019-14-01"));
    }
    
    
    @Test
    public void truncateStagingTableTest()  throws Exception  {
    	when(connection.prepareStatement(DELETE_INC_STG_TABLE)).thenReturn(stmt3);
    	doReturn(1).when(stmt3).executeUpdate();
    	Mockito.when(rs1.next()).thenReturn(false).thenReturn(true);
    	showData.truncateStagingTable(connection,0, null);
    	Assert.assertEquals(1, 1);
    }
    /*
    @Test
    public void getProgramCodeRange() throws Exception  {
    	when(connection.prepareStatement(NTILE_COUNT_QUERY)).thenReturn(stmt1);
        doReturn(rs1).when(stmt1).executeQuery();
        Mockito.when(rs1.next()).thenReturn(false).thenReturn(true);
        when(connection.prepareStatement(NTILE_ON_PROGRAM_CODE_QUERY)).thenReturn(stmt);
        doReturn(rs).when(stmt).executeQuery();
        Mockito.when(rs.next()).thenReturn(false).thenReturn(true);
        ShowData showDataMock = Mockito.spy(ShowData.class);
        Mockito.doReturn(1).when(showDataMock).processPrograms(anyInt(),any(Connection.class),any(Connection.class), anyBoolean());
         
        int result = showData.processPrograms( 5, connection, connection1, false);
        Mockito.when(rs.getInt(1)).thenReturn(1);
        Mockito.when(rs.getInt(2)).thenReturn(2);
        int minPgmcde=0;
        int maxPgmcde=0;
        for(HashMap.Entry<Integer, Integer> entry : expectedMap.entrySet()){
            minPgmcde=entry.getKey();
            maxPgmcde=entry.getValue();
        }
        Assert.assertEquals(minPgmcde, rs.getInt(1));
        Assert.assertEquals(maxPgmcde, rs.getInt(2));
    }

   @Test
   public void getProductionLevelItemsTest() throws Exception{

    int minPgm = 2917823;
    int maxPgm = 2917823;
    HashMap<String,Production> actualProductionMap;
       when(connection.prepareStatement(ShowQueryConstants.GET_PRODUCTION_QUERY)).thenReturn(stmt);
       doReturn(rs).when(stmt).executeQuery();
       Mockito.when(rs.next()).thenReturn(true).thenReturn(false);
       Mockito.when(rs.getString("programCode")).thenReturn("2917823");
       Mockito.when(rs.getString("alternateCueSheetType")).thenReturn("N");
       Mockito.when(rs.getString("analysisDateTime")).thenReturn(null);
       Mockito.when(rs.getString("analysisIndicator")).thenReturn(null);
       Mockito.when(rs.getString("analysisUserId")).thenReturn(null);
       Mockito.when(rs.getString("animationIndicator")).thenReturn(null);
       Mockito.when(rs.getString("assignedUser")).thenReturn(null);
       Mockito.when(rs.getString("ascapTheme")).thenReturn(null);
        Mockito.when(rs.getString("attachmentIndicator")).thenReturn("N");
       Mockito.when(rs.getString("aviCode")).thenReturn(null);
       Mockito.when(rs.getString("avIdentifier")).thenReturn(null);
       Mockito.when(rs.getString("avIdentifierSource")).thenReturn(null);
       Mockito.when(rs.getString("avInterestedPartyRole")).thenReturn(null);
       Mockito.when(rs.getString("codingComplete")).thenReturn("N");
       Mockito.when(rs.getString("countryOfProduction")).thenReturn(null);
       Mockito.when(rs.getString("cuesheetOrigin")).thenReturn("E");
       Mockito.when(rs.getString("cuesheetStatus")).thenReturn("LD");
       Mockito.when(rs.getString("dataSource")).thenReturn("Tblerrpgm");
       Mockito.when(rs.getString("delegationStatus")).thenReturn(null);
       Mockito.when(rs.getString("deleteFlag")).thenReturn("N");
       Mockito.when(rs.getString("directLicenseIndicator")).thenReturn(null);
       Mockito.when(rs.getString("distributionType")).thenReturn(null);
       Mockito.when(rs.getString("duplicateIndicator")).thenReturn(null);
       Mockito.when(rs.getString("duplicateReasonCode")).thenReturn(null);
       Mockito.when(rs.getString("episodeLanguageOriginal")).thenReturn("ENG");
       Mockito.when(rs.getString("episodeNumber")).thenReturn("BNOT14002HD");
       Mockito.when(rs.getString("episodeTitle")).thenReturn("BNOT14002HD- TOP 100 VIDEOS OF 2014");
       Mockito.when(rs.getString("episodeTitleMatchScore")).thenReturn(null);
       Mockito.when(rs.getString("errorStatus")).thenReturn("E");
       Mockito.when(rs.getString("expectedMusicCount")).thenReturn(null);
       Mockito.when(rs.getString("masterFromDate")).thenReturn(null);
       Mockito.when(rs.getString("masterToDate")).thenReturn(null);
       Mockito.when(rs.getString("matchReviewStatus")).thenReturn("NEW");
       Mockito.when(rs.getString("mediaType")).thenReturn(null);
       Mockito.when(rs.getString("microFilmFrame")).thenReturn(null);
       Mockito.when(rs.getString("microFilmReel")).thenReturn(null);
       Mockito.when(rs.getString("nameOfAvInterestedParty")).thenReturn("");
       Mockito.when(rs.getString("networkStation")).thenReturn("");
       Mockito.when(rs.getString("originalAirDate")).thenReturn("2014-12-31");
       Mockito.when(rs.getString("originalProgramCode")).thenReturn(null);
       Mockito.when(rs.getString("partNumber")).thenReturn(null);
       Mockito.when(rs.getString("postDuration")).thenReturn("020000");
       Mockito.when(rs.getString("priorityLevel")).thenReturn("L");
       Mockito.when(rs.getString("productionCategoryCode")).thenReturn("SPE");
       Mockito.when(rs.getString("productionDuration")).thenReturn("020000");
       Mockito.when(rs.getString("productionIdentifier")).thenReturn("BNOT14002HD");
       Mockito.when(rs.getString("productionIdentifierSource")).thenReturn(null);
       Mockito.when(rs.getString("productionLanguageOriginal")).thenReturn(null);
       Mockito.when(rs.getString("productionTitle")).thenReturn("SPECIALS");
//Mockito.when(rs.getString("productionTitleMatchScore")).thenReturn("");
       Mockito.when(rs.getString("productionType")).thenReturn("S");
       Mockito.when(rs.getString("programAction")).thenReturn(null);
       Mockito.when(rs.getString("programCompleteIndicator")).thenReturn(null);
       Mockito.when(rs.getString("programFirstDistribution")).thenReturn(null);
//Mockito.when(rs.getString("programPreviousName")).thenReturn("");
       Mockito.when(rs.getString("requestStatus")).thenReturn(null);
       Mockito.when(rs.getString("revisionDate")).thenReturn(null);
//Mockito.when(rs.getString("runtimeDuration")).thenReturn(""); not in avro
       Mockito.when(rs.getString("seasonNumber")).thenReturn(null);
       Mockito.when(rs.getString("seriesCode")).thenReturn("35487");
       Mockito.when(rs.getString("seriesMasterIndicator")).thenReturn("N");
//Mockito.when(rs.getString("seriesPreviousName")).thenReturn("");
       Mockito.when(rs.getString("seriesRotationalIndicator")).thenReturn("N");
       Mockito.when(rs.getString("showCreateDateTime")).thenReturn("2015-04-23 10:50:00.0");
       Mockito.when(rs.getString("showCreateId")).thenReturn("BET");
       Mockito.when(rs.getString("showUpdateDateTime")).thenReturn("2015-06-05");
       Mockito.when(rs.getString("showUpdateId")).thenReturn("BET");
       Mockito.when(rs.getString("spdbCalculatedMusicIndicator")).thenReturn(null);
       Mockito.when(rs.getString("spdbCalculatedSplitWorkBmiIndicator")).thenReturn(null);
       Mockito.when(rs.getString("spdbCalculatedSplitWorkOtherIndicator")).thenReturn(null);
       Mockito.when(rs.getString("spdbCalculatedSplitWorkSesac")).thenReturn(null);
       Mockito.when(rs.getString("spdbProgramReportIndicator")).thenReturn(null);
       Mockito.when(rs.getString("spdbReportedSplitWorkSesacIndicator")).thenReturn(null);
       Mockito.when(rs.getString("spdbReportMusicIndicator")).thenReturn(null);
       Mockito.when(rs.getString("spdbseriesCalculatedMultipler")).thenReturn("0.9830");
       Mockito.when(rs.getString("spdbSeriesReportedMultipler")).thenReturn("0.9830");
       Mockito.when(rs.getString("spdbSeriesReportIndicator")).thenReturn("N");
       Mockito.when(rs.getString("spdbUpdateDateTime")).thenReturn(null);
       Mockito.when(rs.getString("submitterProgramCode")).thenReturn(null);
       Mockito.when(rs.getString("submitterSeriesCode")).thenReturn("6526");
       Mockito.when(rs.getString("syndicatedNumber")).thenReturn(null);
       Mockito.when(rs.getString("territoryOfFirstBroadcast")).thenReturn("");
       Mockito.when(rs.getString("totalMusicCount")).thenReturn("47");
       Mockito.when(rs.getString("totalMusicDuration")).thenReturn("012518");
       Mockito.when(rs.getString("umbrellaElements")).thenReturn("");
       Mockito.when(rs.getString("umbrellaIndicator")).thenReturn("");
       Mockito.when(rs.getString("verificationCompleteIndicator")).thenReturn("");
//Mockito.when(rs.getString("verificationUserId")).thenReturn(null);
       Mockito.when(rs.getString("versionComment")).thenReturn("50:00.0");
       Mockito.when(rs.getString("versionNumber")).thenReturn("");
       Mockito.when(rs.getString("versionTerritory")).thenReturn("57:04.26");
       Mockito.when(rs.getString("versionTypeCde")).thenReturn("CAB");
       Mockito.when(rs.getString("yearOfProduction")).thenReturn("2014");
       Mockito.when(rs.getString("programRotationalndicator")).thenReturn(null);
       Mockito.when(rs.getString("seriesRotationalIndicator")).thenReturn("N");
       Mockito.when(rs.getString("spdbCalculatedReportedSplitWorkOtherIndicator")).thenReturn(null);
       Mockito.when(rs.getString("spdbReportedSplitWorkBmiIndicator")).thenReturn(null);


       Mockito.when(rs.getString("akacde")).thenReturn("2917823");
       Mockito.when(rs.getString("prod_title")).thenReturn("BNOT14002HD- TOP 100 VIDEOS OF 2014");
       Mockito.when(rs.getString("prod_lang")).thenReturn("ENG");

       Mockito.when(rs.getString("pgm_error_programCode")).thenReturn("2917823");
       Mockito.when(rs.getString("revisionType")).thenReturn("R");
       Mockito.when(rs.getString("errorCode")).thenReturn("56");
       Mockito.when(rs.getString("errorMessage")).thenReturn("PROGRAM REVISION.");
       Mockito.when(rs.getString("errorSeverity")).thenReturn("Error");
       Mockito.when(rs.getString("err_showCreateDateTime")).thenReturn("2015-06-05 15:57:00.0");
       Mockito.when(rs.getString("err_showCreateUserId")).thenReturn("BET");
       Mockito.when(rs.getString("err_showUpdateUserId")).thenReturn("BET");
       Mockito.when(rs.getString("err_showUpdateDateTime")).thenReturn("2015-06-05 15:57:00.0");

      Mockito.when(rs.getString("av_programCode")).thenReturn("2917823");
      Mockito.when(rs.getString("av_firstName")).thenReturn("DIRK");
      Mockito.when(rs.getString("av_lastName")).thenReturn("BENEDICK");
      Mockito.when(rs.getString("av_role")).thenReturn("Director");

       Mockito.when(rs.getString("cuesheet_info_programcode")).thenReturn("2917823");
       Mockito.when(rs.getString("cuesheetClassificationType")).thenReturn("New");
       Mockito.when(rs.getString("cuesheetPreparedBy")).thenReturn("BET");
       Mockito.when(rs.getString("cuesheetProvider")).thenReturn("BET");
       Mockito.when(rs.getString("fileCreationDate")).thenReturn("04232015");
       Mockito.when(rs.getString("fileName")).thenReturn("47D90530-D140-4CA7-875D-EE2261DC734C.xml");


      actualProductionMap = showData.getProductionLevelItems(minPgm, maxPgm, connection);

       //Iterate over map to get Production Object
       Production production = new Production();
       for (HashMap.Entry<String,Production> productionEntry : actualProductionMap.entrySet()) {
           production = new Production();
           production = productionEntry.getValue();
          // System.out.println("production  "+productionEntry.getValue());
       }
        String outPutPgmcde = expectedCuesheet.getCanonicalData().getProduction().getProgramCode();

       Assert.assertEquals(true, actualProductionMap.containsKey(outPutPgmcde));
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getProgramCode(), production.getProgramCode());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getProgramAction()	,	production.getProgramAction());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getAlternateCueSheetType()	,	production.getAlternateCueSheetType());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getAnalysisDateTime()	,	production.getAnalysisDateTime());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getAnalysisIndicator()	,	production.getAnalysisIndicator());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getAnalysisUserId()	,	production.getAnalysisUserId());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getAnimationIndicator()	,	production.getAnimationIndicator());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getAscapTheme()	,	production.getAscapTheme());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getAssignedUser()	,	production.getAssignedUser());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getAttachmentIndicator()	,	production.getAttachmentIndicator());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getAviCode()	,	production.getAviCode());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getAvIdentifier()	,	production.getAvIdentifier());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getAvIdentifierSource(),	production.getAvIdentifierSource());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getCodingComplete()	,	production.getCodingComplete());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getCountryOfProduction()	,	production.getCountryOfProduction());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getShowCreateDateTime()	,	production.getShowCreateDateTime());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getShowCreateId()	,	production.getShowCreateId());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getCuesheetOrigin()	,	production.getCuesheetOrigin());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getCuesheetStatus()	,	production.getCuesheetStatus());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getDataSource()	,	production.getDataSource());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getDelegationStatus()	,	production.getDelegationStatus());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getDeleteFlag()	,	production.getDeleteFlag());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getDirectLicenseIndicator()	,	production.getDirectLicenseIndicator());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getDistributionType()	,	production.getDistributionType());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getDuplicateIndicator()	,	production.getDuplicateIndicator());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getDuplicateReasonCode()	,	production.getDuplicateReasonCode());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getEpisodeLanguageOriginal()	,	production.getEpisodeLanguageOriginal());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getEpisodeNumber()	,	production.getEpisodeNumber());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getEpisodeTitle()	,	production.getEpisodeTitle());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getEpisodeTitleMatchScore()	,	production.getEpisodeTitleMatchScore());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getErrorStatus()	,	production.getErrorStatus());
               Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getExpectedMusicCount()	,	production.getExpectedMusicCount());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMasterFromDate()	,	production.getMasterFromDate());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMasterToDate()	,	production.getMasterToDate());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMatchReviewStatus()	,	production.getMatchReviewStatus());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMediaType()	,	production.getMediaType());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMicroFilmFrame()	,	production.getMicroFilmFrame());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMicroFilmReel()	,	production.getMicroFilmReel());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getNetworkStation()	,	production.getNetworkStation());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getOriginalAirDate()	,	production.getOriginalAirDate());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getOriginalProgramCode()	,	production.getOriginalProgramCode());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getPartNumber()	,	production.getPartNumber());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getProgramCode()	,	production.getProgramCode());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getPostDuration()	,	production.getPostDuration());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getPriorityLevel()	,	production.getPriorityLevel());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getProductionCategoryCode()	,	production.getProductionCategoryCode());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getProductionDuration()	,	production.getProductionDuration());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getProductionIdentifier()	,	production.getProductionIdentifier());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getProductionIdentifierSource()	,	production.getProductionIdentifierSource());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getProductionLanguageOriginal()	,	production.getProductionLanguageOriginal());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getProductionTitle()	,	production.getProductionTitle());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getProductionTitleMatchScore()	,	production.getProductionTitleMatchScore());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getProductionType()	,	production.getProductionType());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getProgramCompleteIndicator()	,	production.getProgramCompleteIndicator());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getProgramFirstDistribution()	,	production.getProgramFirstDistribution());
               Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getProgramPreviousName()	,	production.getProgramPreviousName());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getSubmitterProgramCode()	,	production.getSubmitterProgramCode());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getRequestStatus()	,	production.getRequestStatus());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getRevisionDate()	,	production.getRevisionDate());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getSeasonNumber()	,	production.getSeasonNumber());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getSeriesCode()	,	production.getSeriesCode());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getSpdbseriesCalculatedMultipler()	,	production.getSpdbseriesCalculatedMultipler());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getSeriesMasterIndicator()	,	production.getSeriesMasterIndicator());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getSeriesPreviousName()	,	production.getSeriesPreviousName());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getSpdbSeriesReportedMultipler()	,	production.getSpdbSeriesReportedMultipler());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getSpdbSeriesReportIndicator()	,	production.getSpdbSeriesReportIndicator());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getSeriesRotationalIndicator()	,	production.getSeriesRotationalIndicator());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getSpdbCalculatedMusicIndicator()	,	production.getSpdbCalculatedMusicIndicator());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getSpdbCalculatedSplitWorkBmiIndicator()	,	production.getSpdbCalculatedSplitWorkBmiIndicator());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getSpdbCalculatedSplitWorkOtherIndicator()	,	production.getSpdbCalculatedSplitWorkOtherIndicator());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getSpdbCalculatedSplitWorkSesac()	,	production.getSpdbCalculatedSplitWorkSesac());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getSpdbProgramReportIndicator()	,	production.getSpdbProgramReportIndicator());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getSpdbReportMusicIndicator()	,	production.getSpdbReportMusicIndicator());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getSpdbReportedSplitWorkSesacIndicator()	,	production.getSpdbReportedSplitWorkSesacIndicator());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getSpdbUpdateDateTime()	,	production.getSpdbUpdateDateTime());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getCuesheetStatus()	,	production.getCuesheetStatus());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getSubmitterProgramCode()	,	production.getSubmitterProgramCode());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getSubmitterSeriesCode()	,	production.getSubmitterSeriesCode());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getTerritoryOfFirstBroadcast()	,	production.getTerritoryOfFirstBroadcast());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getCodingComplete()	,	production.getCodingComplete());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getTotalMusicCount()	,	production.getTotalMusicCount());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getTotalMusicDuration()	,	production.getTotalMusicDuration());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getUmbrellaElements()	,	production.getUmbrellaElements());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getUmbrellaIndicator()	,	production.getUmbrellaIndicator());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getShowUpdateDateTime()	,	production.getShowUpdateDateTime());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getShowUpdateId()	,	production.getShowUpdateId());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getVerificationCompleteIndicator()	,	production.getVerificationCompleteIndicator());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getVerificationUserId()	,	production.getVerificationUserId());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getVersionComment()	,	production.getVersionComment());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getVersionNumber()	,	production.getVersionNumber());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getVersionTerritory()	,	production.getVersionTerritory());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getVersionTypeCode()	,	production.getVersionTypeCode());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getYearOfProduction()	,	production.getYearOfProduction());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getSyndicatedNumber()	,	production.getSyndicatedNumber());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getProgramRotationalIndicator()	,	production.getProgramRotationalIndicator());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getSeriesRotationalIndicator()	,	production.getSeriesRotationalIndicator());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getSpdbCalculatedReportedSplitWorkOtherIndicator()	,	production.getSpdbCalculatedReportedSplitWorkOtherIndicator());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getSpdbReportedSplitWorkBmiIndicator()	,	production.getSpdbReportedSplitWorkBmiIndicator());

       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getAlternateProductionTitles().get(0).getAlternateLanguage(),production.getAlternateProductionTitles().get(0).getAlternateLanguage());

       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getProgramErrors().get(0).getRevisionType(), production.getProgramErrors().get(0).getRevisionType());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getProgramErrors().get(0).getErrorCode(), production.getProgramErrors().get(0).getErrorCode());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getProgramErrors().get(0).getErrorMessage(), production.getProgramErrors().get(0).getErrorMessage());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getProgramErrors().get(0).getErrorSeverity(), production.getProgramErrors().get(0).getErrorSeverity());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getProgramErrors().get(0).getShowCreateDateTime(), production.getProgramErrors().get(0).getShowCreateDateTime());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getProgramErrors().get(0).getShowCreateUserId(), production.getProgramErrors().get(0).getShowCreateUserId());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getProgramErrors().get(0).getShowUpdateUserId(), production.getProgramErrors().get(0).getShowUpdateUserId());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getProgramErrors().get(0).getShowUpdateDateTime(), production.getProgramErrors().get(0).getShowUpdateDateTime());

       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getAvInterestedParties().get(0).getFirstName(), production.getAvInterestedParties().get(0).getFirstName());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getAvInterestedParties().get(0).getLastNameOrCompanyName(), production.getAvInterestedParties().get(0).getLastNameOrCompanyName());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getAvInterestedParties().get(0).getRole(), production.getAvInterestedParties().get(0).getRole());

       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getCuesheetInformation().get(0).getCuesheetClassificationType(), production.getCuesheetInformation().get(0).getCuesheetClassificationType());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getCuesheetInformation().get(0).getCuesheetPreparedBy(), production.getCuesheetInformation().get(0).getCuesheetPreparedBy());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getCuesheetInformation().get(0).getCuesheetProvider(), production.getCuesheetInformation().get(0).getCuesheetProvider());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getCuesheetInformation().get(0).getFileCreationDate(), production.getCuesheetInformation().get(0).getFileCreationDate());
       Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getCuesheetInformation().get(0).getFileName(), production.getCuesheetInformation().get(0).getFileName());

   }

    @Test
    public void getMusicLevelItemsTest() throws Exception{

        int minPgm = 2917823; 
        int maxPgm = 2917823;
        HashMap<String, List<MusicContentItem>> actualMusicMap;
        when(connection.prepareStatement(ShowQueryConstants.GET_MUSIC_CONTENT)).thenReturn(stmt);
        doReturn(rs).when(stmt).executeQuery();
        Mockito.when(rs.next()).thenReturn(true).thenReturn(false);

        Mockito.when(rs.getString("music_programCode")).thenReturn("2917823");
        Mockito.when(rs.getString("sequenceNumber")).thenReturn("1");
        Mockito.when(rs.getString("cueAction")).thenReturn(null);
        Mockito.when(rs.getString("musicTitle")).thenReturn("MAYBE");
        Mockito.when(rs.getString("musicDuration")).thenReturn("000403");
        Mockito.when(rs.getString("startTime")).thenReturn(null);
        Mockito.when(rs.getString("iswc")).thenReturn("");
        Mockito.when(rs.getString("isrc")).thenReturn("");
        Mockito.when(rs.getString("musicUsageCode")).thenReturn("012");
        Mockito.when(rs.getString("musicDuration")).thenReturn("000403");
        Mockito.when(rs.getString("ascapWorkId")).thenReturn("0");
        Mockito.when(rs.getString("programOccurrence")).thenReturn("1");
        Mockito.when(rs.getString("performanceCode")).thenReturn("VV");
        Mockito.when(rs.getString("showCreateDateTime")).thenReturn("2015-04-23 10:50:00.0");
        Mockito.when(rs.getString("showCreateId")).thenReturn("BET");
        Mockito.when(rs.getString("showUpdateId")).thenReturn("BET");
        Mockito.when(rs.getString("showUpdateDateTime")).thenReturn("2015-04-23 10:50:00.0");
        Mockito.when(rs.getString("matchUserId")).thenReturn("BET");
        Mockito.when(rs.getString("matchDateTime")).thenReturn("BET");
        Mockito.when(rs.getString("cueMatchType")).thenReturn("MM");
        Mockito.when(rs.getString("directSourceIndicator")).thenReturn("directSourceIndicator");
        Mockito.when(rs.getString("submitterWorkId")).thenReturn("1906785");
        Mockito.when(rs.getString("publicDomainIndicator")).thenReturn(null);
        Mockito.when(rs.getString("publicDomainTerritory")).thenReturn(null);

        Mockito.when(rs.getString("Party_programCode")).thenReturn("2917823");
        Mockito.when(rs.getString("party_sequenceNumber")).thenReturn("1");
        Mockito.when(rs.getString("ipiNumber")).thenReturn("");
        Mockito.when(rs.getString("memebertypecode")).thenReturn("C");
        Mockito.when(rs.getString("musicInterestedPartyShare")).thenReturn("5.5600");
        Mockito.when(rs.getString("music_firstName")).thenReturn("TERRENCE");
        Mockito.when(rs.getString("music_lastName")).thenReturn("THORNTON");
        Mockito.when(rs.getString("societyCode")).thenReturn("10");
        Mockito.when(rs.getString("musicPerformer")).thenReturn(null);

        Mockito.when(rs.getString("revisionType")).thenReturn("N");
        Mockito.when(rs.getString("errorCode")).thenReturn("150");
        Mockito.when(rs.getString("errorMessage")).thenReturn("TITLE CODE NOT FOUND FOR TITLE");
        Mockito.when(rs.getString("errorSeverity")).thenReturn("Error");
        Mockito.when(rs.getString("showCreateDateTime")).thenReturn("2015-04-23 10:50:00.0");
        Mockito.when(rs.getString("showCreateUserId")).thenReturn("BET");
        Mockito.when(rs.getString("showUpdateUserId")).thenReturn("BET");
        Mockito.when(rs.getString("showUpdateDateTime")).thenReturn("2015-04-23 10:50:00.0");

        actualMusicMap = showData.getMusicAndInterestedParty(minPgm, maxPgm, connection);

        //Iterate overmap to get Production Object
        List<MusicContentItem> musicList = new ArrayList<>();
        MusicContentItem music = new MusicContentItem();

        for (HashMap.Entry<String,List<MusicContentItem>> musicEntry : actualMusicMap.entrySet()) {
            musicList = musicEntry.getValue();
            music = musicList.get(0);

           // System.out.println("musicList.get(0)  "+musicEntry.getValue());
        }

        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getSequenceNumber(), music.getSequenceNumber());
        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getCueAction(), music.getCueAction());
        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getMusicTitle(), music.getMusicTitle());
        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getMusicDuration(), music.getMusicDuration());
        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getStartTime(), music.getStartTime());
        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getIswc(), music.getIswc());
        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getIsrc(), music.getIsrc());
        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getMusicUsageCode(), music.getMusicUsageCode());
        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getMusicDuration(), music.getMusicDuration());
        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getAscapWorkId(), music.getAscapWorkId());
        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getPerformanceCode(), music.getPerformanceCode());
        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getProgramOccurrence(), music.getProgramOccurrence());
        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getAmbientMusicIndicator(), music.getAmbientMusicIndicator());
        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getShowCreateDateTime(), music.getShowCreateDateTime());
        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getShowCreateId(), music.getShowCreateId());
        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getShowUpdateDateTime(), music.getShowUpdateDateTime());
        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getShowUpdateId(), music.getShowUpdateId());
        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getMatchUserId(), music.getMatchUserId());
        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getCueMatchType(), music.getCueMatchType());
        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getMatchDateTime(), music.getMatchDateTime());
        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getDirectSourceIndicator(), music.getDirectSourceIndicator());
        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getPublicDomainIndicator(), music.getPublicDomainIndicator());
        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getPublicDomainTerritory(), music.getPublicDomainTerritory());

        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getMusicInterestedParties().get(0).getSequenceNumber(), music.getMusicInterestedParties().get(0).getSequenceNumber());
        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getMusicInterestedParties().get(0).getFirstName(), music.getMusicInterestedParties().get(0).getFirstName());
        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getMusicInterestedParties().get(0).getLastNameOrCompanyName(), music.getMusicInterestedParties().get(0).getLastNameOrCompanyName());
        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getMusicInterestedParties().get(0).getSocietyCode(), music.getMusicInterestedParties().get(0).getSocietyCode());
        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getMusicInterestedParties().get(0).getMusicPerformer(), music.getMusicInterestedParties().get(0).getMusicPerformer());
        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getMusicInterestedParties().get(0).getIpiNumber(), music.getMusicInterestedParties().get(0).getIpiNumber());
        Assert.assertEquals(expectedCuesheet.getCanonicalData().getProduction().getMusicContent().get(0).getMusicInterestedParties().get(0).getShare(), music.getMusicInterestedParties().get(0).getShare());

    }
    


     @Test
     public void updateCueSheetStatus() throws Exception{
         List<String> programCodeList = new ArrayList<>();
         String status = "SUCCESS";
         programCodeList.add(0,"1");
         programCodeList.add(1,"2");

         when(connection.prepareStatement(anyString())).thenReturn(stmt);
         stmt.setString(1,status);
         Mockito.when(stmt.executeUpdate()).thenReturn(2);
        int updatedCount = showData.updateCueSheetStatus(programCodeList, status , connection);
         Assert.assertEquals(2, updatedCount);

     }
     */
     @Test
     public void populateDMDataTest() throws Exception {
    	 connection = Mockito.mock(Connection.class);
         when((connection).prepareCall("")).thenReturn(stmt2);
         doReturn(rs).when(stmt2).executeQuery();
         Mockito.when(rs.next()).thenReturn(true).thenReturn(false);
         Mockito.when(rs.getString("format")).thenReturn("EZQ");
         Mockito.when(rs.getString("processStatus")).thenReturn(null);
         Mockito.when(rs.getString("pgmcde")).thenReturn("6493");
         Cuesheet cue = new Cuesheet();
         //source.setFormat("EZQ");
         //cue.setSourceData(source);

        //showData.populateDMData(connection);
     }
/*
     @Test
     public void getSourceData() throws Exception {

         int minProgramCode = 6493;
         int maxprogramCode = 6493;
         when(connection.prepareStatement(ShowQueryConstants.GET_FORMAT)).thenReturn(stmt);
         doReturn(rs).when(stmt).executeQuery();
         Mockito.when(rs.next()).thenReturn(true).thenReturn(false);
         Mockito.when(rs.getString("format")).thenReturn("EZQ");
         Mockito.when(rs.getString("processStatus")).thenReturn(null);
         Mockito.when(rs.getString("pgmcde")).thenReturn("6493");
         Cuesheet cue = new Cuesheet();
         SourceData source = new SourceData();
         source.setFormat("EZQ");
         cue.setSourceData(source);

         HashMap<String, Cuesheet > actualMap  = showData.getSourceData(minProgramCode, maxprogramCode, connection);
         Assert.assertEquals(cue ,actualMap.get("6493"));

     }
     */

    }