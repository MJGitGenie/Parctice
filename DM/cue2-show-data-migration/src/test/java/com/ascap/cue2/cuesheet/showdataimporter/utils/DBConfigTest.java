package com.ascap.cue2.cuesheet.showdataimporter.utils;

import org.junit.*;
import org.mockito.Mock;
import org.mockito.Mockito;

import com.ascap.cue2.cuesheet.showdataimporter.config.DBConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class DBConfigTest {


    @Mock
    private static Connection connection;
    @Mock
    private static PreparedStatement stmt;
    @Mock
    private static ResultSet rs;

    static DBConfig dBConfig;
    @BeforeClass
    public static void setUp() throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        when(connection.prepareStatement(any(String.class))).thenReturn(stmt);
        PreparedStatement stmt = Mockito.mock(PreparedStatement.class);
        ResultSet rs = Mockito.mock(ResultSet.class);
        dBConfig= new DBConfig("","");
    }


    @Test
    public void shouldCloseResources() {
        dBConfig.closeResources(connection,stmt,rs);
        assertEquals(connection == null, true);
        assertEquals(stmt == null, true);
        assertEquals(rs == null, true);

    }

    @Test
    public void closeResourcesWithNullShouldNotThrow() {
        dBConfig.closeResources(null,null, null);
    }
}