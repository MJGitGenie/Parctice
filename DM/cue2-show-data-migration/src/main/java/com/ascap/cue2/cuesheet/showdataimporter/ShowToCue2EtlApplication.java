package com.ascap.cue2.cuesheet.showdataimporter;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;


public class ShowToCue2EtlApplication {
    private static Properties properties = new Properties();
    private static Integer maxNumberOfPrograms;
    private static String incrementalDm = "incremental";
    private final static Log LOG = LogFactory.getLog(ShowToCue2EtlApplication.class.getName());

    protected static Namespace buildNamespace(String[] args) {
        Namespace namespace = null;
        ArgumentParser parser = ArgumentParsers.newFor("Show to Cue2 DM").build().defaultHelp(true);
        parser.addArgument("--max-programs");  
        parser.addArgument("--db-secret");  
        parser.addArgument("--show-db-secret"); 
        parser.addArgument("--incremental"); 
        

        try {

            namespace = parser.parseArgs(args);
            maxNumberOfPrograms = Integer.parseInt(namespace.getString("max_programs"));
            properties.setProperty(incrementalDm, namespace.getString(incrementalDm));
            properties.setProperty("db_secret", namespace.getString("db_secret"));
            properties.setProperty("show_db_secret", namespace.getString("show_db_secret"));
          

        } catch (ArgumentParserException err) {
            parser.handleError(err);
            System.exit(1);
        }
        return namespace;
    }

    public static void main_OLD(String[] args) throws Exception {
        LOG.info("APPLICATION Start TIME : "+(System.currentTimeMillis()));
        long appStart = System.currentTimeMillis();
        Namespace namespace = buildNamespace(args);
        LOG.info("incremental " + namespace.getString("incremental"));
        ShowData showData = new ShowData(properties);
        showData.getConnection(Boolean.valueOf(namespace.getString("incremental")), maxNumberOfPrograms);
        LOG.info("APPLICATION END TIME : "+(System.currentTimeMillis() - appStart));

    }
}



