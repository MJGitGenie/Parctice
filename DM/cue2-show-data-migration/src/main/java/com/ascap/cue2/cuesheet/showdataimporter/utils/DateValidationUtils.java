package com.ascap.cue2.cuesheet.showdataimporter.utils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class DateValidationUtils {
	
	private static final Log LOG = LogFactory.getLog(DateValidationUtils.class.getName());

	
	DateTimeFormatter formatter = null; 
	 
    public DateValidationUtils(String dateFormat) {
        this.formatter = DateTimeFormatter.ofPattern(dateFormat);
    }
    
    public boolean isValid(String dateStr) {
    	
        try {
            LocalDate.parse(dateStr, formatter);
        } catch (DateTimeParseException dtpe) {
        	LOG.info("The string is not a date and time: " + dtpe.getMessage());
            return false;
        }
        return true;
    }
}
