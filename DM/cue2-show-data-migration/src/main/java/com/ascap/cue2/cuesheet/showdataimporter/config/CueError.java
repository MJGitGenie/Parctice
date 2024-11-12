package com.ascap.cue2.cuesheet.showdataimporter.config;

public class CueError {

	private String errorCode;
	private String errorMessage;
	private String errorSeverity;
	private String revisionType;
	private String showCreateDateTime;
	private String showCreateUserId;
	private String showUpdateDateTime;
	private String showUpdateUserId;

	public String getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(String errorCode) {
		this.errorCode = errorCode;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public String getErrorSeverity() {
		return errorSeverity;
	}

	public void setErrorSeverity(String errorSeverity) {
		this.errorSeverity = errorSeverity;
	}

	public String getRevisionType() {
		return revisionType;
	}

	public void setRevisionType(String revisionType) {
		this.revisionType = revisionType;
	}

	public String getShowCreateDateTime() {
		return showCreateDateTime;
	}

	public void setShowCreateDateTime(String showCreateDateTime) {
		this.showCreateDateTime = showCreateDateTime;
	}

	public String getShowCreateUserId() {
		return showCreateUserId;
	}

	public void setShowCreateUserId(String showCreateUserId) {
		this.showCreateUserId = showCreateUserId;
	}

	public String getShowUpdateDateTime() {
		return showUpdateDateTime;
	}

	public void setShowUpdateDateTime(String showUpdateDateTime) {
		this.showUpdateDateTime = showUpdateDateTime;
	}

	public String getShowUpdateUserId() {
		return showUpdateUserId;
	}

	public void setShowUpdateUserId(String showUpdateUserId) {
		this.showUpdateUserId = showUpdateUserId;
	}

}
