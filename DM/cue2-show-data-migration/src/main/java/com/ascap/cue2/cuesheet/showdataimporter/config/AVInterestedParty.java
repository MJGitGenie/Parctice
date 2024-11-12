package com.ascap.cue2.cuesheet.showdataimporter.config;

public class AVInterestedParty extends InterestedParty {
	private static final long serialVersionUID = 5055313243063781225L;

	private Long programCode;
	private String role;
	private String action;
	private String avCreateDateTime;
	private String avCreateId;
	private String avUpdateDateTime;
	private String avUpdateId;

	public String getRole() {
		return role;
	}

	public void setRole(String role) {
		this.role = role;
	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public Long getProgramCode() {
		return programCode;
	}

	public void setProgramCode(Long programCode) {
		this.programCode = programCode;
	}

	public String getAvCreateDateTime() {
		return avCreateDateTime;
	}

	public void setAvCreateDateTime(String avCreateDateTime) {
		this.avCreateDateTime = avCreateDateTime;
	}

	public String getAvCreateId() {
		return avCreateId;
	}

	public void setAvCreateId(String avCreateId) {
		this.avCreateId = avCreateId;
	}

	public String getAvUpdateDateTime() {
		return avUpdateDateTime;
	}

	public void setAvUpdateDateTime(String avUpdateDateTime) {
		this.avUpdateDateTime = avUpdateDateTime;
	}

	public String getAvUpdateId() {
		return avUpdateId;
	}

	public void setAvUpdateId(String avUpdateId) {
		this.avUpdateId = avUpdateId;
	}

}
