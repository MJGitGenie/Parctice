package com.ascap.cue2.cuesheet.showdataimporter.config;

public class MusicInterestedParty extends InterestedParty {
	private static final long serialVersionUID = -2065475488104147462L;

	private Long programCode;
	private String ipAction;

	private String ipiNumber;

	private String role;

	private String share;

	private String musicPerformer;

	private String submitterIPNumber;

	private String sequenceNumber;

	private String societyCode;

	private String workId;

	private String partyId;
	
	private String partyCreateId;
	private String partyCreateDateTime;
	private String partyUpdateId;
	private String partyUpdateDateTime;


	public String getIpAction() {
		return ipAction;
	}

	public void setIpAction(String ipAction) {
		this.ipAction = ipAction;
	}

	public String getIpiNumber() {
		return ipiNumber;
	}

	public void setIpiNumber(String ipiNumber) {
		this.ipiNumber = ipiNumber;
	}

	public String getRole() {
		return role;
	}

	public void setRole(String role) {
		this.role = role;
	}

	public String getShare() {
		return share;
	}

	public void setShare(String share) {
		this.share = share;
	}

	public String getMusicPerformer() {
		return musicPerformer;
	}

	public void setMusicPerformer(String musicPerformer) {
		this.musicPerformer = musicPerformer;
	}

	public String getSubmitterIPNumber() {
		return submitterIPNumber;
	}

	public void setSubmitterIPNumber(String submitterIPNumber) {
		this.submitterIPNumber = submitterIPNumber;
	}

	public String getSequenceNumber() {
		return sequenceNumber;
	}

	public void setSequenceNumber(String sequenceNumber) {
		this.sequenceNumber = sequenceNumber;
	}

	public String getSocietyCode() {
		return societyCode;
	}

	public void setSocietyCode(String societyCode) {
		this.societyCode = societyCode;
	}

	public String getWorkId() {
		return workId;
	}

	public void setWorkId(String workId) {
		this.workId = workId;
	}

	public String getPartyId() {
		return partyId;
	}

	public void setPartyId(String partyId) {
		this.partyId = partyId;
	}

	public Long getProgramCode() {
		return programCode;
	}

	public void setProgramCode(Long programCode) {
		this.programCode = programCode;
	}

	public String getPartyCreateId() {
		return partyCreateId;
	}

	public void setPartyCreateId(String partyCreateId) {
		this.partyCreateId = partyCreateId;
	}

	public String getPartyCreateDateTime() {
		return partyCreateDateTime;
	}

	public void setPartyCreateDateTime(String partyCreateDateTime) {
		this.partyCreateDateTime = partyCreateDateTime;
	}

	public String getPartyUpdateId() {
		return partyUpdateId;
	}

	public void setPartyUpdateId(String partyUpdateId) {
		this.partyUpdateId = partyUpdateId;
	}

	public String getPartyUpdateDateTime() {
		return partyUpdateDateTime;
	}

	public void setPartyUpdateDateTime(String partyUpdateDateTime) {
		this.partyUpdateDateTime = partyUpdateDateTime;
	}
}
