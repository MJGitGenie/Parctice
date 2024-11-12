package com.ascap.cue2.cuesheet.showdataimporter.config;

public class ProgramError extends CueError {
	private static final long serialVersionUID = -3108185129042844748L;

	private String programCode;
	private String sequenceNumber;



	public String getProgramCode() {
		return programCode;
	}

	public void setProgramCode(String programCode) {
		this.programCode = programCode;
	}

	public String getSequenceNumber() {
		return sequenceNumber;
	}

	public void setSequenceNumber(String sequenceNumber) {
		this.sequenceNumber = sequenceNumber;
	}
}
