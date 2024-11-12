package com.ascap.cue2.cuesheet.showdataimporter.config;

public class UmbrellaElement {
	private long programCode;
	private long umbrellaProgramCode;
	private String umbrellaCreateId;
	private String umbrellaCreateDateTime;
	private String umbrellaUpdateId;
	private String umbrellaUpdateDateTime;
	
	
	public long getProgramCode() {
		return programCode;
	}
	public void setProgramCode(long programCode) {
		this.programCode = programCode;
	}
	public long getUmbrellaProgramCode() {
		return umbrellaProgramCode;
	}
	public void setUmbrellaProgramCode(long umbrellaProgramCode) {
		this.umbrellaProgramCode = umbrellaProgramCode;
	}
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("UmbrellaElement [programCode=");
		builder.append(programCode);
		builder.append(", umbrellaProgramCode=");
		builder.append(umbrellaProgramCode);
		builder.append("]");
		return builder.toString();
	}
	public String getUmbrellaCreateId() {
		return umbrellaCreateId;
	}
	public void setUmbrellaCreateId(String umbrellaCreateId) {
		this.umbrellaCreateId = umbrellaCreateId;
	}
	public String getUmbrellaCreateDateTime() {
		return umbrellaCreateDateTime;
	}
	public void setUmbrellaCreateDateTime(String umbrellaCreateDateTime) {
		this.umbrellaCreateDateTime = umbrellaCreateDateTime;
	}
	public String getUmbrellaUpdateId() {
		return umbrellaUpdateId;
	}
	public void setUmbrellaUpdateId(String umbrellaUpdateId) {
		this.umbrellaUpdateId = umbrellaUpdateId;
	}
	public String getUmbrellaUpdateDateTime() {
		return umbrellaUpdateDateTime;
	}
	public void setUmbrellaUpdateDateTime(String umbrellaUpdateDateTime) {
		this.umbrellaUpdateDateTime = umbrellaUpdateDateTime;
	}
	
}
