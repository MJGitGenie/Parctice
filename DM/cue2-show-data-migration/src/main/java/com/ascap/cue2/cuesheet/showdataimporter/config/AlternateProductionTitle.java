package com.ascap.cue2.cuesheet.showdataimporter.config;

public class AlternateProductionTitle {
	private static final long serialVersionUID = 8496252531975379746L;

	private long programCode;
	private long seriesCode;

	private String alternateLanguage;

	private String productionTitle;

	private String programAction;

	private String seriesAction;
	private String titleCreateDateTime;
	private String titleCreateId;
	private String titleUpdateDateTime;
	private String titleUpdateId;
	

	public String getAlternateLanguage() {
		return alternateLanguage;
	}

	public void setAlternateLanguage(String alternateLanguage) {
		this.alternateLanguage = alternateLanguage;
	}

	public String getProductionTitle() {
		return productionTitle;
	}

	public void setProductionTitle(String productionTitle) {
		this.productionTitle = productionTitle;
	}

	public String getProgramAction() {
		return programAction;
	}

	public void setProgramAction(String programAction) {
		this.programAction = programAction;
	}

	public String getSeriesAction() {
		return seriesAction;
	}

	public void setSeriesAction(String seriesAction) {
		this.seriesAction = seriesAction;
	}

	public long getProgramCode() {
		return programCode;
	}

	public void setProgramCode(long programCode) {
		this.programCode = programCode;
	}

	public long getSeriesCode() {
		return seriesCode;
	}

	public void setSeriesCode(long seriesCode) {
		this.seriesCode = seriesCode;
	}

	public String getTitleCreateDateTime() {
		return titleCreateDateTime;
	}

	public void setTitleCreateDateTime(String titleCreateDateTime) {
		this.titleCreateDateTime = titleCreateDateTime;
	}

	public String getTitleCreateId() {
		return titleCreateId;
	}

	public void setTitleCreateId(String titleCreateId) {
		this.titleCreateId = titleCreateId;
	}

	public String getTitleUpdateDateTime() {
		return titleUpdateDateTime;
	}

	public void setTitleUpdateDateTime(String titleUpdateDateTime) {
		this.titleUpdateDateTime = titleUpdateDateTime;
	}

	public String getTitleUpdateId() {
		return titleUpdateId;
	}

	public void setTitleUpdateId(String titleUpdateId) {
		this.titleUpdateId = titleUpdateId;
	}


}
