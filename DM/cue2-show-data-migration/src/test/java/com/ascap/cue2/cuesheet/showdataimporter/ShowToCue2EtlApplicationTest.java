package com.ascap.cue2.cuesheet.showdataimporter;

import org.junit.Test;

public class ShowToCue2EtlApplicationTest {

    @Test
    public void buildNamespace() {

        String[] args = {
                "--db-secret",
                "survey-systems-rds-dev-ascap-test",
                "--show-db-secret",
                "cue2-show-dev-ascap",
                "--max-programs",
                "10000",
                "--incremental",
                "false",
        };

        ShowToCue2EtlApplication.buildNamespace(args);

    }
}
