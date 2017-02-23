package com.booking.replication.configuration;

import java.util.Collections;
import java.util.List;

/**
 * Created by edmitriev on 2/22/17.
 */
public class HiveImportsConfiguration {
        public List<String> tables = Collections.emptyList();

        /**
         * getTablesForWhichToTrackDailyChanges
         * @return
         */
        public List<String> getTables() {
                return tables;
        }
}
