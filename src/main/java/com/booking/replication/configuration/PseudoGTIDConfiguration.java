package com.booking.replication.configuration;

import java.io.Serializable;

/**
 * Created by edmitriev on 2/22/17.
 */
public class PseudoGTIDConfiguration implements Serializable  {
    String p_gtid_pattern;
    String p_gtid_prefix;
}
