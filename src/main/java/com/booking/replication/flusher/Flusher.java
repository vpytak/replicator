package com.booking.replication.flusher;

public abstract class Flusher {
     private String host;
     private String database;
     private String mycnf;

     public String getHost() {return this.host;}
     public String getDatabase() {return this.database;}
     public String getMycnf() {return this.mycnf;}

     Flusher() {}
     Flusher(String host, String database, String mycnf) {
          this.host = host;
          this.database = database;
          this.mycnf = mycnf;
     }

     abstract void stopSlave() throws FlusherException, UnsupportedOperationException;
     abstract void startSlave() throws FlusherException, UnsupportedOperationException;
     abstract void flushTable(String table) throws FlusherException;
     abstract void flushTables(Iterable<String> tables) throws FlusherException;
}
