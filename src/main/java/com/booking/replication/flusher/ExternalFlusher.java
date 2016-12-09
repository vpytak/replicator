package com.booking.replication.flusher;

import java.util.List;
import java.util.Arrays;

import java.io.File;
import java.io.PrintStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * Wraps the current Python implementation of the binlog flusher.
 * Assumes that the data-flusher.py script has been packaged in resources/binlog-flusher.
 * Does not support parameterizing slave control. It instead just has data-flusher.py do it.
 *
 * Example:
 *
 * ExternalFlusher flusher = new ExternalFlusher("localhost", "meta_replicator", "~/.my.cnf");
 * flusher.flushTable("abc");
 *
 */

public class ExternalFlusher extends Flusher {
     private static final Logger LOGGER = LoggerFactory.getLogger(ExternalFlusher.class);
     private ProcessBuilder pb;

     /**
      * Extract a resource file into a temporary directory
      * @param path absolute path to resources object
      * @return path to temporary file
      */
     private String extractResourcesFile(String path) throws FlusherException {
          try {
               InputStream inputStream = ExternalFlusher.class.getResourceAsStream(path);
               File tempFile = File.createTempFile("flusher", null);
               PrintStream printStream = new PrintStream(tempFile);
               BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
               String readline;

               while ((readline = bufferedReader.readLine()) != null) {
                   printStream.println(readline);
               }

               return tempFile.getPath();
          }
          catch (Exception e) {
               LOGGER.info("Can't extract resource {}", path, e);
               throw new FlusherException();
          }
          // FIXME(psilva): clean up in finally
     }

     /**
      * Initialize a ProcessBuilder object
      * @param table MySQL table to flush
      */
     private void buildProcess(String table) throws FlusherException {
          String path = "/binlog-flusher/data-flusher.py";
          try {
               LOGGER.info("Get resource from path {}", path);
               String script = extractResourcesFile(path);
               LOGGER.info("Got resource from path {}", script);
               List<String> command = Arrays.asList("python",
                                                    script,
                                                    "--host", getHost(),
                                                    "--db", getDatabase(),
                                                    "--mycnf", getMycnf(),
                                                    "--table", table,
                                                    "--stop-slave",
                                                    "--start-slave");
               LOGGER.info("Get ProcessBuilder from command {}", String.join(" ", command));
               this.pb = new ProcessBuilder(command);
               LOGGER.info("Got ProcessBuilder from command {}", String.join(" ", this.pb.command()));
          }
          catch (Exception e) {
               throw new FlusherException("Can't run " + path, e);
          }
     }

     /**
      * @param host MySQL slave hostname (host:port format)
      * @param database MySQL database to flush
      * @param mycnf path to MySQL client configuration file
      */
     public ExternalFlusher(String host, String database, String mycnf) {
          super(host, database, mycnf);
     }

     /**
      * data-flusher.py does not allow this by itself.
      */
     void stopSlave() {
          throw new UnsupportedOperationException("stopSlave is an atomic operation in flushTable");
     }

     /**
      * data-flusher.py does not allow this by itself.
      */
     void startSlave() {
          throw new UnsupportedOperationException("startSlave is an atomic operation in flushTable");
     }


     /**
      * Flush a table by calling the external process.
      * @param table MySQL table to flush
      */
     void flushTable(String table) throws FlusherException {
          try {
               buildProcess(table);
               Process proc = this.pb.start();

               // FIXME(psilva): refactor this out of here
               BufferedReader outReader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
               BufferedReader errReader = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
               String errLine = null;
               String outLine = null;
               while ((outLine = outReader.readLine()) != null || (errLine = errReader.readLine()) != null) {
                    if (outLine != null) {
                         System.out.println(outLine);
                    }
                    if (errLine != null) {
                         System.err.println(outLine);
                    }
               }

               int exit = proc.waitFor();
               if (exit != 0) {
                    throw new FlusherException("Process terminated abnormaly " + exit);
               }
          }
          catch (Exception e) {
               throw new FlusherException("Can't flush table " + table, e);
          }

     }

     /**
      * Flush multiple tables by calling the external process for eaach one.
      * @param tables list of MySQL tables to flush
      */
     void flushTables(Iterable<String> tables) throws FlusherException {
          for (String table : tables) {
               flushTable(table);
          }
     }

     public static void main(String [] args) throws FlusherException {
          ExternalFlusher flusher = new ExternalFlusher("localhost", "meta_replicator", "~/.my.cnf");
          flusher.flushTable("abc");
     }

}
