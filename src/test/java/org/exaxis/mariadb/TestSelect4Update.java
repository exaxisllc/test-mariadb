package org.exaxis.mariadb;

import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.MariaDbPoolDataSource;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.junit.Assert.fail;

public class TestSelect4Update {

  @Test
  public void testDriverManager() {
    var jdbcUrl = "jdbc:tc:mariadb:11.8.2:///test?TC_INITSCRIPT=initDB.sql";

    try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
      conn.setAutoCommit(false);
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("INSERT INTO test_objects (id, name) values (1, \"record 1\")");
        stmt.executeUpdate("INSERT INTO test_objects (id, name) values (2, \"record 2\")");
        conn.commit();

        stmt.executeQuery("select * from test_objects where id = 2 for update");

        try (Connection conn2 = DriverManager.getConnection(jdbcUrl)) {
          conn2.setAutoCommit(false);
          try (Statement stmt2 = conn.createStatement()) {
            stmt2.executeQuery("select * from test_objects where id = 2 for update nowait");
            fail("SelectForUpdateNoWait should fail:");
          } catch (Exception e) {
            System.err.println(e.getMessage());
          }
        } catch (Exception e) {
          System.err.println(e.getMessage());
          fail("SelectForUpdateNoWait should get conn2: "+e.getMessage());
        }
      } catch (Exception e) {
        System.err.println(e.getMessage());
        fail("SelectForUpdateNoWait should create stmt:"+e.getMessage());
      }
    } catch (Exception e) {
      System.err.println(e.getMessage());
      fail("SelectForUpdateNoWait should get conn:"+e.getMessage());
    }
  }

  @Test
  public void testDataSource() {
    try (var mariaDB = new MariaDBContainer<>(DockerImageName.parse("mariadb:11.8.2"))
            .withInitScript("initDB.sql")
            .withDatabaseName("test");
    ) {
      mariaDB.start();

      try (var ds = new MariaDbPoolDataSource()) {
        ds.setUrl(mariaDB.getJdbcUrl());
        ds.setUser(mariaDB.getUsername());
        ds.setPassword(mariaDB.getPassword());

        try (Connection conn = ds.getConnection()) {
          conn.setAutoCommit(false);
          try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("INSERT INTO test_objects (id, name) values (1, \"record 1\")");
            stmt.executeUpdate("INSERT INTO test_objects (id, name) values (2, \"record 2\")");
            conn.commit();

            stmt.executeQuery("select * from test_objects where id = 2 for update");

            try (Connection conn2 = ds.getConnection()) {
              conn2.setAutoCommit(false);
              try (Statement stmt2 = conn.createStatement()) {
                stmt2.executeQuery("select * from test_objects where id = 2 for update nowait");
                fail("SelectForUpdateNoWait should fail:");
              } catch (Exception e) {
                System.err.println(e.getMessage());
              }
            } catch (Exception e) {
              System.err.println(e.getMessage());
              fail("SelectForUpdateNoWait should get conn2: "+e.getMessage());
            }
          } catch (Exception e) {
            System.err.println(e.getMessage());
            fail("SelectForUpdateNoWait should create stmt:"+e.getMessage());
          }
        } catch (Exception e) {
          System.err.println(e.getMessage());
          fail("SelectForUpdateNoWait should get conn:"+e.getMessage());
        }
      } catch (Exception e) {
        System.err.println(e.getMessage());
        fail("SelectForUpdateNoWait should create datasource:"+e.getMessage());
      }
    } catch (Exception e) {
      System.err.println(e.getMessage());
      fail("SelectForUpdateNoWait should create test container:"+e.getMessage());
    }

  }

}
