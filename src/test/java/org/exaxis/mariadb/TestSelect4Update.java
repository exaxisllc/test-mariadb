package org.exaxis.mariadb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.MariaDbDataSource;
import org.mariadb.jdbc.MariaDbPoolDataSource;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class TestSelect4Update {
  private static JdbcDatabaseContainer mariaDB;

  @BeforeAll
  public static void setup() {
    mariaDB = new MariaDBContainer(DockerImageName.parse("mariadb:12.2-rc"))
            .withInitScript("initDB.sql")
            .withDatabaseName("test")
            .withUsername("test")
            .withPassword("test");
    mariaDB.start();
  }

  @AfterAll
  public static void teardown() {
    if (mariaDB!=null)
      mariaDB.stop();
  }

  @Test
  public void testDriverManager() {
    var jdbcUrl = mariaDB.withUrlParam("user", mariaDB.getUsername()).withUrlParam("password", mariaDB.getPassword()).getJdbcUrl();

    try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
      conn.setAutoCommit(false);
      try (Statement stmt = conn.createStatement()) {

        stmt.executeQuery("select * from test_objects where id = 2 for update");

        try (Connection conn2 = DriverManager.getConnection(jdbcUrl)) {
          conn2.setAutoCommit(false);
          try (Statement stmt2 = conn.createStatement()) {
            stmt2.executeQuery("select * from test_objects where id = 2 for update nowait");
            Assertions.fail("SelectForUpdateNoWait should fail:");
          } catch (Exception e) {
            System.err.println(e.getMessage());
          }
        } catch (Exception e) {
          System.err.println(e.getMessage());
          Assertions.fail("testDriverManager should get conn2: "+e.getMessage());
        }
      } catch (Exception e) {
        System.err.println(e.getMessage());
        Assertions.fail("testDriverManager should create stmt:"+e.getMessage());
      }
    } catch (Exception e) {
      System.err.println(e.getMessage());
      Assertions.fail("testDriverManager should get conn:"+e.getMessage());
    }
  }

  @Test
  public void testPooledDataSource() {
    try (var ds = new MariaDbPoolDataSource()) {
      ds.setUser(mariaDB.getUsername());
      ds.setPassword(mariaDB.getPassword());
      ds.setUrl(mariaDB.getJdbcUrl());
      testSelect4Update(ds);
    } catch (Exception e) {
      System.err.println(e.getMessage());
      Assertions.fail("testPooledDataSource should create datasource:" + e.getMessage());
    }
  }

  @Test
  public void testDataSource() {
      MariaDbDataSource ds;
      try {
        ds = new MariaDbDataSource();
        ds.setPassword(mariaDB.getPassword());
        ds.setUser(mariaDB.getUsername());
        ds.setUrl(mariaDB.getJdbcUrl());
        testSelect4Update(ds);
      } catch (Exception e) {
        System.err.println(e.getMessage());
        Assertions.fail("testDataSource should create datasource:" + e.getMessage());
      }
  }

  private void testSelect4Update(DataSource ds) {
     try (Connection conn = ds.getConnection()) {
      conn.setAutoCommit(false);
      try (Statement stmt = conn.createStatement()) {

        stmt.executeQuery("select * from test_objects where id = 2 for update");

        try (Connection conn2 = ds.getConnection()) {
          conn2.setAutoCommit(false);
          try (Statement stmt2 = conn.createStatement()) {
            stmt2.executeQuery("select * from test_objects where id = 2 for update nowait");
            Assertions.fail("SelectForUpdateNoWait should fail:");
          } catch (Exception e) {
            System.err.println(e.getMessage());
          }
        } catch (Exception e) {
          System.err.println(e.getMessage());
          Assertions.fail("testSelect4Update should get conn2: "+e.getMessage());
        }
      } catch (Exception e) {
        System.err.println(e.getMessage());
        Assertions.fail("testSelect4Update should create stmt:"+e.getMessage());
      }
    } catch (Exception e) {
      System.err.println(e.getMessage());
      Assertions.fail("testSelect4Update should get conn:"+e.getMessage());
    }
  }

}
