package com.lob.tunner.server.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Access controldb.accounts table
 *
 * CREATE TABLE accounts ( " +
 "  id BIGINT not null primary key,
 "  name VARCHAR(128) not null unique,
 "  properties TEXT(65535) not null - this is a JSON string
 " ) engine=innodb
 */
public class AccountDao {
    private final static String SQL_GETALL = "SELECT id, name, properties FROM %s.accounts";
    private final static String SQL_INSERT = "INSERT INTO %s.accounts (id, name, properties) VALUE (?, ?, ?)";
    private final static String SQL_DELETE = "DELETE FROM %s.accounts WHERE id=?";
    private final static String SQL_UPDATE = "UPDATE %s.accounts set properties=? WHERE id=?";
    private final static String SQL_GET_BY_NAME = "SELECT id, name, properties FROM %s.accounts WHERE name=?";
    private final static String SQL_GET_BY_ID = "SELECT id, name, properties FROM %s.accounts WHERE id=%d";

    /*
     * createTable must have dbName parameter although we didn't use it
     * Because we will use reflect to call createTable when create db
     */
    public static void createTable(Connection conn, String dbName) throws SQLException {
        String sql = String.format(
                "CREATE TABLE IF NOT EXISTS %s.accounts (\n" +
                        "         id BIGINT not null primary key,\n" +
                        "         name VARCHAR(128) not null unique,\n" +
                        "         properties JSON not null \n" +
                        "        ) engine=innodb ROW_FORMAT=DYNAMIC", dbName
        );
        DaoUtils.executeUpdate(conn, sql);
    }


    private final static RecordLoader<Account> _LOADER = new RecordLoader<Account>() {
        @Override
        public Account load(final ResultSet rs) throws SQLException {
            Account account = new Account();
            account.setId(rs.getLong(1));
            account.setName(rs.getString(2));
            account.setProperties(rs.getString(3));
            return account;
        }
    };

    /**
     * TODO: remove me
     */
    public static List<Account> getAll(Connection conn, String coreDb) throws SQLException {
        DbQuery q = new DbQuery(String.format(SQL_GETALL, coreDb));

        return q.queryList(conn, _LOADER::load);

        /*
        return DaoUtils.getList(conn,
                String.format(SQL_GETALL, coreDb),
                _LOADER::load);
         */
    }

    public static void add(Connection conn,
                           String dbName,
                           long id,
                           String name,
                           String properties) throws SQLException {
        String strProperties = properties==null ? "{}" : properties.toString();

        DaoUtils.executeUpdate(conn, String.format(SQL_INSERT, dbName), id, name, strProperties);
    }

    public static void add(Connection conn, String dbName, Account account) throws SQLException {
        DaoUtils.executeUpdate(conn, String.format(SQL_INSERT, dbName), account.getId(), account.getName(), account.getProperties());
    }

    public static void update(Connection conn, String dbName, long id, Account account) throws SQLException {
        // DaoUtils.executeUpdate(conn, String.format(SQL_UPDATE, dbName), account.getProperties(), id);
        String sql = _getSQL(SQL_UPDATE, dbName);
        _update(conn, sql, account.getProperties(), id);
    }

    public static void delete(Connection conn, String dbName, long id) throws SQLException {
        // DaoUtils.executeUpdate(conn, String.format(SQL_DELETE, dbName), id);
        String sql = String.format(SQL_DELETE, dbName);

        DbUpdator updator = new DbUpdator(conn, sql);

        updator.update();
    }


    public static Account get(Connection conn, String dbName, String name) throws SQLException {
        // return DaoUtils.get(conn, _getSQL(SQL_GET_BY_NAME, dbName), _LOADER::load, name);
        return DaoUtils.get(conn, SQL_GET_BY_NAME, _LOADER::load, name);
    }


    public static Account get(Connection conn, String dbName, long id) throws SQLException {
        String sql = String.format(SQL_GET_BY_ID, dbName, id);
        // return DaoUtils.get(conn, _getSQL(SQL_GET_BY_ID, dbName), _LOADER::load, id);
        return DaoUtils.get(conn, sql, _LOADER::load);
    }


    private static void _update(Connection conn, String sql, Object ...params) throws SQLException {
        DaoUtils.executeUpdate(conn, sql, params);
    }

    private static String _getSQL(String sql, String dbName) {
        return String.format(sql, dbName);
    }
}
