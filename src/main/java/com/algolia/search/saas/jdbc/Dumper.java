package com.algolia.search.saas.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.json.simple.JSONObject;

public class Dumper extends Worker {

    public Dumper(CommandLine cli) throws SQLException, ParseException {
        super(cli);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() throws ParseException, SQLException {
        final String query = cli.getOptionValue("query");
        assert (query != null);

        java.sql.PreparedStatement stmt = database.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE);
        if (stmt instanceof com.mysql.jdbc.Statement) {
            ((com.mysql.jdbc.Statement) stmt).enableStreamingResults();
        }
        ResultSet rs = stmt.executeQuery();
        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            int columns = rsmd.getColumnCount();
            while (rs.next()) {
                JSONObject obj = new JSONObject();
                for (int i = 1; i < columns + 1; i++) {
                    String col = rsmd.getColumnName(i);
                    switch (rsmd.getColumnType(i)) {
                    case java.sql.Types.ARRAY:
                        obj.put(col, rs.getArray(col));
                        break;
                    case java.sql.Types.BIGINT:
                        obj.put(col, rs.getInt(col));
                        break;
                    case java.sql.Types.BOOLEAN:
                        obj.put(col, rs.getBoolean(col));
                        break;
                    case java.sql.Types.BLOB:
                        obj.put(col, rs.getBlob(col));
                        break;
                    case java.sql.Types.DOUBLE:
                        obj.put(col, rs.getDouble(col));
                        break;
                    case java.sql.Types.FLOAT:
                        obj.put(col, rs.getFloat(col));
                        break;
                    case java.sql.Types.INTEGER:
                        obj.put(col, rs.getInt(col));
                        break;
                    case java.sql.Types.NVARCHAR:
                        obj.put(col, rs.getNString(col));
                        break;
                    case java.sql.Types.VARCHAR:
                        obj.put(col, rs.getString(col));
                        break;
                    case java.sql.Types.TINYINT:
                        obj.put(col, rs.getInt(col));
                        break;
                    case java.sql.Types.SMALLINT:
                        obj.put(col, rs.getInt(col));
                        break;
                    case java.sql.Types.DATE:
                        obj.put(col, rs.getDate(col));
                        break;
                    case java.sql.Types.TIMESTAMP:
                        obj.put(col, rs.getTimestamp(col));
                        break;
                    default:
                        obj.put(col, rs.getObject(col));
                        break;
                    }
                }
                System.out.println(obj);
            }
        } finally {
            rs.close();
        }
    }

}
