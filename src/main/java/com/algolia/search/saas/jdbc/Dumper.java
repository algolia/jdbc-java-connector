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
    	final String defaultQuery = configuration == null || configuration.get("query") == null ? null : configuration.get("query").toString();
        final String query = cli.getOptionValue("query", defaultQuery);
        final String defaultIDField = configuration == null || configuration.get("id") == null ? "id" : configuration.get("id").toString();
        assert (query != null);

        java.sql.PreparedStatement stmt = database.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE);
        if (stmt instanceof com.mysql.jdbc.Statement) {
            ((com.mysql.jdbc.Statement) stmt).enableStreamingResults();
        }
        //TODO Activate on the other jdbc
        ResultSet rs = stmt.executeQuery();
        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            int columns = rsmd.getColumnCount();
            while (rs.next()) {
                JSONObject obj = new JSONObject();
                for (int i = 1; i < columns + 1; i++) {
                	if (rsmd.getColumnName(i).equals(cli.getOptionValue("id", defaultIDField)))
                		obj.put("objectID", rs.getObject(i));
                    obj.put(rsmd.getColumnName(i), rs.getObject(i));
                }
                System.out.println(obj);
            }
        } finally {
            rs.close();
        }
    }

}
