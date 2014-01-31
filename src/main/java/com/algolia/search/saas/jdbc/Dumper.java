package com.algolia.search.saas.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.commons.cli.ParseException;
import org.json.JSONException;
import org.json.simple.JSONObject;

public class Dumper extends Worker {

    public Dumper(JSONObject configuration) throws SQLException, ParseException, JSONException {
        super(configuration);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() throws ParseException, SQLException {
        final String query = (String) configuration.get(Connector.CONF_QUERY);
        assert (query != null);
        final String idField = (String) configuration.get(Connector.CONF_UNIQUE_ID_FIELD);
        assert (idField != null);

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
                	if (rsmd.getColumnName(i).equals(idField)) {
                		obj.put("objectID", rs.getObject(i));
                	} else if (this.attributes == null || this.attributes.contains(rsmd.getColumnName(i))) {
                		obj.put(rsmd.getColumnName(i), rs.getObject(i));
                	}
                }
                System.out.println(obj);
            }
        } finally {
            rs.close();
        }
    }

}
