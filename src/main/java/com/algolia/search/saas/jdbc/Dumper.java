package com.algolia.search.saas.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.ParseException;
import org.json.JSONException;
import org.json.simple.JSONObject;

import com.algolia.search.saas.AlgoliaException;

public class Dumper extends Worker {

    public Dumper(JSONObject configuration) throws SQLException, ParseException, JSONException {
        super(configuration);
        
        String batchSize = (String) this.configuration.get(Connector.CONF_BATCH_SIZE);
        this.batchSize = batchSize == null ? 1000 : Integer.parseInt(batchSize);
    }

    @Override
    public void run() throws SQLException, AlgoliaException {
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
            List<org.json.JSONObject> objects = new ArrayList<org.json.JSONObject>();
            while (rs.next()) {
                org.json.JSONObject obj = new org.json.JSONObject();
                for (int i = 1; i < columns + 1; i++) {
                    try {
                    	if (rsmd.getColumnName(i).equals(idField)) {
                    		obj.put("objectID", rs.getObject(i));
                    	} else {
                    		obj.put(rsmd.getColumnName(i), rs.getObject(i));
                    	}
                    } catch (JSONException e) {
                        throw new Error(e);
                    }
                }
                objects.add(obj);
                if (objects.size() >= batchSize) {
                    this.index.addObjects(objects);
                    objects.clear();
                }
            }
            if (!objects.isEmpty()) {
                this.index.addObjects(objects);
            }
        } finally {
            rs.close();
        }
    }

    private final int batchSize;
}
