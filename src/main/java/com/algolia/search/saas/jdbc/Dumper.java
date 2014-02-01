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
        
        Object batchSize = this.configuration.get(Connector.CONF_BATCH_SIZE);
        this.batchSize = batchSize == null ? 1000 : (batchSize instanceof Integer || batchSize instanceof Long ? (Long) batchSize : Long.parseLong((String) batchSize));
        this.query = (String) configuration.get(Connector.CONF_QUERY);
        assert (query != null);

        this.idField = (String) configuration.get(Connector.CONF_UNIQUE_ID_FIELD);
        this.stmt = database.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE);
        if (stmt instanceof com.mysql.jdbc.Statement) {
            ((com.mysql.jdbc.Statement) stmt).enableStreamingResults();
        }
        //TODO Activate on the other jdbc
    }

    @Override
    public void run() throws SQLException, AlgoliaException, JSONException {   
        ResultSet rs = stmt.executeQuery();
        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            int columns = rsmd.getColumnCount();
            List<org.json.JSONObject> actions = new ArrayList<org.json.JSONObject>();
            while (rs.next()) {
                org.json.JSONObject obj = new org.json.JSONObject();
                for (int i = 1; i < columns + 1; i++) {
                    try {
                        if (idField != null && rsmd.getColumnName(i).equals(idField)) {
                            obj.put("objectID", rs.getObject(i));
                        } else {
                            obj.put(rsmd.getColumnName(i), rs.getObject(i));
                        }
                    } catch (JSONException e) {
                        throw new Error(e);
                    }
                }
                org.json.JSONObject action = new org.json.JSONObject();
                action.put("action", "addObject");
                action.put("body",obj);
                actions.add(action);
                if (actions.size() >= batchSize) {
                    actions = addSetting(actions, "0"); //TODO
                    //this.index.batch(actions); TODO
                    actions.clear();
                }
            }
            if (!actions.isEmpty()) {
                this.index.addObjects(actions);
            }
        } finally {
            rs.close();
        }
    }

    private final java.sql.PreparedStatement stmt;
    private final String query;
    private final String idField;
    private final long batchSize;
}
