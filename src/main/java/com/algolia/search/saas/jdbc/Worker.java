package com.algolia.search.saas.jdbc;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import org.apache.commons.cli.ParseException;
import org.json.JSONException;
import org.json.simple.JSONObject;

import com.algolia.search.saas.APIClient;
import com.algolia.search.saas.AlgoliaException;
import com.algolia.search.saas.Index;

public abstract class Worker {
    public Worker(JSONObject configuration) throws SQLException, org.apache.commons.cli.ParseException, JSONException {
        this.configuration = configuration;

        // Algolia connection
        final String applicationId = (String) configuration.get(Connector.CONF_APPLICATION_ID);
        assert (applicationId != null);
        final String apiKey = (String) configuration.get(Connector.CONF_API_KEY);
        assert (apiKey != null);
        final String index = (String) configuration.get(Connector.CONF_INDEX);
        assert (index != null);
        this.client = new APIClient(applicationId, apiKey);
        this.index = this.client.initIndex(index);

        // DB configuration
        this.idField = (String) configuration.get(Connector.CONF_PRIMARY_FIELD);
        assert (idField != null);

        // JDBC connection
        this.source = (String) configuration.get(Connector.CONF_SOURCE);
        if (source == null) {
            throw new ParseException("Missing '" + Connector.CONF_SOURCE + "' option");
        }
        this.database = DriverManager.getConnection(source, (String) configuration.get(Connector.CONF_USERNAME),
                (String) configuration.get(Connector.CONF_PASSWORD));

        // Batch configuration
        Object batchSize = this.configuration.get(Connector.CONF_BATCH_SIZE);
        this.batchSize = batchSize == null ? 1000 : (batchSize instanceof Integer || batchSize instanceof Long ? (Long) batchSize : Long
                .parseLong((String) batchSize));
        
        // fetch userData settings
        org.json.JSONObject userData;
        try {
            org.json.JSONObject settings = this.index.getSettings();
            userData = settings.getJSONObject("userData");
        } catch (Exception e) { //Index not found or First dump
            userData = new org.json.JSONObject();
        }
        this.userData = userData;
    }
    
    public void checkAndReconnect() throws SQLException {
    	if (!this.database.isClosed() && this.database.isValid(1)) {
    		return;
    	}
        this.database = DriverManager.getConnection(source, (String) configuration.get(Connector.CONF_USERNAME),
                (String) configuration.get(Connector.CONF_PASSWORD));
    }

    public void close() throws SQLException {
        if (this.database != null) {
            this.database.close();
        }
    }

    protected abstract void onRow(ResultSetMetaData rsmd, ResultSet rs, String objectID, org.json.JSONObject obj) throws SQLException, AlgoliaException;
    protected abstract void fillUserData(org.json.JSONObject userData) throws JSONException;
    public abstract void run() throws SQLException, AlgoliaException, JSONException;

    protected void push(List<org.json.JSONObject> actions) throws AlgoliaException {
        if (actions.isEmpty()) {
            return;
        }
        org.json.JSONObject settings = null;
        try {
            settings = new org.json.JSONObject();
            fillUserData(userData);
            settings.put("userData", userData);
        } catch (JSONException e) {
            throw new Error(e);
        }

        Connector.LOGGER.log(Level.INFO, "    Push batch of " + (actions.size() - 1) + " actions");
        this.index.batch(actions);
        this.index.setSettings(settings);
    }

    protected void iterateOnQuery(String query) throws SQLException, JSONException, AlgoliaException {
        Connector.LOGGER.info("  Executing query: " + query);
        java.sql.PreparedStatement stmt = database.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY); 
        if (stmt instanceof com.mysql.jdbc.Statement) {
            ((com.mysql.jdbc.Statement) stmt).enableStreamingResults();
        }
        // TODO: postgresql? sqlite?
        ResultSet rs = stmt.executeQuery();
        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            int columns = rsmd.getColumnCount();
            while (rs.next()) {
                String objectID = null;
                org.json.JSONObject obj = new org.json.JSONObject();

                Object lat = null;
                Object lng = null;

                for (int i = 1; i < columns + 1; i++) {
                    try {
                        String colName = rsmd.getColumnLabel(i);

                        if (colName.equals(idField)) {
                            objectID = rs.getObject(i).toString();
                            obj.put("objectID", objectID);
                        } else { // TODO: handle all projected column name with . and bucket the fields as child objects
                            int colType = rsmd.getColumnType(i);
                            if (colType == Types.ARRAY) {
                                obj.put(rsmd.getColumnLabel(i), rs.getArray(i).getArray());
                            }
                            else if(colName.equals(GEO_LAT_FIELD) && IsGeoValueType(colType)) {
                                lat = rs.getObject(i);
                                if(lng != null)
                                    obj.put("_geoloc", BuildGeoJSON(lat, lng));
                            }
                            else if(colName.equals(GEO_LNG_FIELD) && IsGeoValueType(colType)) {
                                lng = rs.getObject(i);
                                if(lat != null)
                                    obj.put("_geoloc", BuildGeoJSON(lat, lng));
                            }
                            else {
                                obj.put(rsmd.getColumnLabel(i), rs.getObject(i));
                            }
                        }
                    } catch (JSONException e) {
                        throw new Error(e);
                    }
                }
                if (objectID == null) {
                	Connector.LOGGER.warning("Primary field not found in row : skip");
                }
                onRow(rsmd, rs, objectID, obj);
                if (actions.size() >= batchSize) {
                    push(actions);
                    actions.clear();
                }
            }
            push(actions);
            actions.clear();
            } finally {
            rs.close();
            }
        Connector.LOGGER.info("  Query executed");
    }

    static private org.json.JSONObject BuildGeoJSON(Object lat, Object lng) throws org.json.JSONException {
        org.json.JSONObject g = new org.json.JSONObject();
        g.put("lat", lat);
        g.put("lng", lng);
        return g;
    }

    static protected boolean IsGeoValueType(int t) {
        switch (t) {
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.DECIMAL:
            case Types.NUMERIC:
            case Types.REAL:
                return true;
            default:
                return false;
        }
    }

    protected final JSONObject configuration;
    protected final APIClient client;
    protected final Index index;
    protected java.sql.Connection database;
    protected final long batchSize;
    protected final String idField;
    protected final String GEO_LAT_FIELD = "_geoloc_lat";
    protected final String GEO_LNG_FIELD = "_geoloc_lng";
    protected final String source;
    protected final org.json.JSONObject userData;
    protected List<org.json.JSONObject> actions = new ArrayList<org.json.JSONObject>();
}
