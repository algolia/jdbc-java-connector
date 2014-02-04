package com.algolia.search.saas.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.commons.cli.ParseException;
import org.json.JSONException;
import org.json.simple.JSONObject;

import com.algolia.search.saas.AlgoliaException;

public class Dumper extends Worker {

    public Dumper(JSONObject configuration) throws SQLException, ParseException, JSONException {
        super(configuration);
    }
    
    @Override
    protected void onRow(ResultSetMetaData rsmd, ResultSet rs, String objectID) throws SQLException {
    }

    @Override
    protected void fillUserData(org.json.JSONObject userData) throws JSONException {
    }
    
    @Override
    public void run() throws SQLException, AlgoliaException, JSONException {
    	Connector.LOGGER.info("Start initial import job");
        iterateOnQuery((String) configuration.get(Connector.CONF_SELECT_QUERY));
        Connector.LOGGER.info("Initial import done");
    }
}
