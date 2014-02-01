package com.algolia.search.saas.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.cli.ParseException;
import org.json.JSONException;
import org.json.simple.JSONObject;

import com.algolia.search.saas.AlgoliaException;

public class Dumper extends Worker {

    public Dumper(JSONObject configuration) throws SQLException, ParseException, JSONException {
        super(configuration);

        this.query = (String) configuration.get(Connector.CONF_QUERY);
        assert (query != null);
        this.stmt = database.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        this.stmt.setFetchSize(Integer.MIN_VALUE);
        if (this.stmt instanceof com.mysql.jdbc.Statement) {
            ((com.mysql.jdbc.Statement) this.stmt).enableStreamingResults();
        }
    }

    @Override
    public void run() throws SQLException, AlgoliaException, JSONException {   
        iterateOnQuery(stmt);
    }

    private final java.sql.PreparedStatement stmt;
    private final String query;
}
