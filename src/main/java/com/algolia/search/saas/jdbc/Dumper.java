package com.algolia.search.saas.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

public class Dumper extends Worker {

    public Dumper(CommandLine cli) throws SQLException, ParseException {
        super(cli);
    }

    @Override
    public void run() throws ParseException, SQLException {
        final String query = cli.getOptionValue("query");
        assert(query != null);

        java.sql.PreparedStatement stmt = database.prepareStatement(query);
        ResultSet req = stmt.executeQuery();
        int columns = req.getMetaData().getColumnCount();
        while (req.next()) {
            for (int i = 0; i < columns; ++i) {
                System.out.println(req.getString(i));
            }
        }
        req.close();
    }

}
