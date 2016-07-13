JDBC Java Connector
===================

**WARNING** : This connector is deprecated, please use the [Java API CLient] (https://github.com/algolia/algoliasearch-client-java-2).

This connector synchronizes your existing SQL database with Algolia's indices without requiring you to write a single line of code in your application or website. MySQL, PostgreSQL and Sqlite3 are currently supported.

First, it will list all your rows with the `selectQuery` and perform the initial indexing. Then, every `refreshRate` seconds, it will look for updated rows using the `updateQuery` and send the updates to Algolia. Deletions will be detected every `deleteRate` minutes performing a full scan (+diff) of both your database and index.

Setup
-----
```shell
curl -fsSL https://raw.github.com/algolia/jdbc-java-connector/master/dist/jdbc-connector.sh > jdbc-connector.sh
```

Usage
-----
```shell
usage: jdbc-connector.sh [option]... [path/to/config.json]
    --apiKey <YourApiKey>                                      Algolia APPI_KEY
    --applicationId <YourApplicationID>                        Algolia APPLICATION_ID
    --batchSize <arg>                                          Size of the batch. (default: 1000)
 -d,--dump                                                     Perform the initial import and exit (no incremental updates)
    --syncRate <rateInMinutes>                                 The refresh interval to check addition/deletion, in minutes (default: 5)
 -h,--help                                                     Print this help.
    --index <YourIndex>                                        Destination index
    --log <path/to/logging.properties>                         Path to logging configuration file.
    --password <arg>                                           DB password
    --primaryField <id>                                        Field name used to identify rows (default: id)
 -q,--selectQuery <SELECT * FROM table>                        SQL query used to fetched all rows
 -r,--refreshRate <rateInSeconds>                              The refresh interval, in seconds (default: 10)
 -s,--source <jdbc:DRIVER://HOST/DB>                           JDBC connection string
 -u,--updateQuery <SELECT * FROM table WHERE updatedAt > _$>   SQL query used to fetched updated rows. Use _$ as placeholder
    --updatedAtField <field>                                   Field name used to find updated rows (default: updated_at)
    --username <arg>                                           DB username
```

Sample configuration:
-------------------

All command line options can be specified in an external JSON file:

```json
{
  "selectQuery" : "SELECT * FROM projects WHERE deleted = 0",
  "updateQuery" : "SELECT * FROM projects WHERE deleted = 0 AND updated_at > _$",
  "primaryField" : "id",
  "updatedAtField": "updated_at",
  "source" : "jdbc:mysql://localhost/github",
  "username" : "mysqluser",
  "password" : "mysqlpassword",
  "applicationId" : "YourAlgoliaApplicationID",
  "apiKey" : "YourAlgoliaApiKey",
  "index" : "YourIndexName"
}
```

### Geo-Search

You can make the connector generate the `"_geoloc": { "lat": XX, "lng": YY }` nested attribute by naming your latitude & longitude columns `_geoloc_lat` and `_geoloc_lng`.
