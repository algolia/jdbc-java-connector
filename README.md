JDBC Java Connector
===================

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

Configuration File:
-------------------

All command line options can be specified in an external json file:

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
  "index" : "YourIndexName",
}

```


                                                                                
			                                  IIIIIIIIIIIII                                 
			                               IIIIIIIIIIIIIIIIII                               
			                              IIIIIIIIIIIIIIIIIIII                              
			                              IIIIIIIIIIIIIIIIIIIII                             
			                              IIIIIIIIIIIIIIIIIIIII                             
			                              I                 III                             
			                                                                                
			                                                                                
			           III                     IIIIIIIII                                    
			        IIIIIIIII            IIIIIIIIIIIIIIIIIIIII                              
			       IIIIIIIII         IIIIIIIIIIIIIIIIIIIIIIIIIIIII                          
			      IIIIIIII         IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII                        
			      IIIIIII        IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII                      
			      IIIII        IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII                    
			       III       IIIIIIIIIIIIIII               IIIIIIIIIIIIIII                  
			        I       IIIIIIIIIIIII                     IIIIIIIIIIIII                 
			               IIIIIIIIIII                           IIIIIIIIIII                
			             IIIIIIIIIIII                             IIIIIIIIIIII              
			            IIIIIIIIIII                     IIIII       IIIIIIIIIII             
			            IIIIIIIIII                      IIIIIII      IIIIIIIIII             
			           IIIIIIIIII                      IIIIIIIIII     IIIIIIIIII            
			           IIIIIIIII                       IIIIIIIIIII     IIIIIIIII            
			          IIIIIIIII                       IIIIIIIIIIIII     IIIIIIIII           
			          IIIIIIII                        IIIIIIIIIIIII      IIIIIIII           
			         IIIIIIIII                       IIIIIIIIIII         IIIIIIIII          
			         IIIIIIIII                       IIIIIIII            IIIIIIIII          
			         IIIIIIII                       IIIIII                IIIIIIII          
			         IIIIIIII                       III                   IIIIIIII          
			         IIIIIIII                       I                     IIIIIIII          
			         IIIIIIII                                             IIIIIIII          
			         IIIIIIIII                                           IIIIIIIII          
			         IIIIIIIII                                           IIIIIIIII          
			          IIIIIIII                                           IIIIIIII           
			          IIIIIIIII                                         IIIIIIIII           
			           IIIIIIIII                                       IIIIIIIII            
			           IIIIIIIIII                                     IIIIIIIIII            
			            IIIIIIIIII                                   IIIIIIIIII             
			             IIIIIIIIII                                 IIIIIIIIII              
			              IIIIIIIIIII                             IIIIIIIIIII               
			               IIIIIIIIIIII                         IIIIIIIIIIII                
			                IIIIIIIIIIIII                     IIIIIIIIIIIII                 
			                 IIIIIIIIIIIIIIII             IIIIIIIIIIIIIIII                  
			                   IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII                    
			                     IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII                      
			                       IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII                        
			                          IIIIIIIIIIIIIIIIIIIIIIIIIII                           
			                              IIIIIIIIIIIIIIIIIII

