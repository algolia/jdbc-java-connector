JDBC Java Connector
===================


Setup:
------

```shell
sh$> mvn install
sh$> mvn package
sh$> mvn exec:java -Dexec.mainClass="com.algolia.search.saas.jdbc.Connector" -Dexec.args="--dump --query 'SELECT '*' FROM projects' --target YourApplicationId:YourApiKey:YourIndex --source jdbc:mysql://localhost/YourDB --username YourUser --password YourPassword"
```

Usage:
------

```
usage: jdbc-java-connector
 -c,--configuration <path/to/config.json>   Configuration file.
 -d,--dump                                  Perform a dump
 -h,--help                                  Print this help.
    --password <arg>                        DB password
 -q,--query <SELECT * FROM table>           SQL query used to fetched all rows
 -r,--refresh <rateInMS>                    The refresh interval, in seconds
 -s,--source <jdbc:DRIVER://HOST/DB>        JDBC connection string
 -t,--target <APPID:APPKEY:Index>           Algolia credentials and index
 -u,--update                                Perform an update
    --updatedAtField <field>                Field name used to find updated rows.
    --username <arg>                        DB username
```

Update mode:
------------

In update mode you can use the last value of the tracked attribute like this:

	SELECT * FROM table WHERE _$ < UPDATED_AT

Configuration File:
-------------------

```json
{
	"attributes":["attr1", "attr2", ...],
	"track": "updated_at"
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

