JDBC Java Connector
===================


Setup:
------

	sh$> mvn install
	sh$> mvn package
	sh$> mvn exec:java -Dexec.mainClass="com.algolia.search.saas.jdbc.Connector"

Usage:
------

	- target: target to algolia "APPID:APPKEY:Index"
	- output: path of file used to keep the last modification
	- config: path of configuration file
	- host: url jdbc
	- mode: dump or update
	- username: username of the db
	- password: password of the db
	- query: sql query to fetch data
	- attribute: attribute used to update
	- time: time between to refresh

Update mode:
------------

In update mode you can use the last value of the tracked attribute like this:

		SELECT * FROM table WHERE _$ < UPDATED_AT

Configuration File:
-------------------

It's a json.

	{
		"attributes":["attr1", "attr2", ...],
		"track": "updated_at"
	}


                                                                                
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

