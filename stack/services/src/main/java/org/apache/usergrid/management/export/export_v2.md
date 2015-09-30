#Export V2
Defines how the future iterated export handles what gets exported. 

##Endpoints
Endpoints are the same as export v1.

` POST /management/orgs/<org_name>/apps/<app_name>/collection/<collection_name>/export `

` POST /management/orgs/<org_name>/apps/<app_name>/export `

` POST /management/orgs/<org_name>/export`

` GET /management/orgs/<org_name>/export/<uuid>`


##What payload to the post endpoints take?

	curl -X POST -i -H 'Authorization: Bearer <your admin token goes here>' 'http://localhost:8080/management/orgs/<org_name>/apps/<app_name>/export' -d 
	'{"target":{
		"storage_provider":"s3", 
		"storage_info":{
			"s3_access_id":"<your_aws_access_id>", 
			"s3_key":"<your_aws_secret_key>", 	
			"bucket_location":"<name_of_s3_bucket>" }
	  },
	  "filters":{
	  	"ql": "<usergrid query here>",
	  	"apps":["<test_app_1>","<test_app_2>"],
	  	"collections":[],
	  	"connections":[]
	  }
	  }
	 

###Target
The target json data contains information that will be needed to connect to different storage_providers. Currently only s3 is supported. However, in the future we can change what is acceptable in the storage provider field and then change the storage information that is needed.

###Filters
There are 4 ways that you can filter data out by. 

- Apps
	- In order to export specific apps simply type out the names of the applications you want to export in the apps array. Then those are the only applications that get exported. 
	- If you want to export all the applications then simply delete the filter and it will export all applications. 
- Collections
	- In order to export specific collections simply type out the names of the collections you want to export in the collections array. Those are the only collections that will be exported.  
	- If you want to export all collections then simply delete the collection filter and it will export all the applications.
		- In this case, if you have a query then it will then be applied to all the collections. 
- Query 
	- You can apply a usergrid query to the data you want to export. These queries are applied to each collection that is currently being exported. 
- Connections 
	- In order to export specific connections you can list them same as the other filters. If this is filled in then you are only exporting the connections that contain the names listed in the connections json array.
	- If you want to export all the connections then delete the filter and it will export all the connections. 	
	
##Data Format for export

###Applications
Different applications are exported in seperate files.

For example: Let say I have two applications in my organization. Application sandbox and Application testApplication. When I go export both of them I get the following files in the s3 bucket. 

entities1sandbox.json
connection1sandbox.json

entities1testApplication.json
connections1testApplication.json

All files that have entities will contain the prefix "entities". All files that have connections will contain the prefix "connections". 

Each file that contains entities will at most contain 1000 entities. After that it is split into a seperate file and given a number to indicate what part of the file it is. Each file split will also split the connections file regardless of how many connections there are. For example: If there are 1001 entities with 500 total connections the following will be the file output.

	entities1sandbox.json
	entities2sandbox.json
	connections1sandbox.json
	connections2sandbox.json

In the above files it is possible that there are no connections in either of the two connection files, but they are generated all the same. Likewise if we have 500 entities and 10k connections the following will be the file output. 

	entities1sandbox.json
	connections1sandbox.json

The resulting connections file could be massive but currently this is not addressed. 


###Entities 

####Need to add information on dictionaries and how they functional alongside with entities. Maybe I should include it in entity data. Think about how they import. Might help with how they should be seperated.
	{"uuid":"0c152411-6610-11e5-95fd-8eb055b4b9ec","type":"role","name":"guest","roleName":"guest","inactivity":0,"created":1443465481289,"modified":1443465481289,"title":"Guest","metadata":{"size":381}}
	{"uuid":"0c10de45-6610-11e5-95fd-8eb055b4b9ec","type":"role","name":"default","roleName":"default","inactivity":0,"created":1443465481261,"modified":1443465481261,"title":"Default","metadata":{"size":387}}
	{"uuid":"0c0bfc38-6610-11e5-95fd-8eb055b4b9ec","type":"role","name":"admin","roleName":"admin","inactivity":0,"created":1443465481229,"modified":1443465481229,"title":"Administrator","metadata":{"size":389}}
	{"uuid":"11552288-6610-11e5-95fd-8eb055b4b9ec","type":"user","created":1443465490097,"modified":1443465490097,"username":"billybob996","email":"test996@anuff.com","metadata":{"size":350}}
	{"uuid":"11545f2f-6610-11e5-95fd-8eb055b4b9ec","type":"user","created":1443465490092,"modified":1443465490092,"username":"billybob995","email":"test995@anuff.com","metadata":{"size":350}}
	
	
###Connections
	{"0c4b767b-6610-11e5-95fd-8eb055b4b9ec":{"testconnections":["11552288-6610-11e5-95fd-8eb055b4b9ec","11545f2f-6610-11e5-95fd-8eb055b4b9ec","0c522d4d-6610-11e5-95fd-8eb055b4b9ec","0c4ef8f4-6610-11e5-95fd-8eb055b4b9ec"]}}
	

##Sample Curl calls
Lets assume a base data set. That consists of the following entities and connections
###Sample Entities
<!--{"uuid":"0c152411-6610-11e5-95fd-8eb055b4b9ec","type":"role","name":"guest","roleName":"guest","inactivity":0,"created":1443465481289,"modified":1443465481289,"title":"Guest","metadata":{"size":381}}
		
	{"uuid":"0c10de45-6610-11e5-95fd-8eb055b4b9ec","type":"role","name":"default","roleName":"default","inactivity":0,"created":1443465481261,"modified":1443465481261,"title":"Default","metadata":{"size":387}}

	{"uuid":"0c0bfc38-6610-11e5-95fd-8eb055b4b9ec","type":"role","name":"admin","roleName":"admin","inactivity":0,"created":1443465481229,"modified":1443465481229,"title":"Administrator","metadata":{"size":389}}
	-->
###File 1: entities1Home.json
Application Name:Home
	
	{"uuid":"A","type":"user","created":1,"modified":1,"username":"A","email":"A@test.com","Sex":"Female","metadata":{"size":350}}
	
	{"uuid":"B","type":"user","created":2,"modified":2,"username":"B","email":"B@test.com","Sex":"Male","metadata":{"size":350}}
	
	{"uuid":"C","type":"cat","created":3,"modified":3,"username":"C","email":"C@test.com","Sex":"Male","metadata":{"size":350}}
	
###File 2: entities1Office.json
Application Name:Office

	{"uuid":"D","type":"mangement","created":1,"modified":1,"username":"D","email":"D@test.com","Sex":"Female","metadata":{"size":350}}
	
	{"uuid":"E","type":"worker","created":2,"modified":2,"username":"E","email":"E@test.com","Sex":"Female","metadata":{"size":350}}
	
	{"uuid":"F","type":"worker","created":3,"modified":3,"username":"F","email":"F@test.com","Sex":"Male","metadata":{"size":350}}
	
###File 1: connections1Home
	{"A":{
	"likes":["B"],
	"hates":["C"]}}
	
	{"B":{
	"likes":["C"],
	"hates":["A"]}}
	
	{"C":{
	"hates":["A","B"]}

###File 2: connections1Office
	{"D":{
	"manages":["E","F"]}
	
	{"E":{
	"likes":["F"],
	"hates":["D"]}}
	
	{"F":{
	"indifferent":["D","E"]}
	
###What curl calls would export
	curl -X POST -i -H 'Authorization: Bearer <your admin token goes here>' 'http://localhost:8080/management/orgs/<org_name>/apps/<app_name>/export' -d 
	'{"target":{
		"storage_provider":"s3", 
		"storage_info":{
			"s3_access_id":"<your_aws_access_id>", 
			"s3_key":"<your_aws_secret_key>", 	
			"bucket_location":"<name_of_s3_bucket>" }
	  }
	  }
The above exports all the data in the format listed above. Equivalently you could also have 
	
	curl -X POST -i -H 'Authorization: Bearer <your admin token goes here>' 'http://localhost:8080/management/orgs/<org_name>/export' -d 
	'{"target":{
		"storage_provider":"s3", 
		"storage_info":{
			"s3_access_id":"<your_aws_access_id>", 
			"s3_key":"<your_aws_secret_key>", 	
			"bucket_location":"<name_of_s3_bucket>" }
	  },
	  "filters":{
	  	"apps":[],
	  	"collections":[],
	  	"connections":[]
	  }
	  }
And it would still export all the data in that organization.

---
	
	curl -X POST -i -H 'Authorization: Bearer <your admin token goes here>' 'http://localhost:8080/management/orgs/<org_name>/export' -d 
	'{"target":{
		"storage_provider":"s3", 
		"storage_info":{
			"s3_access_id":"<your_aws_access_id>", 
			"s3_key":"<your_aws_secret_key>", 	
			"bucket_location":"<name_of_s3_bucket>" }
	  },
	  "filters":{
	    "ql":"select * where sex = "Male"",
	  	"apps":["home"],
	  	"collections":[],
	  	"connections":[]
	  }
	  }
Since queries are applied to all collections (unless otherwise filtered) the query filter is applied to each collection. Also we disregard all office information because we're only looking at the home application. We get the following results. 
######Entities
	{"uuid":"B","type":"user","created":2,"modified":2,"username":"B","email":"B@test.com","Sex":"Male","metadata":{"size":350}}
	
	{"uuid":"C","type":"cat","created":3,"modified":3,"username":"C","email":"C@test.com","Sex":"Male","metadata":{"size":350}}
######Connections
	{"B":{
	"likes":["C"],
	"hates":["A"]}}
	
	{"C":{
	"hates":["A","B"]}
---
	
	curl -X POST -i -H 'Authorization: Bearer <your admin token goes here>' 'http://localhost:8080/management/orgs/<org_name>/export' -d 
	'{"target":{
		"storage_provider":"s3", 
		"storage_info":{
			"s3_access_id":"<your_aws_access_id>", 
			"s3_key":"<your_aws_secret_key>", 	
			"bucket_location":"<name_of_s3_bucket>" }
	  },
	  "filters":{
	  	"apps":["home"],
	  	"collections":["cats"],
	  	"ql":"select * where sex = "Male"",
	  	"connections":[]
	  }
	  }
Returns all cats who are male along with the connections that those cats made.
######Entities

	{"uuid":"C","type":"cat","created":3,"modified":3,"username":"C","email":"C@test.com","Sex":"Male","metadata":{"size":350}}
######Connections
	{"C":{
	"hates":["A","B"]}
---

	curl -X POST -i -H 'Authorization: Bearer <your admin token goes here>' 'http://localhost:8080/management/orgs/<org_name>/export' -d 
	'{"target":{
		"storage_provider":"s3", 
		"storage_info":{
			"s3_access_id":"<your_aws_access_id>", 
			"s3_key":"<your_aws_secret_key>", 	
			"bucket_location":"<name_of_s3_bucket>" }
	  },
	  "filters":{
	  	"apps":[],
	  	"collections":[],
	  	"connections":["hates","manages"]
	  }
	  }
Will export all entities and only the connections that are named "hates" and "manages".

#####Entities
######File 1: entities1Home.json
	{"uuid":"A","type":"user","created":1,"modified":1,"username":"A","email":"A@test.com","Sex":"Female","metadata":{"size":350}}
	
	{"uuid":"B","type":"user","created":2,"modified":2,"username":"B","email":"B@test.com","Sex":"Male","metadata":{"size":350}}
	
	{"uuid":"C","type":"cat","created":3,"modified":3,"username":"C","email":"C@test.com","Sex":"Male","metadata":{"size":350}}

######File 2: entities1Office.json
		{"uuid":"D","type":"mangement","created":1,"modified":1,"username":"D","email":"D@test.com","Sex":"Female","metadata":{"size":350}}
	
	{"uuid":"E","type":"worker","created":2,"modified":2,"username":"E","email":"E@test.com","Sex":"Female","metadata":{"size":350}}
	
	{"uuid":"F","type":"worker","created":3,"modified":3,"username":"F","email":"F@test.com","Sex":"Male","metadata":{"size":350}}
#####Connections
######File 1: connections1Home.json
	{"A":{
	"hates":["C"]}}
	
	{"B":{
	"hates":["A"]}}
	
	{"C":{
	"hates":["A","B"]}
######File 2: connections1Office.json
	{"D":{
	"manages":["E","F"]}
	
	{"E":{
	"hates":["D"]}}
---
	
	curl -X POST -i -H 'Authorization: Bearer <your admin token goes here>' 'http://localhost:8080/management/orgs/<org_name>/export' -d 
	'{"target":{
		"storage_provider":"s3", 
		"storage_info":{
			"s3_access_id":"<your_aws_access_id>", 
			"s3_key":"<your_aws_secret_key>", 	
			"bucket_location":"<name_of_s3_bucket>" }
	  },
	  "filters":{
	    "ql":"select * where email = C@test.com"
	  	"apps":["home"],
	  	"collections":["cats","users"],
	  	"connections":["hates"]
	  }
	  }
	  
#####Entities
######File 1: entities1Home.json
	{"uuid":"C","type":"cat","created":3,"modified":3,"username":"C","email":"C@test.com","Sex":"Male","metadata":{"size":350}}
######File 2: connections1Home.json
	{"C":{
	"hates":["A","B"]}

With the above example you need to be careful if you are importing because you wouldn't have the entity data for what C is connected to. Export doesn't currently safeguard against cases such as these as it follows what you asked for. 

Need to make sure we only search between a single org. I guess with the new endpoint since its only one we don't need to worry about that because it will only ever be in an org scope. 
	
	
	
##Ok thats great but how does it work under the hood?
After a curl call is sent we hit the rest tier which then in turn validates the information for a fail fast integration. Once we verify that the information is correct and has the permissions to upload to s3 (I guess we could error check filters too) we go to the service tier and schedule the job. Once the job is ready to be run we then head back and export information in the following order

- Organization
	- Application
		- Collections
			- Entity
				- Dictionaries
				- Connections  
				
Starting from the top in psudeo code

	In the context of the current organization
		Create list of files that will contain all the exported entity files.
		Create list of files that will contain all of the exported connection files. 
		
		Retrieve only the applications that have been specified in the filter 
		Otherwise retrieve a list of all the applications in the organization
		For each application
			Create a new file that the application entity data will be written to.
			Set file to delete after jvm exits.
			Add file to the list of entity files.
			
			Create a new file that the application connection data will be written to. 
			Set file to delete after jvm exits.
			Add file to list of connection files.
			
			Create a counter that will allow you to track how many entities are being written to the entity data.
			Create a counter that will notifiy you how many different file parts there are for this export. 
			Retrieve the filtered list of collections that need to be exported.
			If the filtered list if empty or nonexistant then return all collections
			
			For each collection
				Check the filter to see if you have a query filter.
				If you do have a query filter then create a new query object set it with the query in the query filter.
				If you don't have a query filter or it is empty then set a empty Query object.
				
				Pull entity data from the collection 250 entities at a time.
				For each entity
					Write entity to entity file. 
					Write entity dictionaries to entity file (Should probably be included in entity.)
					Write entity connection to connection file. 
					Increment entity counter.
					If counter % 1000 == 0
						Increment fileParts counter.
						Create new File to store additional entities in.
						Switch to write all data to the new file.
						Set file to delete after jvm exits.
						
						Create new File to store additional connections in.
						Switch to write all connection data to the new file.
						Set file to delete after jvm exists. 
		
		Create a HashMap
		Put the list of entity files into the hashmap
		Put the list of connection files into the hashmap.
		Return the hashmap.

The actual mechanism through which it writes to the file is through a Json Generator using minimal pretty printing and the json lines format. The only case where this might currently mess up is using dictionaries as it would mean that there are two json entities per line instead of one. 

###How specifically does it pull data out of Usergrid?
In code we have the following lines 

	Results entities = em.searchCollection( em.getApplicationRef(), collectionName, query );
	PagingResultsIterator itr = new PagingResultsIterator( entities );

These two lines take an application and search for a specific collection within the application and apply a query to each collection. 

If the query is empty/null (need to apply the fix where queries can be null, currently they are always initialized) or if the query is "select *" then we go and search through the graph database for each entity. If the query is more sophisticated then we query elasticsearch for all relevant entity information.  (Maybe a useful feature would be to perform a reindexing operation on each collection we wish to export. ) This would work exactly the same way a generic query performed at the rest tier would access the data. 


##Improvements
Need to add s3 permission failures. I.E fail fast without having to iterator over everything for every single call.

In the future if your s3 call fails or can't work don't just delete the ephmeral file. Make sure you either try to reupload it or store it somewhere the person can access the file. Please log all exceptions don't let any just get thrown up or more importantly don't let any get swallowed. 

Make the names of fields configurable since often times we will change them during this process. 

Dictionaries should be included in entity metadata or as a field

When we hit 1000 entities we shouldn't create the extra files if thats all that is contained. 


	
	
