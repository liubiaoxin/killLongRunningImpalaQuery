from datetime import datetime, timedelta
import sys
import cm_client
from cm_client.rest import ApiException
from pprint import pprint

# Configure HTTP basic authorization: basic
cm_client.configuration.username = 'admin'
cm_client.configuration.password = 'cm123$#@!'

# configuration = cm_client.Configuration()
# configuration.username = 'admin'
# configuration.password = 'cm123$#@!'

# Create an instance of the API class
#http://10.50.8.112:7180/api/v1/clusters/Cluster%201/services/impala
api_host = 'http://10.50.8.112'
port = '7180'
api_version = 'v17'

## Cluster Name
cluster_name = "Cluster 1"

## *****************************************

#fmt = '%Y-%m-%dT%H:%M:%S.%Z'
fmt = '%Y-%m-%dT%H:%M:%S.%fZ'

def printUsageMessage():
    print("Usage: killLongRunningImpalaQueries.py <queryRunningSeconds>  [KILL]")
    print("Example that lists queries that have run more than 10 minutes:")
    print("./killLongRunningImpalaQueries.py 600")
    print("Example that kills queries that have run more than 10 minutes:")
    print("./killLongRunningImpalaQueries.py 600 KILL")

## ** Validate command line args *************

if len(sys.argv) == 1 or len(sys.argv) > 3:
    printUsageMessage()
    quit(1)

queryRunningSeconds = sys.argv[1]

if not queryRunningSeconds.isdigit():
    print("Error: the first argument must be a digit (number of seconds)")
    printUsageMessage()
    quit(1)

kill = False

if len(sys.argv) == 3:
    if sys.argv[2] != 'KILL':
        print("the only valid second argument is \"KILL\"")
        printUsageMessage()
        quit(1)
    else:
        kill = True

# Lists all known clusters.
# Construct base URL for API
# http://cmhost:7180/api/v17
api_url = api_host + ':' + port + '/api/' + api_version
## Connect to CM
print("\nConnecting to Cloudera Manager at " + api_host + ":" + port)

## Get the IMPALA service
api_client = cm_client.ApiClient(api_url)
services_api_instance = cm_client.ServicesResourceApi(api_client)
services = services_api_instance.read_services(cluster_name, view='FULL')
for service in services.items:
    #print(service.name, "-", service.type)
    if service.type == "IMPALA":
         impala_service = service
         print("Located Impala Service: " + service.name)
         break
if impala_service is None:
     print("Error: Could not locate Impala Service")
     quit(1)

## A window of one day assumes queries have not been running more than 24 hours
now = datetime.utcnow()
start = now - timedelta(days=1)

print("Looking for Impala queries running more than " + str(queryRunningSeconds) + " seconds")

#filterStr = 'queryDuration > ' + queryRunningSeconds + 's'
api_instance = cm_client.ImpalaQueriesResourceApi(cm_client.ApiClient(api_url))

service_name = 'impala'
filter = 'query_duration > ' + queryRunningSeconds + 's'
#_from = ''
limit = 1000
offset = 0
to = 'now'
try:
    # Returns a list of queries that satisfy the filter.
    impala_query_response = api_instance.get_impala_queries(cluster_name, service_name,filter=filter,_from=start,limit=limit, offset=offset, to=to)
    #pprint(api_response)
    queries = impala_query_response.queries
    longRunningQueryCount = 0

    for i in range (0, len(queries)):
        query = queries[i]
        #pprint(query.query_state)
        #pprint(query)

        if query.query_state != 'FINISHED' and query.query_state != 'EXCEPTION':

            longRunningQueryCount = longRunningQueryCount + 1

            if longRunningQueryCount == 1:
                print('-- long running queries -------------')

            print("queryState : " + query.query_state)
            print("queryId: " + query.query_id)
            print("user: " + query.user)
            print("startTime: " +query.start_time)



            # query_duration = now - query.start_time
            #print("query running time (seconds): " + (query.durationMillis/1000))
            print("SQL: " + query.statement)

            if kill:
                print("Attempting to kill query,query_id="+query.query_id+"...")
                api_instance.cancel_impala_query(cluster_name=cluster_name,query_id=query.query_id,service_name=service_name)
            print('-------------------------------------')
    if longRunningQueryCount == 0:
        print("No queries found")

    print("done")

except ApiException as e:
    print("Exception when calling ImpalaQueriesResourceApi->get_impala_queries: %s\n" % e)

## Get the Cluster
# api_response = cluster_api_instance.read_clusters(view='SUMMARY')
# for cluster in api_response.items:
#     print (cluster.name, '-', cluster.full_version)