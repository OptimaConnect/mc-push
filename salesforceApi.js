const axios 				= require('axios');
const { ServiceBusClient } 	= require('@azure/service-bus');
require('dotenv').config();

const environment = {
  authUrl: 							process.env.authUrl,
  clientId: 						process.env.clientId,
  clientSecret: 					process.env.clientSecret,
  restUrl: 							process.env.restUrl,
  queryFolder: 						process.env.queryFolder,
  azureServiceBusConnectionString:	process.env.azureServiceBusConnectionString,
  azureQueueName: 					process.env.azureQueueName,
  automationEndpoint: 				process.env.automationEndpoint,
};
console.dir(environment);

const automationUrl 		= environment.restUrl + "automation/v1/automations/";
const queryUrl 				= environment.restUrl + "automation/v1/queries/";

exports.getOauth2Token = getOauth2Token

async function getOauth2Token(){
    try{
	    const oauthResponse = await axios({
	    	method: 'post',
	    	url: environment.authUrl,
	    	data:{
	    		"grant_type": "client_credentials",
	    		"client_id": environment.clientId,
	    		"client_secret": environment.clientSecret
	    	}
	    })
	    console.dir('Bearer '.concat(oauthResponse.data.access_token));
	    return 'Bearer '.concat(oauthResponse.data.access_token);
    
	} catch(e) {
		console.dir("Error getting Oauth Token");
		throw new Error(e);
	}
}

exports.createSQLQuery = async function(targetKey, query, updateType, target, name, description){
    const tokenResponse = await getOauth2Token();
    
	//console.dir("Oauth Token");
	//console.dir(tokenResponse);
	/**
	* targetUpdateTypeId
	* 0 = Overwrite
	* 1 = Add/Update (requires PK)
	* 2 = Append
	*/
	var queryDefinitionPayload = {
	    "name": name,
	    "description": description,
	    "queryText": query,
	    "targetName": target,
	    "targetKey": targetKey,
	    "targetUpdateTypeId": updateType,
        "categoryId": environment.queryFolder}
        
    const response = await axios({
		method: 'post',
		url: queryUrl,
		headers: {'Authorization': tokenResponse},
		data: queryDefinitionPayload
	})
	console.dir(response.data);
	return (response.data.queryDefinitionId);
}

exports.runSQLQuery = async function(executeThisQueryId, queryName) {
	const sbClient = ServiceBusClient.createFromConnectionString(environment.azureServiceBusConnectionString);
	const queueClient = sbClient.createQueueClient(environment.azureQueueName);
	const sender = queueClient.createSender();
	try {
		const queryToRunMessage = {
			body: {
				queryId: executeThisQueryId,
				queryName: queryName
			},
			contentType: "application/json",
		}

		await sender.send(queryToRunMessage);

		console.dir(`Query queued for execution: ${JSON.stringify(queryToRunMessage.body)}`);
		await queueClient.close();

	} finally {
		await sbClient.close();
	}
}