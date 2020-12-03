const axios 			= require('axios');
require('dotenv').config();
const local       		= false;
if ( !local ) {
	var environment = {
	  authUrl: 							process.env.authUrl,
	  clientId: 						process.env.clientId,
	  clientSecret: 					process.env.clientSecret,
	  restUrl: 							process.env.restUrl,
	};
	console.dir(environment);
}
const automationUrl 		= environment.restUrl + "/automation/v1/automations/";
const queryUrl 				= environment.restUrl + "/automation/v1/queries/";

exports.getOauth2Token = async function(){
    try{
	    const oauthResponse = await axios({
	    	method: 'post',
	    	url: marketingCloud.authUrl,
	    	data:{
	    		"grant_type": "client_credentials",
	    		"client_id": marketingCloud.clientId,
	    		"client_secret": marketingCloud.clientSecret
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
    await getOauth2Token();
    
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
        "categoryId": marketingCloud.queryFolder}
        
    try {        
	    const response = await axios({
	    	method: 'post',
	    	url: automationUrl,
	    	headers: {'Authorization': tokenResponse},
	    	data: queryDefinitionPayload
	    })
	    console.dir(response.data);
	    return (response.data.queryDefinitionId);
	} catch(e) {
	    console.dir(e);
	    throw new Error(e);
    }
}
