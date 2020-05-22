'use strict';

// Module Dependencies
const axios 			= require('axios');
var express     		= require('express');
var bodyParser  		= require('body-parser');
var errorhandler 		= require('errorhandler');
var http        		= require('http');
var path        		= require('path');
var request     		= require('request');
var routes      		= require('./routes');
var activity    		= require('./routes/activity');
var urlencodedparser 	= bodyParser.urlencoded({extended:false});
var app 				= express();
var local       		= false;


// access Heroku variables
if ( !local ) {
	var marketingCloud = {
	  authUrl: 						process.env.authUrl,
	  clientId: 					process.env.clientId,
	  clientSecret: 				process.env.clientSecret,
	  restUrl: 						process.env.restUrl,
	  appUrl: 						process.env.baseUrl,
	  controlGroupsDataExtension: 	process.env.controlGroupsDataExtension,
	  updateContactsDataExtension: 	process.env.updateContactsDataExtension,
	  promotionsDataExtension: 		process.env.promotionsDataExtension,
	  insertDataExtension: 			process.env.insertDataExtension,
	  incrementDataExtension: 		process.env.incrementDataExtension,
	  seedDataExtension: 			process.env.seedlist,
	  targetKey: 					process.env.targetKey,
	  targetId: 					process.env.targetId,
	  automationEndpoint: 			process.env.automationEndpoint,
	  promotionTableName: 			process.env.promotionTableName,
	  communicationTableName: 		process.env.communicationTableName,
	  assignmentTableName: 			process.env.assignmentTableName,
	  messageTableName: 			process.env.messageTableName,
	  offerTableName: 				process.env.offerTableName,
	  mobilePushMainTable: 			process.env.mobilePushMainTable,
	  partyCardDetailsTable:  		process.env.partyCardDetailsTable,
	  promotionDescriptionTable: 	process.env.promotionDescriptionTable,
	  seedListTable: 				process.env.seedListTable
	};
	console.dir(marketingCloud);
}

// url constants
const controlGroupsUrl 	= marketingCloud.restUrl + "data/v1/customobjectdata/key/" 	+ marketingCloud.controlGroupsDataExtension 	+ "/rowset";
const updateContactsUrl = marketingCloud.restUrl + "data/v1/customobjectdata/key/" 	+ marketingCloud.updateContactsDataExtension 	+ "/rowset";
const promotionsUrl 	= marketingCloud.restUrl + "data/v1/customobjectdata/key/" 	+ marketingCloud.promotionsDataExtension 		+ "/rowset";
const insertUrl 		= marketingCloud.restUrl + "hub/v1/dataevents/key:" 	+ marketingCloud.insertDataExtension 			+ "/rowset";
const incrementsUrl 	= marketingCloud.restUrl + "data/v1/customobjectdata/key/" 	+ marketingCloud.incrementDataExtension 		+ "/rowset";
const updateIncrementUrl = marketingCloud.restUrl + "hub/v1/dataevents/key:" 	+ marketingCloud.incrementDataExtension 			+ "/rowset";
const automationUrl = marketingCloud.automationEndpoint;

// Configure Express master
app.set('port', process.env.PORT || 3000);
app.use(bodyParser.raw({type: 'application/jwt'}));
app.use(express.static(path.join(__dirname, 'public')));
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

// Express in Development Mode
if ('development' == app.get('env')) {
	app.use(errorhandler());
}

const getOauth2Token = () => new Promise((resolve, reject) => {
	axios({
		method: 'post',
		url: marketingCloud.authUrl,
		data:{
			"grant_type": "client_credentials",
			"client_id": marketingCloud.clientId,
			"client_secret": marketingCloud.clientSecret
		}
	})
	.then(function (oauthResponse) {
		console.dir('Bearer '.concat(oauthResponse.data.access_token));
		return resolve('Bearer '.concat(oauthResponse.data.access_token));
	})
	.catch(function (error) {
		console.dir("Error getting Oauth Token");
		return reject(error);
	});
});

async function definePayloadAttributes(payload, seed) {

	var t = 0;
	var promotionKey;
	var updateContactDE;
	var controlGroupDE;
	var messageKeySaved;
	var automationName;
	var pushType;
	var automationRunDate;
	var automationRunTime;
	var automationReoccuring;
	
	try {
		for ( t = 0; t < payload.length; t++ ) {

			if ( payload[t].key == "message_key_hidden") {
				messageKeySaved = payload[t].value;
			} else if ( payload[t].key == "control_group") {
				controlGroupDE = payload[t].value;
			} else if ( payload[t].key == "update_contacts") {
				updateContactDE = payload[t].value;
			} else if ( payload[t].key == "widget_name") {
				automationName = payload[t].value;
			} else if ( payload[t].key == "pushType") {
				pushType = payload[t].value;
			} else if ( payload[t].key == "offer_promotion") {
				promotionKey = payload[t].value;
			} else if ( payload[t].key == "automation_run_time" ) {
				automationRunTime = payload[t].value;
			} else if ( payload[t].key == "automation_run_date" ) {
				automationRunDate = payload[t].value;
			} else if ( payload[t].key == "automation_reoccuring" ) {
				automationReoccuring = payload[t].value;
			}
		}

		var attributes = {
			key: messageKeySaved, 
			control_group: controlGroupDE, 
			update_contact: updateContactDE, 
			query_name: automationName,
			push_type: pushType,
			promotion_key: promotionKey,
			query_date: automationRunDate + " " + automationRunTime,
			query_reoccuring: automationReoccuring
		};

		console.dir("The attributes return is");
		console.dir(attributes);

		return attributes;
	} catch(e) {
		return e;
	}

};
const sendQuery = (query, target, name, description) => new Promise((resolve, reject) => {

	getOauth2Token().then((tokenResponse) => {

		console.dir("Oauth Token");
		console.dir(tokenResponse);

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
		    "targetKey": marketingCloud.targetKey,
		    "targetId": marketingCloud.targetId,
		    "targetUpdateTypeId": 0,
		    "categoryId": 21650
		}

	   	axios({
			method: 'post',
			url: automationUrl,
			headers: {'Authorization': tokenResponse},
			data: queryDefinitionPayload
		})
		.then(function (response) {
			console.dir(response.data);
			return resolve(response.data.items[0].queryDefinitionId);
		})
		.catch(function (error) {
			console.dir(error);
			return reject(error);
		});

	})

});


async function addQueryActivity(payload) {

	console.dir("Payload for Query");
	console.dir(payload);
	var returnIds = [];

	try {
		const payloadAttributes = await definePayloadAttributes(payload);
		console.dir("The Payload Attributes");
		console.dir(payloadAttributes);

		var communicationQuery = `
				SELECT \n 
				bucket.PARTY_ID, \n 
				cpa.communication_cell_id AS COMMUNICATION_CELL_ID, \n 
				GETDATE() as CONTACT_DATE \n 
				FROM \n 
				[` + payloadAttributes.update_contact + `] as bucket \n 
				LEFT JOIN [` + marketingCloud.promotionTableName + `] as cpa \n 
				ON cpa.promotion_key = [` + payloadAttributes.promotion_key + `] \n 
				WHEN cpa.promotionType = 'online' OR cpa.promotionType = 'online_instore' OR cpa.promotionType = 'instore'\n`;
		console.dir(communicationQuery);

		if ( payloadAttributes.push_type = "offer" ) {

			var assignmentQuery = `
				SELECT \n 
				bucket.PARTY_ID, \n 
				cpa.MC_ID_1 AS MC_UNIQUE_PROMOTION_ID, \n 
				GETDATE() as ASSIGNMENT_DATETIME \n 
				FROM \n 
				[` + payloadAttributes.update_contact + `] as bucket \n 
				LEFT JOIN [` + marketingCloud.promotionTableName + `] as cpa \n 
				ON cpa.promotion_key = bucket.PROMOTION_KEY \n 
				WHEN cpa.promotionType = 'online' OR cpa.promotionType = 'online_instore' \n 
				UNION \n 
				SELECT \n 
				bucket.PARTY_ID, \n 
				cpa.MC_ID_6 AS MC_UNIQUE_PROMOTION_ID, \n 
				GETDATE() as ASSIGNMENT_DATETIME \n 
				FROM \n 
				[` + payloadAttributes.update_contact + `] as bucket \n 
				LEFT JOIN [` + marketingCloud.promotionTableName + `] as cpa \n 
				ON cpa.promotion_key = bucket.PROMOTION_KEY \n 
				WHEN cpa.promotionType = 'instore' OR cpa.promotionType = 'online_instore' \n`;
				console.dir(assignmentQuery);

			var memberOfferQuery = `
				SELECT \n 
				'Matalan' AS SCHEME_ID, \n 
				PCD.APP_CARD_NUMBER AS LOYALTY_CARD_NUMBER, \n 
				MPT.offer_id AS OFFER_ID, \n 
				PT.instore_code_1 AS VOUCHER_IN_STORE_CODE, \n 
				CASE \n 
				WHERE PT.promotionTypeOnline = 'unique' THEN PT.unique_code_1 \n 
				WHERE PT.promotionTypeOnline = 'global' THEN PT.global_code_1 \n 
				END  \n 
				AS VOUCHER_ON_LINE_CODE, \n 
				PD.[VALID_FROM_DATETIME] AS [START_DATE_TIME], \n 
				PD.VISIBLETO AS [END_DATE_TIME], \n 
				PD.NUMBER_OF_REDEMPTIONS_ALLOWED AS NO_REDEMPTIONS_ALLOWED, \n 
				PD.VISIBLEFROM AS [VISIBLE_FROM_DATE_TIME], \n 
				'A' AS STATUS \n 
				FROM \n 
				[" + payloadAttributes.update_contact + "] as UpdateContactDE \n 
				LEFT JOIN [` + marketingCloud.mobilePushMainTable + `] AS MPT \n 
				ON MPT.push_key = [` + payloadAttributes.key + `] \n 
				LEFT JOIN [` + marketingCloud.promotionTableName + `] as PT \n 
				ON PT.promotion_key = MPT.offer_promotion \n 
				LEFT JOIN [` + marketingCloud.partyCardDetailsTable + `] AS PCD \n 
				ON PCD.PARTY_ID = UpdateContactDE.PARTY_ID \n 
				LEFT JOIN [` + marketingCloud.promotionDescriptionTable + `] AS PD \n 
				ON PD.MC_UNIQUE_PROMOTION_ID = PT.MC_ID_6 \n`;
				console.dir(memberOfferQuery);
		
		} else {

			var messageQuery = `
				SELECT \n 
				'Matalan' AS SCHEME_ID, \n 
				_CustomObjectKey AS MOBILE_MESSAGE_ID, \n 
				PCD.APP_CARD_NUMBER AS LOYALTY_CARD_NUMBER, \n 
				MPT.message_content AS MESSAGE_CONTENT, \n 
				CONCAT(MPT.message_target_send_date, ' ', MPT.message_target_send_time) AS TARGET_SEND_DATE_TIME, \n 
				'A' AS STATUS, \n 
				MPT.message_short_content AS SHORT_MESSAGE_CONTENT \n 
				FROM [` + payloadAttributes.update_contact + `] as UpdateContactDE \n 
				LEFT JOIN [` + marketingCloud.partyCardDetailsTable + `] AS PCD \n 
				ON PCD.PARTY_ID = UpdateContactDE.PARTY_ID \n 
				LEFT JOIN [` + marketingCloud.mobilePushMainTable + `] as MPT \n 
				ON MPT.push_key = [` + payloadAttributes.key + `] \n`;
				console.dir(messageQuery);
		
		}

		const communicationQueryId = await sendQuery(communicationQuery, marketingCloud.communicationTableName, "Communication Cell - " + payloadAttributes.query_name, "Communication Cell Assignment in IF028 for " + payloadAttributes.query_name);
		
		await logQuery(communicationQueryId, payloadAttributes.automationReoccuring, payloadAttributes.query_date);
		
		returnIds["communication_query_id"] = communicationQueryId;
		
		if ( payloadAttributes.push_type = "offer" ) {
			
			const assignmentQueryId = await sendQuery(assignmentQuery, marketingCloud.assignmentTableName, "Assignment - " + payloadAttributes.query_name, "Assignment in PROMOTION_ASSIGNMENT in IF024 for " + payloadAttributes.query_name);
			await logQuery(assignmentQueryId, payloadAttributes.automationReoccuring, payloadAttributes.query_date);
			returnIds["assignment_query_id"] = assignmentQueryId;
			const memberOfferQueryId = await sendQuery(memberOfferQuery, marketingCloud.offerTableName, "IF008 Offer - " + payloadAttributes.query_name, "Member Offer Assignment in IF008 for " + payloadAttributes.query_name);
			await logQuery(memberOfferQueryId, payloadAttributes.automationReoccuring, payloadAttributes.query_date);
			returnIds["member_offer_query_id"] = assignmentQueryId;
		
		} else {
			
			const messageQueryId = await sendQuery(messageQuery, marketingCloud.messageTableName, "IF008 Message - " + payloadAttributes.query_name, "Message Assignment in IF008 for " + payloadAttributes.query_name);
			await logQuery(messageQueryId, payloadAttributes.automationReoccuring, payloadAttributes.query_date);
			returnIds["member_message_query_id"] = assignmentQueryId;

		}
		

		return returnIds;

	} catch(e) {

		return e;

	}
};

const logQuery = (queryId, type, scheduledDate) => new Promise((resolve, reject) => {

	console.dir("Payload:");
	console.dir(pushPayload);
	console.dir("Current Key:");
	console.dir(incrementData);

	var queryPayload = [{
        "keys": {
            "query_id": parseInt(queryId)
        },
        "values": {
        	"reoccurring": type,
        	"scheduled_run_date_time": scheduledDate
        }
	}];
	
	console.dir(queryPayload);

	getOauth2Token().then((tokenResponse) => {
	   	axios({
			method: 'post',
			url: queryUrl,
			headers: {'Authorization': tokenResponse},
			data: queryPayload
		})
		.then(function (response) {
			console.dir(response.data);
			return resolve(response.data);
		})
		.catch(function (error) {
			console.dir(error);
			return reject(error);
		});
	})	
	
});

const getIncrements = () => new Promise((resolve, reject) => {
	getOauth2Token().then((tokenResponse) => {

		axios.get(incrementsUrl, { 
			headers: { 
				Authorization: tokenResponse
			}
		})
		.then(response => {
			// If request is good...
			console.dir(response.data.items[0].values);
			return resolve(response.data.items[0].values);
		})
		.catch((error) => {
		    console.dir("Error getting increments");
		    return reject(error);
		});
	})
});

const updateIncrements = (currentIncrement) => new Promise((resolve, reject) => {

	console.dir("Current Increment");
	console.dir(currentIncrement.increment);

	var newIncrement = parseInt(currentIncrement.increment) + 1;

	var updatedIncrementObject = {};
	updatedIncrementObject.increment = parseInt(newIncrement);

	console.dir(updatedIncrementObject);

	var insertPayload = [{
        "keys": {
            "id": 1
        },
        "values": updatedIncrementObject
	}];
		
	console.dir(insertPayload);

	getOauth2Token().then((tokenResponse) => {
	   	axios({
			method: 'post',
			url: updateIncrementUrl,
			headers: {'Authorization': tokenResponse},
			data: insertPayload
		})
		.then(function (response) {
			console.dir(response.data);
			return resolve(response.data);
		})
		.catch(function (error) {
			console.dir(error);
			return reject(error);
		});
	})	
	
});

const saveToDataExtension = (pushPayload, incrementData) => new Promise((resolve, reject) => {

	console.dir("Payload:");
	console.dir(pushPayload);
	console.dir("Current Key:");
	console.dir(incrementData);


	var insertPayload = [{
        "keys": {
            "push_key": parseInt(incrementData.increment)
        },
        "values": pushPayload
	}];
	
	console.dir(insertPayload);

	getOauth2Token().then((tokenResponse) => {
	   	axios({
			method: 'post',
			url: insertUrl,
			headers: {'Authorization': tokenResponse},
			data: insertPayload
		})
		.then(function (response) {
			console.dir(response.data);
			return resolve(response.data);
		})
		.catch(function (error) {
			console.dir(error);
			return reject(error);
		});
	})	
	
});

async function buildAndSend(payload) {
	try {
		const incrementData = await getIncrements();
		const pushPayload = await buildPushPayload(payload, incrementData);
		const pushObject = await saveToDataExtension(pushPayload, incrementData);
		await updateIncrements(incrementData);
		return pushPayload;
	} catch(err) {
		console.dir(err);
	}
}

function buildPushPayload(payload, incrementData) {
	var mobilePushData = {};
	for ( var i = 0; i < payload.length; i++ ) {
		//console.dir("Step is: " + payload[i].step + ", Key is: " + payload[i].key + ", Value is: " + payload[i].value + ", Type is: " + payload[i].type);
		mobilePushData[payload[i].key] = payload[i].value;

	}
	console.dir("building push payload")
	console.dir(mobilePushData);

	return mobilePushData;
}

async function sendBackPayload(payload) {
	try {
		const getIncrementsForSendback = await getIncrements();
		var sendBackPromotionKey = parseInt(getIncrementsForSendback.increment);
		const fullAssociationPayload = await buildAndSend(payload);
		return sendBackPromotionKey;
	} catch(err) {
		console.dir(err);
	}

}

// insert data into data extension
app.post('/dataextension/add/', async function (req, res){ 
	console.dir("Dump request body");
	console.dir(req.body);
	try {
		const returnedPayload = await sendBackPayload(req.body)
		res.send(JSON.stringify(returnedPayload));
	} catch(err) {
		console.dir(err);
	}
});

// insert data into data extension
app.post('/automation/create/query', async function (req, res){ 
	console.dir("Dump request body");
	console.dir(req.body);
	try {
		const returnedQueryId = await addQueryActivity(req.body);
		res.send(JSON.stringify(returnedQueryId));
	} catch(err) {
		console.dir(err);
	}
	
});

// insert data into data extension
app.post('/automation/create/query/seed', async function (req, res){ 
	console.dir("Dump request body");
	console.dir(req.body);
	try {
		const returnedSeedQueryId = await addSeedQueryActivity(req.body);
		res.send(JSON.stringify(returnedSeedQueryId));
	} catch(err) {
		console.dir(err);
	}
	
});

//Fetch increment values
app.get("/dataextension/lookup/increments", (req, res, next) => {

	getOauth2Token().then((tokenResponse) => {

		axios.get(incrementsUrl, { 
			headers: { 
				Authorization: tokenResponse
			}
		})
		.then(response => {
			// If request is good... 
			res.json(response.data);
		})
		.catch((error) => {
		    console.dir("Error getting increments");
		    console.dir(error);
		});
	})
});

//Fetch rows from control group data extension
app.get("/dataextension/lookup/controlgroups", (req, res, next) => {

	getOauth2Token().then((tokenResponse) => {

		axios.get(controlGroupsUrl, { 
			headers: { 
				Authorization: tokenResponse
			}
		})
		.then(response => {
			// If request is good... 
			res.json(response.data);
		})
		.catch((error) => {
		    console.dir("Error getting control groups");
		    console.dir(error);
		});
	})		

});

//Fetch rows from update contacts data extension
app.get("/dataextension/lookup/updatecontacts", (req, res, next) => {

	getOauth2Token().then((tokenResponse) => {

		axios.get(updateContactsUrl, { 
			headers: { 
				Authorization: tokenResponse
			}
		})
		.then(response => {
			// If request is good... 
			res.json(response.data);
		})
		.catch((error) => {
		    console.dir("Error getting update contacts");
		    console.dir(error);
		});
	})		

});

//Fetch rows from update contacts data extension
app.get("/dataextension/lookup/promotions", (req, res, next) => {

	getOauth2Token().then((tokenResponse) => {

		axios.get(promotionsUrl, { 
			headers: { 
				Authorization: tokenResponse
			}
		})
		.then(response => {
			// If request is good... 
			res.json(response.data);
		})
		.catch((error) => {
		    console.dir("Error getting update contacts");
		    console.dir(error);
		});
	})		

});


app.post('/journeybuilder/save/', activity.save );
app.post('/journeybuilder/validate/', activity.validate );
app.post('/journeybuilder/publish/', activity.publish );
app.post('/journeybuilder/execute/', activity.execute );
app.post('/journeybuilder/stop/', activity.stop );
app.post('/journeybuilder/unpublish/', activity.unpublish );

// listening port
http.createServer(app).listen(app.get('port'), function(){
  console.log('Express server listening on port ' + app.get('port'));
});