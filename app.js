'use strict';

// Module Dependencies
require('dotenv').config();
const axios 				= require('axios');
const express     			= require('express');
const bodyParser  			= require('body-parser');
const errorhandler 			= require('errorhandler');
const http        			= require('http');
const path        			= require('path');
const request     			= require('request');
const moment 				= require('moment-timezone');
const routes      			= require('./routes');
const activity    			= require('./routes/activity');
const urlencodedparser 		= bodyParser.urlencoded({extended:false});
const app 					= express();
const { ServiceBusClient } 	= require('@azure/service-bus');
const journeyTokenHandler 	= require("./journeytokenhandler.js");
const recurringSql 			= require("./recurringSql.js");
const salesforceApi			= require("./salesforceApi.js");
const adhocSql				= require("./adhocSql.js");

// access Heroku variables

var marketingCloud = {
	authUrl: 							process.env.authUrl,
	clientId: 							process.env.clientId,
	clientSecret: 						process.env.clientSecret,
	restUrl: 							process.env.restUrl,
	appUrl: 							process.env.baseUrl,
	communicationCellDataExtension: 	process.env.communicationCellDataExtension,
	controlGroupsDataExtension: 		process.env.controlGroupsDataExtension,
	updateContactsDataExtension: 		process.env.updateContactsDataExtension,
	promotionsDataExtension: 			process.env.promotionsDataExtension,
	insertDataExtension: 				process.env.insertDataExtension,
	incrementDataExtension: 			process.env.incrementDataExtension,
	commCellIncrementDataExtension: 	process.env.commCellIncrementDataExtension,
	seedDataExtension: 					process.env.seedlist,
	automationEndpoint: 				process.env.automationEndpoint,
	promotionTableName: 				process.env.promotionTableName,
	communicationTableName: 			process.env.communicationTableName,
	communicationHistoryKey: 			process.env.communicationHistoryKey,
	assignmentTableName: 				process.env.assignmentTableName,
	assignmentKey: 						process.env.assignmentKey,
	messageTableName: 					process.env.messageTableName,
	messageKey: 						process.env.messageKey,
	masterOfferTableName: 				process.env.masterOfferTableName,
	masterOfferKey: 					process.env.masterOfferKey,
	memberOfferTableName: 				process.env.memberOfferTableName,
	memberOfferKey: 					process.env.memberOfferKey,
	mobilePushMainTable: 				process.env.mobilePushMainTable,
	mobilePushMainKey:					process.env.mobilePushMainKey,
	partyCardDetailsTable:  			process.env.partyCardDetailsTable,
	promotionDescriptionTable: 			process.env.promotionDescriptionTable,
	seedListTable: 						process.env.seedListTable,
	automationScheduleExtension:  		process.env.automationScheduleExtension,
	queryFolder: 						process.env.queryFolder,
	uniqueVoucherPotsKey:				process.env.uniqueVoucherPotsKey,
	voucherGroupKey:					process.env.voucherGroupKey,
	promotionIncrementsName:			process.env.promotionIncrementsName,
	voucherSetName:						process.env.voucherSetName,
	voucherSubsetName:					process.env.voucherSubsetName,
	recurringVoucherSubsetsName:		process.env.recurringVoucherSubsetsName,
	stagingCommunicationCellName:		process.env.stagingCommunicationCellName,
	stagingPromotionDescriptionName:	process.env.stagingPromotionDescriptionName,
	globalVoucherName:					process.env.globalVoucherName,
	communicationCellName:				process.env.communicationCellName,
	recurringCampaignsName: 			process.env.recurringCampaignsName,
	stagingMemberOfferName:				process.env.stagingMemberOfferName,
	uniqueVoucherName:					process.env.uniqueVoucherName,
	stagingCommunicationCellId:			process.env.stagingCommunicationCellId,
	stagingPromotionDescriptionId:		process.env.stagingPromotionDescriptionId,
	recurringVoucherSubsetsId:			process.env.recurringVoucherSubsetsId,
	promotionDescriptionId:				process.env.promotionDescriptionId,
	recurringCampaignsId:				process.env.recurringCampaignsId,
	stagingMemberOfferId:				process.env.stagingMemberOfferId,
	uniqueVoucherId:					process.env.uniqueVoucherId
};
console.dir(marketingCloud);

// url constants
const scheduleUrl 					= marketingCloud.restUrl + "hub/v1/dataevents/key:" 		+ marketingCloud.automationScheduleExtension 	+ "/rowset";
const communicationCellUrl 			= marketingCloud.restUrl + "hub/v1/dataevents/key:" 		+ marketingCloud.communicationCellDataExtension + "/rowset";
const controlGroupsUrl 				= marketingCloud.restUrl + "data/v1/customobjectdata/key/" 	+ marketingCloud.controlGroupsDataExtension 	+ "/rowset";
const updateContactsUrl 			= marketingCloud.restUrl + "data/v1/customobjectdata/key/" 	+ marketingCloud.updateContactsDataExtension 	+ "/rowset";
const promotionsUrl 				= marketingCloud.restUrl + "data/v1/customobjectdata/key/" 	+ marketingCloud.promotionsDataExtension 		+ "/rowset";
const insertUrl 					= marketingCloud.restUrl + "hub/v1/dataevents/key:" 		+ marketingCloud.insertDataExtension 			+ "/rowset";
const incrementsUrl 				= marketingCloud.restUrl + "data/v1/customobjectdata/key/" 	+ marketingCloud.incrementDataExtension 		+ "/rowset";
const updateIncrementUrl 			= marketingCloud.restUrl + "hub/v1/dataevents/key:" 		+ marketingCloud.incrementDataExtension 		+ "/rowset";
const commCellIncrementUrl 			= marketingCloud.restUrl + "data/v1/customobjectdata/key/" 	+ marketingCloud.commCellIncrementDataExtension + "/rowset";
const updateCommCellIncrementUrl  	= marketingCloud.restUrl + "hub/v1/dataevents/key:" 		+ marketingCloud.commCellIncrementDataExtension + "/rowset";
const mobilePushMainTableUrl  		= marketingCloud.restUrl + "data/v1/customobjectdata/key/" 	+ marketingCloud.mobilePushMainKey				+ "/rowset";
const uniqueVoucherPotsUrl 			= marketingCloud.restUrl + "data/v1/customobjectdata/key/" 	+ marketingCloud.uniqueVoucherPotsKey			+ "/rowset";
const voucherGroupUrl 				= marketingCloud.restUrl + "data/v1/customobjectdata/key/" 	+ marketingCloud.voucherGroupKey 				+ "/rowset";


const automationUrl 		= marketingCloud.automationEndpoint;
const queryUrl 				= marketingCloud.restUrl + "/automation/v1/queries/";

// Configure Express master
app.set('port', process.env.PORT || 3000);
app.use(bodyParser.raw({type: 'application/jwt'}));
app.use(express.static(path.join(__dirname, 'public')));
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

app.post('/journeybuilder/save/', activity.save );
app.post('/journeybuilder/validate/', activity.validate );
app.post('/journeybuilder/publish/', activity.publish );
app.post('/journeybuilder/execute/', activity.execute );
app.post('/journeybuilder/stop/', activity.stop );
app.post('/journeybuilder/unpublish/', activity.unpublish );

// Express in Development Mode
if ('development' == app.get('env')) {
	app.use(errorhandler());
} else {
	app.use(journeyTokenHandler.validateToken);
}

function formatPromotionKey (promotionKey) {
	let formattedPromotionKey;
	if(promotionKey != "no-code"){
		formattedPromotionKey = promotionKey;
	}
	return formattedPromotionKey;
}

function formatQueryDate (automationRunDate, automationRunTime) {
	const formattedQueryDate = 	automationRunDate + " " + automationRunTime;
	return formattedQueryDate;
}

function isRecurring (automationRecurring) {
	let queryRecurring;
	if ( !automationRecurring) {
		queryRecurring = false;
	} else {
		queryRecurring = true;
	}
	return queryRecurring;
}

function definePayloadAttributes(payload) {

	console.dir("Payload passed to attributes function is:");
	console.dir(payload);

	var attributes = {
		key: payload.find(prop => prop.key == "message_key_hidden")?.value,
		control_group: decodeURI(payload.find(prop => prop.key == "control_group")?.value),
		update_contact: decodeURI(payload.find(prop => prop.key == "update_contacts")?.value), 
		query_name: payload.find(prop => prop.key == "widget_name")?.value,
		push_type: payload.find(prop => prop.key == "push_type")?.value,
		promotion_key: formatPromotionKey(payload.find(prop => prop.key == "offer_promotion")?.value),
		query_date: formatQueryDate(payload.find(prop => prop.key == "automation_run_date")?.value, payload.find(prop => prop.key == "automation_run_time")?.value),
		//query_recurring: isRecurring(payload.find(prop => prop.key == "automation_recurring")?.value),
		offer_channel: payload.find(prop => prop.key == "offer_channel")?.value,
		promotion_type: payload.find(prop => prop.key == "offer_promotion_type")?.value,
		online_promotion_type: payload.find(prop => prop.key == "offer_online_promotion_type")?.value,
		instore_code_1: payload.find(prop => prop.key == "offer_instore_code_1")?.value,
		online_code_1: payload.find(prop => prop.key == "offer_online_code_1")?.value,
		unique_code_1: payload.find(prop => prop.key == "offer_unique_code_1")?.value,
		mc_1: payload.find(prop => prop.key == "offer_mc_1")?.value,
		mc_6: payload.find(prop => prop.key == "offer_mc_6")?.value,
		communication_key: payload.find(prop => prop.key == "communication_key")?.value,
		communication_key_control: payload.find(prop => prop.key == "communication_key_control")?.value,
		offer_validity: payload.find(prop => prop.key == "offer_validity")?.value
	};
		
	console.dir("The attributes return is");
	console.dir(attributes);
	return attributes;
};

const updateTypes = {
	Overwrite: 0,
	AddUpdate: 1,
	Append: 2
}



async function getNewPushKey() {

	const token = await salesforceApi.getOauth2Token();

	const getResponse = await axios.get(incrementsUrl, { 
		headers: { 
			Authorization: token
		}
	});

	const newPushKey = parseInt(getResponse.data.items[0].values.increment);

	if (isNaN(newPushKey)) {
		throw new Error(`Invalid push_key increment value: ${getResponse.data.items[0].values.increment}`);
	}

	const updateNextIdPayload = [{
        "keys": {
            "id": 1
        },
        "values": {
			"increment": newPushKey + 1
		}
	}];

	await axios({
		method: 'post',
		url: updateIncrementUrl,
		headers: { Authorization: token },
		data: updateNextIdPayload
	});

	return newPushKey;
};

async function getNewCommCellId() {

	const token = await salesforceApi.getOauth2Token();
	const getResponse = await axios.get(commCellIncrementUrl, { 
		headers: { 
			Authorization: token
		}
	});

	const newCommCellId = parseInt(getResponse.data.items[0].values.communication_cell_code_id_increment);
	if (isNaN(newCommCellId)) {
		throw new Error(`Invalid communication_cell_id increment value: ${getResponse.data.items[0].values.communication_cell_code_id_increment}`);
	}

	const updateNextIdPayload = [{
        "keys": {
            "increment_key": 1
        },
        "values": {
			"communication_cell_code_id_increment": newCommCellId + 3
		}
	}];

	await axios({
		method: 'post',
		url: updateCommCellIncrementUrl,
		headers: { Authorization: token },
		data: updateNextIdPayload
	});	

	return newCommCellId;
};

async function saveToCommunicationDataExtension(commCellPayload, commCellId) {

	console.dir("Payload:");
	console.dir(commCellPayload);
	console.dir("CommCellId:");
	console.dir(commCellId);

	const insertPayload = [{
		"keys": {
            "communication_cell_id": commCellId
        },
        "values": commCellPayload
	}];
	
	console.dir(insertPayload);

	const token = await salesforceApi.getOauth2Token();

	const response = await axios({
		method: 'post',
		url: communicationCellUrl,
		headers: { Authorization: token },
		data: insertPayload
	});

	console.dir(response.data);
	return response.data;
};

async function saveToMainDataExtension(pushPayload, pushKey) {
	console.dir("Payload:");
	console.dir(pushPayload);
	console.dir("Current Key:");
	console.dir(pushKey);

	const insertPayload = [{
        "keys": {
            "push_key": parseInt(pushKey)
        },
        "values": pushPayload
	}];
	
	console.dir(insertPayload);

	const tokenResponse = await salesforceApi.getOauth2Token();

	const response = await axios({
		method: 'post',
		url: insertUrl,
		headers: {'Authorization': tokenResponse},
		data: insertPayload
	});
	console.dir(response.data);
	return response.data;	
};

async function buildAndUpdate(payload, key) {
	const updatedPushPayload = updatePushPayload(payload);
	const updatedPushObject = await saveToMainDataExtension(updatedPushPayload, key);

	return updatedPushPayload;
}

async function buildAndSend(payload) {
	const newPushKey = await getNewPushKey();

	let commCellId;
	const existingCommCellId = payload.find(element => element.key == "communication_key")?.value;
	if (existingCommCellId) {
		commCellId = existingCommCellId;
	} else {
		commCellId = await getNewCommCellId();
	}

	const pushPayload = buildPushPayload(payload, commCellId);
	const pushObject = await saveToMainDataExtension(pushPayload, newPushKey);
	
	const commPayload = buildCommPayload(pushPayload);
	const commObject = await saveToCommunicationDataExtension(commPayload, commCellId);

	return newPushKey;
}

function getSfmcDatetimeNow() {
	const sfmcNow = moment().tz("Etc/GMT+6");
	return sfmcNow.format();
}

function buildPushPayload(payload, commCellKey) {
	let mobilePushData = {};
	for ( let i = 0; i < payload.length; i++ ) {
		mobilePushData[payload[i].key] = payload[i].value;
	}

	if (!mobilePushData["communication_key"]) {
		mobilePushData["communication_key"] = commCellKey;
		mobilePushData["communication_control_key"] = parseInt(commCellKey) + 1;
	}
	
	const currentDateTimeStamp = getSfmcDatetimeNow();
	console.dir("The current DT stamp is");
	console.dir(currentDateTimeStamp);

	mobilePushData.date_updated = currentDateTimeStamp;
	mobilePushData.date_created = currentDateTimeStamp;

	console.dir("building push payload")
	console.dir(mobilePushData);

	return mobilePushData;
}

function updatePushPayload(payload) {
	let mobilePushData = {};
	for ( let i = 0; i < payload.length; i++ ) {
		mobilePushData[payload[i].key] = payload[i].value;
	}

	const currentDateTimeStamp = getSfmcDatetimeNow();
	console.dir("The current DT stamp is");
	console.dir(currentDateTimeStamp);

	mobilePushData.date_updated = currentDateTimeStamp;
	delete mobilePushData.message_key_hidden;

	console.dir("building push payload")
	console.dir(mobilePushData);

	return mobilePushData;
}

function buildCommPayload(payload) {

	const pushType = payload["push_type"];
	if (!pushType) {
		throw new Error("No push_type specified in the payload.");
	}

	let communicationCellData;

	if (pushType == "offer") {
		communicationCellData = {
			"cell_code"					: payload["offer_cell_code"],
    		"cell_name"					: payload["offer_cell_name"],
        	"campaign_name"				: payload["offer_campaign_name"],
			"campaign_code"				: payload["offer_campaign_code"],
        	"cell_type"					: 1,
        	"channel"					: 5,
        	"is_putput_flag"			: 1,
			"sent"						: true,
			"base_contact_date"			: payload["offer_start_datetime"]
		}
	} else {
		communicationCellData = {
			"cell_code"					: payload["cell_code"],
    		"cell_name"					: payload["cell_name"],
        	"campaign_name"				: payload["campaign_name"],
        	"campaign_code"				: payload["campaign_code"],
        	"cell_type"					: 1,
        	"channel"					: 6,
        	"is_putput_flag"			: 1,
			"sent"						: true,
			"base_contact_date"			: payload["message_target_send_datetime"]
		}
	}

	console.dir(communicationCellData);
	return communicationCellData;
}

async function sendBackUpdatedPayload(payload) {
	let messageKeyToUpdate = payload.find(element => element.key == "message_key_hidden");

	if (!messageKeyToUpdate) {
		throw new Error("No message_key provided in the payload.");
	}

	messageKeyToUpdate = messageKeyToUpdate.value;
	await buildAndUpdate(payload, messageKeyToUpdate);
	return messageKeyToUpdate;
}

async function getKeyForVoucherDataExtensionByName(voucherDEName) {
	let tokenResponse = await salesforceApi.getOauth2Token();

	const fullRequestUrl = `${uniqueVoucherPotsUrl}?$filter=dataExtensionName eq ${voucherDEName}`;

	try {
		let response = await axios.get(fullRequestUrl, { 
			headers: { 
				Authorization: tokenResponse
			}
		});

		let data = response.data.items[0];
		return data.keys.id;
	}
	catch (error) {
		throw new Error(`Error getting voucher DE key: ${error}`);
	}
}

async function getExistingAppData(message_key) {
	let token = await salesforceApi.getOauth2Token();
	const fullRequestUrl = `${mobilePushMainTableUrl}?$filter=push_key eq ${message_key}`;

	try {
		let response = await axios.get(fullRequestUrl, {
			headers: {
				Authorization: token
			}
		});

		let data = response.data.items[0];

		return {...data.keys, ...data.values}

	} catch (error) {
		console.dir(error);
		return null;
	}
}

async function cancelOffer(message_key, offer_id) {
	console.log(`Cancelling offer_id = ${offer_id}`);
	
	let cancelOfferQuery = 
		`SELECT  o.SCHEME_ID
		,       o.OFFER_ID
		,       o.VOUCHER_IN_STORE_CODE
		,       o.SHORT_DESCRIPTION
		,       o.LONG_DESCRIPTION
		,       o.START_DATE_TIME
		,       o.END_DATE_TIME
		,       'D'                 AS [STATUS]
		,       o.OFFER_TYPE
		,       o.IMAGE_URL_1
		,       o.MORE_INFO_TEXT
		,       o.ONLINE_OFFER_CLICKTHROUGH_URL
		,       o.OFFER_CHANNEL
		,       o.OFFER_STORES
		,       o.SHOW_VALIDITY
		,       o.INFO_BUTTON_TEXT
		,       o.CRITERIA
		,       o.DATE_CREATED
		,       SYSDATETIME()       AS DATE_UPDATED
		FROM    [${marketingCloud.mobilePushMainTable}] AS mpt
		INNER JOIN [${marketingCloud.masterOfferTableName}] AS o
		ON      mpt.OFFER_ID = o.OFFER_ID
		WHERE   mpt.push_type = 'offer'
		AND     mpt.push_key = ${message_key}`;

	console.log(`Cancel Offer Query: ${cancelOfferQuery}`);

	const dateTimestamp = new Date().toISOString();
	const cancelOfferQueryName = `Cancel Offer ${offer_id} - ${dateTimestamp}`;

	const cancelOfferQueryId = await salesforceApi.createSQLQuery(marketingCloud.masterOfferKey, cancelOfferQuery, updateTypes.AddUpdate, marketingCloud.masterOfferTableName, cancelOfferQueryName, cancelOfferQueryName);
	await salesforceApi.runSQLQuery(cancelOfferQueryId, cancelOfferQueryName);
}

async function cancelPush(message_key, push_content) {
	console.log(`Cancelling push message: ${push_content}`);

	let cancelPushQuery =
		`SELECT  SCHEME_ID
		,       MOBILE_MESSAGE_ID
		,       LOYALTY_CARD_NUMBER
		,       MESSAGE_CONTENT
		,       TARGET_SEND_DATE_TIME
		,       'D'         AS [STATUS]
		,       PUSH_KEY
		,       DATE_CREATED
		,       SYSDATETIME() AS DATE_UPDATED
		,       [URL]
		,       [TITLE]
		FROM    [${marketingCloud.messageTableName}]
		WHERE   PUSH_KEY = '${message_key}'
		AND     CAST(TARGET_SEND_DATE_TIME AS datetime) AT TIME ZONE 'UTC' >= SYSDATETIMEOFFSET()`;

	console.log(`Cancel Push Query: ${cancelPushQuery}`);

	const dateTimestamp = new Date().toISOString();
	const cancelPushQueryName = `Cancel Push "${push_content.substring(0, 20)}..." - ${dateTimestamp}`;

	const cancelPushQueryId = await salesforceApi.createSQLQuery(marketingCloud.messageKey, cancelPushQuery, updateTypes.AddUpdate, marketingCloud.messageTableName, cancelPushQueryName, cancelPushQueryName);
	await salesforceApi.runSQLQuery(cancelPushQueryId, cancelPushQueryName);
}


//#region endpoints

// insert data into mobile_push_main data extension
app.post('/dataextension/add/', async function (req, res, next){ 
	console.dir("Dump request body");
	console.dir(req.body);
	try {
		const newPushKey = await buildAndSend(req.body)
		res.status(201).send(JSON.stringify(newPushKey));
	} catch(err) {
		console.dir(err);
		const error_message = err.response?.data?.additionalErrors[0]?.message;
		if (error_message){
			res.status(400).send(error_message);
		}
		res.status(500).send(JSON.stringify(err));		
	}
});

// update data in mobile_push_main data extension
app.post('/dataextension/update/', async function (req, res, next){ 
	console.dir("Dump request body");
	console.dir(req.body);
	try {
		const returnedUpdatePayload = await sendBackUpdatedPayload(req.body)
		res.send(JSON.stringify(returnedUpdatePayload));
	} catch(err) {
		console.dir(err);
		const error_message = err.response?.data?.additionalErrors[0]?.message;
		if (error_message){
			res.status(400).send(error_message);
		}
		res.status(500).send(JSON.stringify(err));
	}
});

// cancel a push or offer
app.post('/cancel/:message_key', async function (req, res, next) {

	try {
		const message_key = req.params.message_key;

		// get offer type from mobile_push_main DE
		const existingRow = await getExistingAppData(message_key);

		if (existingRow == null) {
			res.sendStatus(404);
			return;
		}

		const offer_type = existingRow.push_type;

		if (offer_type == "offer"){
			await cancelOffer(message_key, existingRow.offer_id);
		} else if (offer_type.includes("message")) {
			await cancelPush(message_key, existingRow.message_content);
		} else {
			res.status(405).send("Unrecognised app message type.");
			return;
		}

		res.sendStatus(202);
	} catch (error) {
		console.dir(error);
		const error_message = err.response?.data?.additionalErrors[0]?.message;
		if (error_message){
			res.status(400).send(error_message);
		}
		res.status(500).send(JSON.stringify(error));
	}
});

app.post('/send/broadcast', async function (req, res, next){ 
	console.dir("Dump request body");
	console.dir(req.body);
	try {
		const returnedQueryId = await adhocSql.addQueryActivity(definePayloadAttributes(req.body), false, marketingCloud, updateTypes);
		res.send(JSON.stringify(returnedQueryId));
	} catch(err) {
		console.dir(err);
		const error_message = err.response?.data?.additionalErrors[0]?.message;
		if (error_message){
			res.status(400).send(error_message);
		}
		res.status(500).send(JSON.stringify(err));
	}
});

app.post('/send/createautomation', async function (req, res, next){ 
	console.dir("Dump request body");
	console.dir(req.body);
	const payloadAttributes = await definePayloadAttributes(payload);
	try {
		const returnedQueryId = await recurringSql.recurringCamapign(marketingCloud, payloadAttributes);
		res.send(JSON.stringify(returnedQueryId));
	} catch(err) {
		console.dir(err);
		const error_message = err.response?.data?.additionalErrors[0]?.message;
		if (error_message){
			res.status(400).send(error_message);
		}
		res.status(500).send(JSON.stringify(err));
	}
});

app.post('/send/seed', async function (req, res, next){ 
	console.dir("Dump request body");
	console.dir(req.body);
	try {
		const returnedQueryId = await adhocSql.addQueryActivity(definePayloadAttributes(req.body), true, marketingCloud, updateTypes);
		res.send(JSON.stringify(returnedQueryId));
	} catch(err) {
		console.dir(err);
		const error_message = err.response?.data?.additionalErrors[0]?.message;
		if (error_message){
			res.status(400).send(error_message);
		}
		res.status(500).send(JSON.stringify(err));
	}
	
});

// Fetch existing row from mobile push main table
app.get("/dataextension/lookup/mobilepushmain/:pushKey", (req, res, next) => {
	salesforceApi.getOauth2Token().then((tokenResponse) => {
		const fullRequestUrl = `${mobilePushMainTableUrl}?$filter=push_key eq ${req.params.pushKey}`;

		axios.get(fullRequestUrl, { 
			headers: { 
				Authorization: tokenResponse
			}
		})
		.then(response => {
			// If request is good... 
			res.json(response.data);
		})
		.catch((error) => {
		    console.dir("Error getting push main data");
			console.dir(error);
			next(error)
		});
	})
});


//Fetch increment values
app.get("/dataextension/lookup/increments", (req, res, next) => {

	salesforceApi.getOauth2Token().then((tokenResponse) => {

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
			next(error);
		});
	})
});

//Fetch increment values
app.get("/dataextension/lookup/commincrements", (req, res, next) => {

	salesforceApi.getOauth2Token().then((tokenResponse) => {

		axios.get(commCellIncrementUrl, { 
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
			next(error);
		});
	})
});

//Fetch rows from control group data extension
app.get("/dataextension/lookup/controlgroups", (req, res, next) => {

	salesforceApi.getOauth2Token().then((tokenResponse) => {

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
			next(error);
		});
	})		

});

//Fetch rows from update contacts data extension
app.get("/dataextension/lookup/updatecontacts", (req, res, next) => {

	salesforceApi.getOauth2Token().then((tokenResponse) => {

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
			next(error);
		});
	})		

});

//Fetch rows from update contacts data extension
app.get("/dataextension/lookup/promotions", (req, res, next) => {

	salesforceApi.getOauth2Token().then((tokenResponse) => {

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
		    console.dir("Error getting promotions");
			console.dir(error);
			next(error);
		});
	})		

});

app.get("/dataextension/lookup/vouchergroups", (req, res, next) => {

	salesforceApi.getOauth2Token().then((tokenResponse) => {

		axios.get(voucherGroupUrl, { 
			headers: { 
				Authorization: tokenResponse
			}
		})
		.then(response => {
			// If request is good... 
			res.json(response.data);
		})
		.catch((error) => {
		    console.dir("Error getting voucher Groups");
			console.dir(error);
			next(error);
		});
	})		

});

//#endregion endpoints


// listening port
http.createServer(app).listen(app.get('port'), function(){
  console.log('Express server listening on port ' + app.get('port'));
});