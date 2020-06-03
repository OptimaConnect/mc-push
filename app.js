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
	  authUrl: 							process.env.authUrl,
	  clientId: 						process.env.clientId,
	  clientSecret: 					process.env.clientSecret,
	  restUrl: 							process.env.restUrl,
	  appUrl: 							process.env.baseUrl,
	  communicationCellDataExtension: 	process.env.communicationCellDataExtension,
	  controlGroupsDataExtension: 		process.env.controlGroupsDataExtension,
	  updateContactsDataExtension: 		process.env.updateContactsDataExtension,
	  promotionsDataExtension: 			process.env.promotionsDataExtension,
	  insertDataExtension: 				process.env.insertDataExtension,
	  incrementDataExtension: 			process.env.incrementDataExtension,
	  commCellIncrementDataExtension: 	process.env.commCellIncrementDataExtension,
	  seedDataExtension: 				process.env.seedlist,
	  automationEndpoint: 				process.env.automationEndpoint,
	  promotionTableName: 				process.env.promotionTableName,
	  communicationTableName: 			process.env.communicationTableName,
	  assignmentTableName: 				process.env.assignmentTableName,
	  messageTableName: 				process.env.messageTableName,
	  offerTableName: 					process.env.offerTableName,
	  mobilePushMainTable: 				process.env.mobilePushMainTable,
	  partyCardDetailsTable:  			process.env.partyCardDetailsTable,
	  promotionDescriptionTable: 		process.env.promotionDescriptionTable,
	  seedListTable: 					process.env.seedListTable,
	  automationScheduleExtension:  	process.env.automationScheduleExtension,
	  communicationHistoryID: 			process.env.communicationHistoryID,
	  communicationHistoryKey: 			process.env.communicationHistoryKey,
	  assignmentID: 					process.env.assignmentID,
	  assignmentKey: 					process.env.assignmentKey,
	  messageID: 						process.env.messageID,
	  messageKey: 						process.env.messageKey,
	  offerID: 							process.env.offerID,
	  offerKey: 						process.env.offerKey,
	  queryFolder: 						process.env.queryFolder
	};
	console.dir(marketingCloud);
}

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


const automationUrl 		= marketingCloud.automationEndpoint;
const queryUrl 				= marketingCloud.restUrl + "/automation/v1/queries/";

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

	console.dir("Payload passed to attributes function is:");
	console.dir(payload);

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
	var setAutomationState = false;
	var communicationKey;
	var offerChannel;
	var promotionType;
	var onlinePromotionType;
	var onlineCode1;
	var instoreCode1;
	var uniqueCode1;
	var mc1;
	var mc6;
	var cellKey;
	var controlKey;
	var communicationKey;
	var communicationKeyControl;
	
	try {
		for ( t = 0; t < payload.length; t++ ) {

			console.dir("The payload key is: " + payload[t].key + " and the payload value is: " + payload[t].value);

			if ( payload[t].key == "message_key_hidden") {
				messageKeySaved = payload[t].value;
			} else if ( payload[t].key == "control_group") {
				controlGroupDE = payload[t].value;
			} else if ( payload[t].key == "update_contacts") {
				updateContactDE = payload[t].value;
			} else if ( payload[t].key == "widget_name") {
				automationName = payload[t].value;
			} else if ( payload[t].key == "push_type") {
				pushType = payload[t].value;
			} else if ( payload[t].key == "offer_promotion" && payload[t].value != "no-code" ) {
				promotionKey = payload[t].value;
			} else if ( payload[t].key == "automation_run_time" ) {
				automationRunTime = payload[t].value;
			} else if ( payload[t].key == "automation_run_date" ) {
				automationRunDate = payload[t].value;
			} else if ( payload[t].key == "automation_reoccuring" ) {
				automationReoccuring = payload[t].value;
			} else if ( payload[t].key == 'offer_channel' ) {
				offerChannel = payload[t].value;
			} else if ( payload[t].key == 'offer_promotion_type') {
				promotionType = payload[t].value;
			} else if ( payload[t].key == 'offer_online_promotion_type') {
				onlinePromotionType = payload[t].value;
			} else if ( payload[t].key == 'offer_online_code_1') {
				onlineCode1 = payload[t].value;
			} else if ( payload[t].key == 'offer_instore_code_1') {
				instoreCode1 = payload[t].value;
			} else if ( payload[t].key == 'offer_unique_code_1') {
				uniqueCode1 = payload[t].value;
			} else if (payload[t].key == 'offer_mc_1') {
				mc1 = payload[t].value;
			} else if (payload[t].key == 'offer_mc_6') {
				mc6 = payload[t].value;
			} else if (payload[t].key == 'communication_key') {
				communicationKey = payload[t].value;
			} else if (payload[t].key == 'communication_key_control') {
				communicationKeyControl = payload[t].value;
			}
		}

		if ( !automationReoccuring ) {
			setAutomationState = false;
		} else {
			setAutomationState = true;
		}

		var attributes = {
			key: messageKeySaved, 
			control_group: decodeURI(controlGroupDE), 
			update_contact: decodeURI(updateContactDE), 
			query_name: automationName,
			push_type: pushType,
			promotion_key: promotionKey,
			query_date: automationRunDate + " " + automationRunTime,
			query_reoccuring: setAutomationState,
			offer_channel: offerChannel,
			promotion_type: promotionType,
			online_promotion_type: onlinePromotionType,
			instore_code_1: instoreCode1,
			online_code_1: onlineCode1,
			unique_code_1: uniqueCode1,
			mc_1: mc1,
			mc_6: mc6,
			communication_key: communicationKey,
			communication_key_control: communicationKeyControl
		};

		console.dir("The attributes return is");
		console.dir(attributes);

		return attributes;
	} catch(e) {
		return e;
	}

};
const sendQuery = (targetId, targetKey, query, target, name, description) => new Promise((resolve, reject) => {

	getOauth2Token().then((tokenResponse) => {

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
		    "targetId": targetId,
		    "targetUpdateTypeId": 2,
		    "categoryId": marketingCloud.queryFolder
		}

	   	axios({
			method: 'post',
			url: automationUrl,
			headers: {'Authorization': tokenResponse},
			data: queryDefinitionPayload
		})
		.then(function (response) {
			console.dir(response.data);
			return resolve(response.data.queryDefinitionId);
		})
		.catch(function (error) {
			console.dir(error);
			return reject(error);
		});

	})

});


async function addQueryActivity(payload, seed) {

	console.dir("Payload for Query");
	console.dir(payload);
	var returnIds = [];

	var m = new Date();
	var dateString =
	    m.getUTCFullYear() +
	    ("0" + (m.getUTCMonth()+1)).slice(-2) +
	    ("0" + m.getUTCDate()).slice(-2) +
	    ("0" + m.getUTCHours()).slice(-2) +
	    ("0" + m.getUTCMinutes()).slice(-2) +
	    ("0" + m.getUTCSeconds()).slice(-2);

	try {
		const payloadAttributes = await definePayloadAttributes(payload);
		console.dir("The Payload Attributes");
		console.dir(payloadAttributes);
		console.dir("The Payload Attributes type is");
		console.dir(payloadAttributes.push_type);

		var sourceDataModel;
		var appCardNumber;

		if ( seed ) {
			payloadAttributes.update_contact = marketingCloud.seedListTable;
			payloadAttributes.query_name = payloadAttributes.query_name + " - SEEDLIST";
			sourceDataModel = marketingCloud.seedListTable;
			appCardNumber = "PCD.MATALAN_CARD_NUMBER";

		} else {
			sourceDataModel = marketingCloud.partyCardDetailsTable;
			appCardNumber = "PCD.APP_CARD_NUMBER";
		}
		var communicationQuery;
		var memberOfferQuery;
		var messageQuery;
		var assignmentQuery;

		if ( payloadAttributes.push_type == 'message' ) {

			// message data comes from message page TESTED
			communicationQuery = "SELECT bucket.PARTY_ID, MPT.communication_key AS COMMUNICATION_CELL_ID, CONCAT(MPT.message_target_send_date, ' ', MPT.message_target_send_time) AS CONTACT_DATE FROM [" + payloadAttributes.update_contact + "] as bucket LEFT JOIN [" + marketingCloud.mobilePushMainTable + "] AS MPT ON MPT.push_key = '" + payloadAttributes.key + "'";
			console.dir(communicationQuery);

		} else if ( payloadAttributes.push_type == 'offer' && payloadAttributes.offer_channel != '3' ) {

			// this is legit promotion, use promo key and join for comm data NOT TESTED
			communicationQuery = "SELECT bucket.PARTY_ID AS PARTY_ID, MPT.communication_key AS COMMUNICATION_CELL_ID, CONCAT(MPT.offer_start_date, ' ', MPT.offer_start_time) as CONTACT_DATE FROM [" + payloadAttributes.update_contact + "] as bucket LEFT JOIN [" + marketingCloud.mobilePushMainTable + "] AS MPT ON MPT.push_key = '" + payloadAttributes.key + "'";
			console.dir(communicationQuery);

		} else if ( payloadAttributes.push_type == 'offer' && payloadAttributes.offer_channel == '3') {

			// this is informational, comm cell should come from offer page TESTED
			communicationQuery = "SELECT bucket.PARTY_ID AS PARTY_ID, MPT.communication_key AS COMMUNICATION_CELL_ID, CONCAT(MPT.offer_start_date, ' ', MPT.offer_start_time) as CONTACT_DATE FROM [" + payloadAttributes.update_contact + "] as bucket LEFT JOIN [" + marketingCloud.mobilePushMainTable + "] AS MPT ON MPT.push_key = '" + payloadAttributes.key + "'";
			console.dir(communicationQuery);
		}


		// everyone gets contacted in some way so send one of the above queries to marketing cloud
		const communicationQueryId = await sendQuery(marketingCloud.communicationHistoryID, marketingCloud.communicationHistoryKey, communicationQuery, marketingCloud.communicationTableName, "IF028 - Communication History - " + dateString + " - " + payloadAttributes.query_name, "Communication Cell Assignment in IF028 for " + payloadAttributes.query_name);
		if ( seed ) {
			await runQuery(communicationQueryId)
		} else {		
			await logQuery(communicationQueryId, payloadAttributes.query_reoccuring, payloadAttributes.query_date);
			returnIds["communication_query_id"] = communicationQueryId;
		}

		// now we handle whether this is legit offer or a informational offer
		if ( payloadAttributes.push_type == "offer" ) {

			if ( payloadAttributes.offer_channel != "3" || payloadAttributes.offer_channel != 3 ) {

				if ( payloadAttributes.promotion_type == 'online') {
					assignmentQuery = "SELECT bucket.PARTY_ID AS PARTY_ID, MPT.offer_mc_id_1 AS MC_UNIQUE_PROMOTION_ID, CONCAT(MPT.offer_start_date, ' ', MPT.offer_start_time) as ASSIGNMENT_DATETIME FROM [" + payloadAttributes.update_contact + "] as bucket LEFT JOIN [" + marketingCloud.mobilePushMainTable + "] AS MPT ON MPT.push_key = '" + payloadAttributes.key + "' ";
					console.dir(assignmentQuery);
				} else if (payloadAttributes.promotion_type == 'online_instore') {
					assignmentQuery = "SELECT bucket.PARTY_ID AS PARTY_ID, MPT.offer_mc_id_1 AS MC_UNIQUE_PROMOTION_ID, CONCAT(MPT.offer_start_date, ' ', MPT.offer_start_time) as ASSIGNMENT_DATETIME FROM [" + payloadAttributes.update_contact + "] as bucket LEFT JOIN [" + marketingCloud.mobilePushMainTable + "] AS MPT ON MPT.push_key = '" + payloadAttributes.key + "' UNION SELECT bucket.PARTY_ID AS PARTY_ID, MPT.offer_mc_id_6 AS MC_UNIQUE_PROMOTION_ID, CONCAT(MPT.offer_start_date, ' ', MPT.offer_start_time) as ASSIGNMENT_DATETIME FROM [" + payloadAttributes.update_contact + "] as bucket LEFT JOIN [" + marketingCloud.mobilePushMainTable + "] AS MPT ON MPT.push_key = '" + payloadAttributes.key + "' ";
					console.dir(assignmentQuery);
				} else if (payloadAttributes.promotion_type == 'instore') {
					assignmentQuery = "SELECT bucket.PARTY_ID AS PARTY_ID, MPT.offer_mc_id_6 AS MC_UNIQUE_PROMOTION_ID, CONCAT(MPT.offer_start_date, ' ', MPT.offer_start_time) as ASSIGNMENT_DATETIME FROM [" + payloadAttributes.update_contact + "] as bucket LEFT JOIN [" + marketingCloud.mobilePushMainTable + "] AS MPT ON MPT.push_key = '" + payloadAttributes.key + "' ";
					console.dir(assignmentQuery);
				}

				if ( payloadAttributes.promotion_type == 'online') {

					if ( payloadAttributes.online_promotion_type == 'unique') {

						// handle unique query
						memberOfferQuery = "SELECT A.SCHEME_ID, A.LOYALTY_CARD_NUMBER, A.OFFER_ID, vp.CouponCode AS VOUCHER_ON_LINE_CODE, '' AS VOUCHER_IN_STORE_CODE, A.[START_DATE_TIME], A.[END_DATE_TIME], A.NO_REDEMPTIONS_ALLOWED, A.[VISIBLE_FROM_DATE_TIME], A.STATUS FROM ( SELECT 'Matalan' AS SCHEME_ID, " + appCardNumber + " AS LOYALTY_CARD_NUMBER, MPT.offer_id AS OFFER_ID, CONCAT(MPT.offer_start_date, ' ', MPT.offer_start_time) AS [START_DATE_TIME], CONCAT(MPT.offer_end_date, ' ', MPT.offer_end_time) AS [END_DATE_TIME], MPT.offer_redemptions  AS NO_REDEMPTIONS_ALLOWED, CONCAT(MPT.offer_start_date, ' ', MPT.offer_start_time) AS [START_DATE_TIME], MPT.offer_status AS STATUS, RN = ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) FROM [" + payloadAttributes.update_contact + "] as UpdateContactDE LEFT JOIN [" + marketingCloud.mobilePushMainTable + "] AS MPT ON MPT.push_key = '" + payloadAttributes.key + "' INNER JOIN [" + sourceDataModel + "] AS PCD ON PCD.PARTY_ID = UpdateContactDE.PARTY_ID ) A LEFT JOIN [" + payloadAttributes.unique_code_1 + "] AS VP ON A.RN = VP.RowNumber";
						console.dir(memberOfferQuery);
					
					} else {

						// handle global query
						memberOfferQuery = "SELECT 'Matalan' AS SCHEME_ID, " + appCardNumber + " AS LOYALTY_CARD_NUMBER, MPT.offer_id AS OFFER_ID, MPT.offer_online_code_1 AS VOUCHER_ON_LINE_CODE, CONCAT(MPT.offer_start_date, ' ', MPT.offer_start_time) AS [START_DATE_TIME], CONCAT(MPT.offer_end_date, ' ', MPT.offer_end_time) AS [END_DATE_TIME], '999' AS NO_REDEMPTIONS_ALLOWED, CONCAT(MPT.offer_start_date, ' ', MPT.offer_start_time) AS [VISIBLE_FROM_DATE_TIME], MPT.offer_status AS STATUS FROM [" + payloadAttributes.update_contact + "] as UpdateContactDE LEFT JOIN [" + marketingCloud.mobilePushMainTable + "] AS MPT ON MPT.push_key = '" + payloadAttributes.key + "' INNER JOIN [" + sourceDataModel + "] AS PCD ON PCD.PARTY_ID = UpdateContactDE.PARTY_ID";
						console.dir(memberOfferQuery);
					}

				} else if (payloadAttributes.promotion_type == 'online_instore') {

					if ( payloadAttributes.online_promotion_type == 'unique') {

						// handle unique query + instore code
						memberOfferQuery =
						`SELECT A.SCHEME_ID,
						A.LOYALTY_CARD_NUMBER,
						A.OFFER_ID,
						vp.CouponCode AS VOUCHER_ON_LINE_CODE,
						A.VOUCHER_IN_STORE_CODE AS VOUCHER_IN_STORE_CODE,
						A.[START_DATE_TIME],
						A.[END_DATE_TIME],
						A.NO_REDEMPTIONS_ALLOWED,
						A.[VISIBLE_FROM_DATE_TIME],
						A.STATUS 
						FROM (
							SELECT 'Matalan' AS SCHEME_ID,
							${appCardNumber} AS LOYALTY_CARD_NUMBER,
							MPT.offer_id AS OFFER_ID,
							MPT.offer_instore_code_1 AS VOUCHER_IN_STORE_CODE,
							CONCAT(MPT.offer_start_date, ' ', MPT.offer_start_time) AS [START_DATE_TIME],
							CONCAT(MPT.offer_end_date, ' ', MPT.offer_end_time) AS [END_DATE_TIME],
							MPT.offer_redemptions  AS NO_REDEMPTIONS_ALLOWED,
							CONCAT(MPT.offer_start_date, ' ', MPT.offer_start_time) AS [VISIBLE_FROM_DATE_TIME],
							MPT.offer_status AS STATUS,
							RN = ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
							FROM [${payloadAttributes.update_contact}] as UpdateContactDE
							LEFT JOIN [${marketingCloud.mobilePushMainTable}] AS MPT
							ON MPT.push_key = '${payloadAttributes.key}'
							INNER JOIN [${sourceDataModel}] AS PCD
							ON PCD.PARTY_ID = UpdateContactDE.PARTY_ID
						) A 
						LEFT JOIN (
							SELECT  CouponCode
							,       RN = ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
							FROM    [${payloadAttributes.unique_code_1}]
							WHERE   IsClaimed = 0
						) VP
						ON A.RN = VP.RN`

						console.dir(memberOfferQuery);
					
					} else {

						// handle global query + instore code
						memberOfferQuery = "SELECT 'Matalan' AS SCHEME_ID, " + appCardNumber + " AS LOYALTY_CARD_NUMBER, MPT.offer_id AS OFFER_ID, MPT.offer_online_code_1 AS VOUCHER_ON_LINE_CODE, MPT.offer_instore_code_1 AS VOUCHER_IN_STORE_CODE, CONCAT(MPT.offer_start_date, ' ', MPT.offer_start_time) AS [START_DATE_TIME], CONCAT(MPT.offer_end_date, ' ', MPT.offer_end_time) AS [END_DATE_TIME], '1' AS NO_REDEMPTIONS_ALLOWED, CONCAT(MPT.offer_start_date, ' ', MPT.offer_start_time) AS [VISIBLE_FROM_DATE_TIME], MPT.offer_status AS STATUS FROM [" + payloadAttributes.update_contact + "] as UpdateContactDE LEFT JOIN [" + marketingCloud.mobilePushMainTable + "] AS MPT ON MPT.push_key = '" + payloadAttributes.key + "' INNER JOIN [" + sourceDataModel + "] AS PCD ON PCD.PARTY_ID = UpdateContactDE.PARTY_ID";
						console.dir(memberOfferQuery);

					}

				} else if (payloadAttributes.promotion_type == 'instore') {

					// handle instore code
					memberOfferQuery = "SELECT 'Matalan' AS SCHEME_ID, " + appCardNumber + " AS LOYALTY_CARD_NUMBER, MPT.offer_id AS OFFER_ID, MPT.offer_instore_code_1 AS VOUCHER_IN_STORE_CODE, CONCAT(MPT.offer_start_date, ' ', MPT.offer_start_time) AS [START_DATE_TIME], CONCAT(MPT.offer_end_date, ' ', MPT.offer_end_time) AS [END_DATE_TIME], MPT.offer_redemptions AS NO_REDEMPTIONS_ALLOWED, CONCAT(MPT.offer_start_date, ' ', MPT.offer_start_time) AS [VISIBLE_FROM_DATE_TIME], MPT.offer_status AS STATUS FROM [" + payloadAttributes.update_contact + "] as UpdateContactDE LEFT JOIN [" + marketingCloud.mobilePushMainTable + "] AS MPT ON MPT.push_key = '" + payloadAttributes.key + "' INNER JOIN [" + sourceDataModel + "] AS PCD ON PCD.PARTY_ID = UpdateContactDE.PARTY_ID";
					console.dir(memberOfferQuery);
				}
				// send the assignment query

				const assignmentQueryId = await sendQuery(marketingCloud.assignmentID, marketingCloud.assignmentKey, assignmentQuery, marketingCloud.assignmentTableName, "IF024 Assignment - " + dateString + " - " + payloadAttributes.query_name, "Assignment in PROMOTION_ASSIGNMENT in IF024 for " + payloadAttributes.query_name);
				if ( seed ) {
					await runQuery(assignmentQueryId)
				} else {
					await logQuery(assignmentQueryId, payloadAttributes.query_reoccuring, payloadAttributes.query_date);
					returnIds["assignment_query_id"] = assignmentQueryId;
				}				


			} else {

				// same here we need member offer but not assignment as this is informational TESTED
				memberOfferQuery = "SELECT 'Matalan' AS SCHEME_ID, " + appCardNumber + " AS LOYALTY_CARD_NUMBER, MPT.offer_id AS OFFER_ID, CONCAT(MPT.offer_start_date, ' ', MPT.offer_start_time) AS [START_DATE_TIME], CONCAT(MPT.offer_end_date, ' ', MPT.offer_end_time) AS [END_DATE_TIME], '0' AS NO_REDEMPTIONS_ALLOWED, CONCAT(MPT.offer_start_date, ' ', MPT.offer_start_time) AS [VISIBLE_FROM_DATE_TIME], MPT.offer_status AS STATUS FROM [" + payloadAttributes.update_contact + "] as UpdateContactDE LEFT JOIN [" + marketingCloud.mobilePushMainTable + "] AS MPT ON MPT.push_key = '" + payloadAttributes.key + "' INNER JOIN [" + sourceDataModel + "] AS PCD ON PCD.PARTY_ID = UpdateContactDE.PARTY_ID";
				console.dir(memberOfferQuery);			

			}

			// always send the member offer query
			const memberOfferQueryId = await sendQuery(marketingCloud.offerID, marketingCloud.offerKey, memberOfferQuery, marketingCloud.offerTableName, "IF008 Offer - " + dateString + " - " + payloadAttributes.query_name, "Member Offer Assignment in IF008 for " + payloadAttributes.query_name);
			if ( seed ) {
				await runQuery(memberOfferQueryId)
			} else {
				await logQuery(memberOfferQueryId, payloadAttributes.query_reoccuring, payloadAttributes.query_date);
				returnIds["member_offer_query_id"] = memberOfferQueryId;				
			}


		
		} else if ( payloadAttributes.push_type == "message" ) {

			// message query TESTED
			messageQuery = "SELECT 'Matalan' AS SCHEME_ID, (cast(DATEDIFF(SS,'2020-01-01',getdate()) as bigint) * 100000) + row_number() over (order by (select null)) AS MOBILE_MESSAGE_ID, " + appCardNumber + " AS LOYALTY_CARD_NUMBER, MPT.message_content AS MESSAGE_CONTENT, CONCAT(MPT.message_target_send_date, ' ', MPT.message_target_send_time) AS TARGET_SEND_DATE_TIME, MPT.message_status AS STATUS, MPT.message_short_content AS SHORT_MESSAGE_CONTENT FROM [" + payloadAttributes.update_contact + "] as UpdateContactDE INNER JOIN [" + sourceDataModel + "] AS PCD ON PCD.PARTY_ID = UpdateContactDE.PARTY_ID LEFT JOIN [" + marketingCloud.mobilePushMainTable + "] as MPT ON MPT.push_key = " + payloadAttributes.key + "";
			console.dir(messageQuery);
			const messageQueryId = await sendQuery(marketingCloud.messageID, marketingCloud.messageKey, messageQuery, marketingCloud.messageTableName, "IF008 Message - " + dateString + " - " + payloadAttributes.query_name, "Message Assignment in IF008 for " + payloadAttributes.query_name);
			if ( seed ) {
				await runQuery(messageQueryId)
			} else {			
				await logQuery(messageQueryId, payloadAttributes.query_reoccuring, payloadAttributes.query_date);
				returnIds["member_message_query_id"] = messageQueryId;
			}
		}
		return returnIds;

	} catch(e) {

		console.dir(e);

	}
};

const logQuery = (queryId, type, scheduledDate) => new Promise((resolve, reject) => {

	console.dir("type:");
	console.dir(type);
	console.dir("query:");
	console.dir(queryId);
	var automationType;
	if ( type ) {
		automationType = true;
	} else {
		automationType = false;
	}

	var queryPayload = [{
        "keys": {
            "queryId": queryId
        },
        "values": {
        	"reoccurring": automationType,
        	"scheduled_run_date_time": scheduledDate
        }
	}];
	
	console.dir(queryPayload);

	getOauth2Token().then((tokenResponse) => {
	   	axios({
			method: 'post',
			url: scheduleUrl,
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

const getCommCellIncrements = () => new Promise((resolve, reject) => {
	getOauth2Token().then((tokenResponse) => {

		axios.get(commCellIncrementUrl, { 
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

const updateCommunicationCellIncrement = (key) => new Promise((resolve, reject) => {

	console.dir("current key is");
	console.dir(key);

	var insertPayload = [{
        "keys": {
            "increment_key": 1
        },
        "values": {
        	"communication_cell_code_id_increment": parseInt(key) + 3
        }
	}];
		
	console.dir(insertPayload);

	getOauth2Token().then((tokenResponse) => {
	   	axios({
			method: 'post',
			url: updateCommCellIncrementUrl,
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

const saveToCommunicationDataExtension = (payload, key) => new Promise((resolve, reject) => {

	console.dir("Payload:");
	console.dir(payload);
	console.dir("key:");
	console.dir(key);

	var insertPayload = [{
        "keys": {
            "communication_cell_id": (parseInt(key) + 1)
        },
        "values": payload.control,

	},
	{
        "keys": {
            "communication_cell_id": (parseInt(key) + 2)
        },
        "values": payload.not_control,
        
	}];
	
	console.dir(insertPayload);

	getOauth2Token().then((tokenResponse) => {
	   	axios({
			method: 'post',
			url: communicationCellUrl,
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

const updateDataExtension = (updatedPushPayload, existingKey) => new Promise((resolve, reject) => {

	console.dir("Payload:");
	console.dir(updatedPushPayload);
	console.dir("Current Key:");
	console.dir(existingKey);


	var insertPayload = [{
        "keys": {
            "push_key": parseInt(existingKey)
        },
        "values": updatedPushPayload
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


async function buildAndUpdate(payload, key) {
	try {

		const updatedPushPayload = await updatePushPayload(payload);
		const updatedPushObject = await updateDataExtension(updatedPushPayload, key);

		return updatedPushPayload;

	} catch(err) {

		console.dir(err);
	}
}

async function buildAndSend(payload) {
	try {
		const incrementData = await getIncrements();
		const commCellIncrementData = await getCommCellIncrements();

		const pushPayload = await buildPushPayload(payload, commCellIncrementData.communication_cell_code_id_increment);
		const pushObject = await saveToDataExtension(pushPayload, incrementData);

		const commPayload = await buildCommPayload(pushPayload);
		const commObject = await saveToCommunicationDataExtension(commPayload, commCellIncrementData.communication_cell_code_id_increment);

		await updateIncrements(incrementData);
		await updateCommunicationCellIncrement(commCellIncrementData.communication_cell_code_id_increment);

		return pushPayload;
	} catch(err) {
		console.dir(err);
	}
}

function getDateString(dateOffSetted) {
	let date_ob = new Date(dateOffSetted);
	let date = ("0" + date_ob.getDate()).slice(-2);
	let month = ("0" + (date_ob.getMonth() + 1)).slice(-2);
	let year = date_ob.getFullYear();
	let hours = date_ob.getHours();
	let minutes = date_ob.getMinutes();
    let seconds = date_ob.getSeconds();
    if ( minutes < 10 ) {
        minutes = "0" + minutes;
    }
    if ( seconds < 10 ) {
        seconds = "0" + seconds;
    }
	
	let dateString = year + "/" + month + "/" + date + " " + hours + ":" + minutes + ":" + seconds;	
	return dateString;
}

function getDateAndOffSet() {
    var dt = new Date();
    //console.log(dt); // Gives Tue Mar 22 2016 09:30:00 GMT+0530 (IST)

    dt.setTime(dt.getTime()+dt.getTimezoneOffset()*60*1000);
    //console.log(dt); // Gives Tue Mar 22 2016 04:00:00 GMT+0530 (IST)

    var offset = -300; //Timezone offset for EST in minutes.
    var estDate = new Date(dt.getTime() + offset*60*1000);
    console.log(estDate);
    return estDate; 
}

function buildPushPayload(payload, commCellKey) {
	var mobilePushData = {};
	for ( var i = 0; i < payload.length; i++ ) {
		//console.dir("Step is: " + payload[i].step + ", Key is: " + payload[i].key + ", Value is: " + payload[i].value + ", Type is: " + payload[i].type);
		mobilePushData[payload[i].key] = payload[i].value;

	}
	if ( mobilePushData["push_type"] == 'message' || mobilePushData["push_type"] == 'offer' && mobilePushData["offer_channel"] == '3' ) {
		mobilePushData["communication_key"] = commCellKey;
		mobilePushData["communication_control_key"] = parseInt(commCellKey) + 1;		
	}

	if ( mobilePushData.push_type == 'message') {
		mobilePushData.message_status = 'A';
	} else {
		mobilePushData.offer_status = 'A';
	}

	console.dir("building push payload")
	console.dir(mobilePushData);

	return mobilePushData;
}

function updatePushPayload(payload) {
	var mobilePushData = {};
	for ( var i = 0; i < payload.length; i++ ) {
		//console.dir("Step is: " + payload[i].step + ", Key is: " + payload[i].key + ", Value is: " + payload[i].value + ", Type is: " + payload[i].type);
		mobilePushData[payload[i].key] = payload[i].value;

	}

	var currentDateTimeStamp = getDateString(getDateAndOffSet());
	console.dir("The current DT stamp is");
	console.dir(currentDateTimeStamp);

	mobilePushData.date_added = currentDateTimeStamp;
	if ( mobilePushData.push_type == 'message') {
		mobilePushData.message_status = 'A';
	} else {
		mobilePushData.offer_status = 'C';
	}
	

	delete mobilePushData.message_key_hidden;

	console.dir("building push payload")
	console.dir(mobilePushData);

	return mobilePushData;
}

function buildCommPayload(payload, type) {

	var communicationCellData = {
			"not_control": {
		    	"cell_code"					: payload["cell_code"],
		    	"cell_name"					: payload["cell_name"],
		        "campaign_name"				: payload["campaign_name"],
		        "campaign_id"				: payload["campaign_id"],
		        "campaign_code"				: payload["campaign_code"],
		        "cell_type"					: "1",
		        "channel"					: payload["channel"],
		        "is_putput_flag"			: "1",
		        "sent"						: true			
			},
			"control": {
		    	"cell_code"					: payload["cell_code"],
		    	"cell_name"					: payload["cell_name"],
		        "campaign_name"				: payload["campaign_name"],
		        "campaign_id"				: payload["campaign_id"],
		        "campaign_code"				: payload["campaign_code"],
		        "cell_type"					: "2",
		        "channel"					: payload["channel"],
		        "is_putput_flag"			: "0",
		        "sent"						: true				
			}
	};
	console.dir(communicationCellData);
	return communicationCellData;
}

async function sendBackPayload(payload) {
	try {
		const getIncrementsForSendback = await getIncrements();
		const getCommCellForSendback =  await getCommCellIncrements();
		var sendBackPromotionKey = parseInt(getIncrementsForSendback.increment);
		const fullAssociationPayload = await buildAndSend(payload);
		return sendBackPromotionKey;
	} catch(err) {
		console.dir(err);
	}

}

async function sendBackUpdatedPayload(payload) {

	var messageKeyToUpdate;
	var h;
	for ( h = 0; h < payload.length; h++ ) {
		if ( payload[h].key == 'message_key_hidden' ) {

			messageKeyToUpdate = payload[h].value;
		}
	}
	try {
		await buildAndUpdate(payload, messageKeyToUpdate);
		return messageKeyToUpdate;
	} catch(err) {
		console.dir(err);
	}

}

const executeQuery = (executeThisQueryId) => new Promise((resolve, reject) => {

	console.dir("Executing this query Id");
	console.dir(executeThisQueryId);

	var queryPayload = queryUrl + executeThisQueryId + "/actions/start/";
	
	console.dir(queryPayload);

	getOauth2Token().then((tokenResponse) => {
	   	axios({
			method: 'post',
			url: queryPayload,
			headers: {'Authorization': tokenResponse},
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

async function runQuery(executeThisQueryId) {
	try {
		const returnQueryStatus = await executeQuery(executeThisQueryId);
		console.dir("The query status is");
		console.dir(returnQueryStatus);
		return returnQueryStatus;
	} catch(err) {
		console.dir(err);
	}
}

/**

POST /automation/v1/queries/{{queryID}}/actions/start/
Host: {{yourendpoint}}.rest.marketingcloudapis.com
Authorization: Bearer {{Oauth Key}}
Content-Type: application/json

*/
app.post('/run/query/:queryId', async function(req, res) {

	//res.send("Enviro is " + req.params.enviro + " | Interface is " + req.params.interface + " | Folder is " + req.params.folder);
	console.dir("Query ID sent from Automation Studio");
	console.dir(req.params.queryId);
	var executeThisQueryId = req.params.queryId;
	try {
		const returnQueryResponse = await runQuery(executeThisQueryId);
		console.dir("The query response object is");
		console.dir(returnQueryResponse);
		res.send(JSON.stringify(returnQueryResponse));
	} catch (err) {
		console.dir(err);
	}
});

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
app.post('/dataextension/update/', async function (req, res){ 
	console.dir("Dump request body");
	console.dir(req.body);
	try {
		const returnedUpdatePayload = await sendBackUpdatedPayload(req.body)
		res.send(JSON.stringify(returnedUpdatePayload));
	} catch(err) {
		console.dir(err);
	}
});

// insert data into data extension
app.post('/automation/create/query', async function (req, res){ 
	console.dir("Dump request body");
	console.dir(req.body);
	try {
		const returnedQueryId = await addQueryActivity(req.body, false);
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
		const returnedQueryId = await addQueryActivity(req.body, true);
		res.send(JSON.stringify(returnedQueryId));
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

//Fetch increment values
app.get("/dataextension/lookup/commincrements", (req, res, next) => {

	getOauth2Token().then((tokenResponse) => {

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