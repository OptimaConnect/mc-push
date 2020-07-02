'use strict';

// Module Dependencies
require('dotenv').config();
const axios 			= require('axios');
const express     		= require('express');
const bodyParser  		= require('body-parser');
const errorhandler 		= require('errorhandler');
const http        		= require('http');
const path        		= require('path');
const request     		= require('request');
const moment 			= require('moment-timezone');
const routes      		= require('./routes');
const activity    		= require('./routes/activity');
const urlencodedparser 	= bodyParser.urlencoded({extended:false});
const app 				= express();
const local       		= false;
const { ServiceBusClient } = require('@azure/service-bus');


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
	  communicationHistoryKey: 			process.env.communicationHistoryKey,
	  assignmentTableName: 				process.env.assignmentTableName,
	  assignmentKey: 					process.env.assignmentKey,
	  messageTableName: 				process.env.messageTableName,
	  messageKey: 						process.env.messageKey,
	  masterOfferTableName: 			process.env.masterOfferTableName,
	  masterOfferKey: 					process.env.masterOfferKey,
	  memberOfferTableName: 			process.env.memberOfferTableName,
	  memberOfferKey: 					process.env.memberOfferKey,
	  mobilePushMainTable: 				process.env.mobilePushMainTable,
	  mobilePushMainKey:				process.env.mobilePushMainKey,
	  partyCardDetailsTable:  			process.env.partyCardDetailsTable,
	  promotionDescriptionTable: 		process.env.promotionDescriptionTable,
	  seedListTable: 					process.env.seedListTable,
	  automationScheduleExtension:  	process.env.automationScheduleExtension,
	  queryFolder: 						process.env.queryFolder,
	  uniqueVoucherPotsKey:				process.env.uniqueVoucherPotsKey
	};
	console.dir(marketingCloud);
}

const azureServiceBusConnectionString =	process.env.azureServiceBusConnectionString;
const azureQueueName = process.env.azureQueueName;

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

const updateTypes = {
	Overwrite: 0,
	AddUpdate: 1,
	Append: 2
}

const createSQLQuery = (targetKey, query, updateType, target, name, description) => new Promise((resolve, reject) => {

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
		    "targetUpdateTypeId": updateType,
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
		("0" + (m.getUTCMonth() + 1)).slice(-2) +
		("0" + m.getUTCDate()).slice(-2) +
		("0" + m.getUTCHours()).slice(-2) +
		("0" + m.getUTCMinutes()).slice(-2) +
		("0" + m.getUTCSeconds()).slice(-2);

	const payloadAttributes = await definePayloadAttributes(payload);
	console.dir("The Payload Attributes");
	console.dir(payloadAttributes);
	console.dir("The Payload Attributes type is");
	console.dir(payloadAttributes.push_type);

	let sourceDataModel;
	let appCardNumber;
	let target_send_date_time;
	let visible_from_date_time;

	if (seed) {
		payloadAttributes.update_contact = marketingCloud.seedListTable;
		payloadAttributes.query_name = payloadAttributes.query_name + " - SEEDLIST";
		sourceDataModel = marketingCloud.seedListTable;
		appCardNumber = "PCD.MATALAN_CARD_NUMBER";
		target_send_date_time =
			`CASE	WHEN MPT.[message_seed_send_datetime] AT TIME ZONE 'GMT Standard Time' < SYSDATETIMEOFFSET()
						THEN SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time'
					ELSE MPT.[message_seed_send_datetime] AT TIME ZONE 'GMT Standard Time'
			END`;
		visible_from_date_time = "SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time'"
	} else {
		sourceDataModel = marketingCloud.partyCardDetailsTable;
		appCardNumber = "PCD.APP_CARD_NUMBER";
		target_send_date_time = "MPT.[message_target_send_datetime] AT TIME ZONE 'GMT Standard Time'";
		visible_from_date_time = "MPT.[offer_start_datetime] AT TIME ZONE 'GMT Standard Time'";
	}

	let communicationQuery;
	if (payloadAttributes.push_type == 'message') {

		// message data comes from message page
		communicationQuery =
			`SELECT bucket.PARTY_ID,
			MPT.communication_key	AS COMMUNICATION_CELL_ID,
			CAST(${target_send_date_time} AS datetime) AS CONTACT_DATE
			FROM [${payloadAttributes.update_contact}] AS bucket
			INNER JOIN [${marketingCloud.mobilePushMainTable}] AS MPT
			ON MPT.push_key = ${payloadAttributes.key}`

		console.dir(communicationQuery);

	} else if (payloadAttributes.push_type == 'offer' && payloadAttributes.offer_channel != '3') {
		// TODO - Update this to pull communication cell id from promotion widget table - possibly unneeded - communication_key may already have come from the promo widget
		// this is legit promotion, use promo key and join for comm data NOT TESTED
		communicationQuery =
			`SELECT bucket.PARTY_ID AS PARTY_ID,
			MPT.communication_key 	AS COMMUNICATION_CELL_ID,
			CAST(${visible_from_date_time} AS datetime) AS CONTACT_DATE
			FROM [${payloadAttributes.update_contact}] AS bucket
			INNER JOIN [${marketingCloud.mobilePushMainTable}] AS MPT
			ON MPT.push_key = ${payloadAttributes.key}`

		console.dir(communicationQuery);

	} else if (payloadAttributes.push_type == 'offer' && payloadAttributes.offer_channel == '3') {

		// this is informational, comm cell should come from offer page
		communicationQuery =
			`SELECT bucket.PARTY_ID AS PARTY_ID,
			MPT.communication_key 	AS COMMUNICATION_CELL_ID,
			CAST(${visible_from_date_time} AS datetime) AS CONTACT_DATE
			FROM [${payloadAttributes.update_contact}] as bucket
			INNER JOIN [${marketingCloud.mobilePushMainTable}] AS MPT
			ON MPT.push_key = ${payloadAttributes.key}`

		console.dir(communicationQuery);
	}

	// Create and run comms history SQL - not needed for seeds or non-loyalty pushes
	if (!seed && payloadAttributes.push_type != 'message_non_loyalty') {
		const communicationQueryName = `IF028 - Communication History - ${dateString} - ${payloadAttributes.query_name}`;
		const communicationQueryId = await createSQLQuery(marketingCloud.communicationHistoryKey, communicationQuery, updateTypes.Append, marketingCloud.communicationTableName, communicationQueryName, `Communication Cell Assignment in IF028 for ${payloadAttributes.query_name}`);
		await runSQLQuery(communicationQueryId, communicationQueryName)
		returnIds["communication_query_id"] = communicationQueryId;
	}

	// now we handle whether this is a voucher offer or a informational offer
	if (payloadAttributes.push_type == "offer") {

		if (payloadAttributes.offer_channel != "3" || payloadAttributes.offer_channel != 3) {
			let online_assignment_query =
				`SELECT bucket.PARTY_ID	AS PARTY_ID,
				MPT.offer_mc_id_1       AS MC_UNIQUE_PROMOTION_ID,
				GETDATE()               AS ASSIGNMENT_DATETIME
				FROM [${payloadAttributes.update_contact}] AS bucket
				INNER JOIN [${marketingCloud.mobilePushMainTable}] AS MPT
				ON MPT.push_key = ${payloadAttributes.key}`

			let instore_assignment_query =
				`SELECT bucket.PARTY_ID AS PARTY_ID,
				MPT.offer_mc_id_6       AS MC_UNIQUE_PROMOTION_ID,
				GETDATE()               AS ASSIGNMENT_DATETIME
				FROM [${payloadAttributes.update_contact}] AS bucket
				INNER JOIN [${marketingCloud.mobilePushMainTable}] AS MPT
				ON MPT.push_key = ${payloadAttributes.key}`

			let assignmentQuery;
			if (payloadAttributes.promotion_type == 'online') {
				assignmentQuery = online_assignment_query
				console.dir(assignmentQuery);
			}
			else if (payloadAttributes.promotion_type == 'online_instore') {
				assignmentQuery =
					`${online_assignment_query} 
						UNION
					${instore_assignment_query}`

				console.dir(assignmentQuery);
			}
			else if (payloadAttributes.promotion_type == 'instore') {
				assignmentQuery = instore_assignment_query
				console.dir(assignmentQuery);
			}

			// create and run the voucher assignment query
			const assignmentQueryName = `IF024 Assignment - ${dateString} - ${payloadAttributes.query_name}`;
			const assignmentQueryId = await createSQLQuery(marketingCloud.assignmentKey, assignmentQuery, updateTypes.Append, marketingCloud.assignmentTableName, assignmentQueryName, `Assignment in PROMOTION_ASSIGNMENT in IF024 for ${payloadAttributes.query_name}`);
			await runSQLQuery(assignmentQueryId, assignmentQueryName);
			returnIds["assignment_query_id"] = assignmentQueryId;
		}

		let memberOfferQuery;
		let claimUniqueVoucherQuery;
		if ((payloadAttributes.promotion_type == 'online' || payloadAttributes.promotion_type == 'online_instore')
			&& payloadAttributes.online_promotion_type == 'unique') {

			// Query to handle any offers that use unique online codes
			memberOfferQuery =
				`SELECT A.SCHEME_ID,
				A.LOYALTY_CARD_NUMBER,
				A.PARTY_ID,
				A.OFFER_ID,
				ISNULL(A.EXISTING_ON_LINE_CODE, vp.CouponCode) AS VOUCHER_ON_LINE_CODE,
				A.VOUCHER_IN_STORE_CODE AS VOUCHER_IN_STORE_CODE,
				A.[VISIBLE_FROM_DATE_TIME],
				A.[START_DATE_TIME],
				A.[END_DATE_TIME],
				A.NO_REDEMPTIONS_ALLOWED,
				A.STATUS,
				A.DATE_UPDATED
				FROM (
					SELECT 'Matalan' AS SCHEME_ID,
					${appCardNumber} AS LOYALTY_CARD_NUMBER,
					PCD.PARTY_ID,
					MPT.offer_id AS OFFER_ID,
					NULLIF(MPT.offer_instore_code_1, 'no-code') AS VOUCHER_IN_STORE_CODE,
					mo.VOUCHER_ON_LINE_CODE						AS EXISTING_ON_LINE_CODE,
					CASE 	WHEN mo.[VISIBLE_FROM_DATE_TIME] <> mo.START_DATE_TIME THEN mo.[VISIBLE_FROM_DATE_TIME]  /* If a seed, keep the existing visible from datetime */
							ELSE FORMAT(${visible_from_date_time} AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss')
							END AS [VISIBLE_FROM_DATE_TIME],
					FORMAT(MPT.[offer_start_datetime] AT TIME ZONE 'GMT Standard Time' AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss')  AS [START_DATE_TIME],
					FORMAT(MPT.[offer_end_datetime] AT TIME ZONE 'GMT Standard Time' AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss')    AS [END_DATE_TIME],
					MPT.offer_redemptions                           AS NO_REDEMPTIONS_ALLOWED,
					CASE    WHEN mo.STATUS IS NULL THEN 'A'
							WHEN mo.STATUS = 'A' AND (mo.DATE_MOBILIZE_SYNC < mo.DATE_UPDATED OR mo.DATE_MOBILIZE_SYNC IS NULL) THEN 'A'
							ELSE 'C' END			    AS STATUS,
					SYSDATETIME()                       AS DATE_UPDATED,
					ROW_NUMBER() OVER (ORDER BY (SELECT NULL))      AS RN
					FROM [${payloadAttributes.update_contact}] AS UpdateContactDE
					INNER JOIN [${marketingCloud.mobilePushMainTable}] AS MPT
					ON MPT.push_key = ${payloadAttributes.key}
					INNER JOIN [${sourceDataModel}] AS PCD
					ON PCD.PARTY_ID = UpdateContactDE.PARTY_ID
					LEFT JOIN [${marketingCloud.memberOfferTableName}] AS mo
					ON  ${appCardNumber} = mo.LOYALTY_CARD_NUMBER
					AND MPT.OFFER_ID = mo.OFFER_ID
					WHERE ${appCardNumber} IS NOT NULL
				) A
				LEFT JOIN (
					SELECT  CouponCode
					,       ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RN
					FROM    [${payloadAttributes.unique_code_1}]
					WHERE   IsClaimed = 0
				) VP
				ON A.RN = VP.RN`

			claimUniqueVoucherQuery =
				`SELECT uv.CouponCode
				,       1           AS IsClaimed
				,       mo.PARTY_ID AS SubscriberKey
				,       mo.PARTY_ID AS PARTY_ID
				,       SYSDATETIMEOFFSET() AS ClaimedDate
				FROM    [${marketingCloud.memberOfferTableName}] AS mo
				INNER JOIN [${marketingCloud.mobilePushMainTable}] AS mpt
				ON      mo.OFFER_ID = mpt.OFFER_ID
				INNER JOIN [${payloadAttributes.unique_code_1}] AS uv 
				ON      mo.VOUCHER_ON_LINE_CODE = uv.CouponCode
				WHERE   mpt.push_key = ${payloadAttributes.key}
				AND 	uv.IsClaimed = 0`
		}
		else {

			// Query to handle all other member offer types - global vouchers and informational
			memberOfferQuery =
				`SELECT 'Matalan'   AS SCHEME_ID,
				${appCardNumber}    AS LOYALTY_CARD_NUMBER,
				PCD.PARTY_ID,
				MPT.offer_id        AS OFFER_ID,
				NULLIF(MPT.offer_online_code_1, 'no-code')  AS VOUCHER_ON_LINE_CODE,
				NULLIF(MPT.offer_instore_code_1, 'no-code') AS VOUCHER_IN_STORE_CODE,
				CASE 	WHEN mo.[VISIBLE_FROM_DATE_TIME] <> mo.START_DATE_TIME THEN mo.[VISIBLE_FROM_DATE_TIME]  /* If a seed, keep the existing visible from datetime */
						ELSE FORMAT(${visible_from_date_time} AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss')
						END AS [VISIBLE_FROM_DATE_TIME],
				FORMAT(MPT.[offer_start_datetime] AT TIME ZONE 'GMT Standard Time' AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss')  AS [START_DATE_TIME],
				FORMAT(MPT.[offer_end_datetime] AT TIME ZONE 'GMT Standard Time' AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss')    AS [END_DATE_TIME],
				ISNULL(MPT.offer_redemptions, 1)    AS NO_REDEMPTIONS_ALLOWED,
				CASE    WHEN mo.STATUS IS NULL THEN 'A'
						WHEN mo.STATUS = 'A' AND (mo.DATE_MOBILIZE_SYNC < mo.DATE_UPDATED OR mo.DATE_MOBILIZE_SYNC IS NULL) THEN 'A'
						ELSE 'C' END			    AS STATUS,
				SYSDATETIME()   AS DATE_UPDATED
				FROM [${payloadAttributes.update_contact}] AS UpdateContactDE
				INNER JOIN [${marketingCloud.mobilePushMainTable}] AS MPT
				ON MPT.push_key = ${payloadAttributes.key}
				INNER JOIN [${sourceDataModel}] AS PCD
				ON PCD.PARTY_ID = UpdateContactDE.PARTY_ID
				LEFT JOIN [${marketingCloud.memberOfferTableName}] AS mo
				ON  ${appCardNumber} = mo.LOYALTY_CARD_NUMBER
				AND MPT.OFFER_ID = mo.OFFER_ID
				WHERE ${appCardNumber} IS NOT NULL`;
		}

		let masterOfferQuery =
			`SELECT	'Matalan' AS SCHEME_ID,
			mpt.offer_id AS OFFER_ID,
			mpt.offer_instore_code_1 									AS VOUCHER_IN_STORE_CODE,
			mpt.offer_short_content 									AS SHORT_DESCRIPTION,
			ISNULL(mpt.offer_long_description, ' ') 		AS LONG_DESCRIPTION,
			FORMAT(MPT.offer_start_datetime AT TIME ZONE 'GMT Standard Time' AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss') AS START_DATE_TIME,
			FORMAT(MPT.offer_end_datetime AT TIME ZONE 'GMT Standard Time' AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss') AS END_DATE_TIME,
			CASE    WHEN o.STATUS IS NULL THEN 'A'
					WHEN o.STATUS = 'A' AND (o.DATE_MOBILIZE_SYNC < o.DATE_UPDATED OR o.DATE_MOBILIZE_SYNC IS NULL) THEN 'A'
					ELSE 'C' END			AS STATUS,
			mpt.offer_type 					AS OFFER_TYPE,
			mpt.offer_image_url 			AS IMAGE_URL_1,
			mpt.offer_more_info 			AS MORE_INFO_TEXT,
			mpt.offer_click_through_url 	AS ONLINE_OFFER_CLICKTHROUGH_URL,
			mpt.offer_channel 				AS OFFER_CHANNEL,
			'501' 							AS OFFER_STORES,
			mpt.offer_validity 				AS SHOW_VALIDITY,
			mpt.offer_info_button_text 		AS INFO_BUTTON_TEXT,
			SYSDATETIME()                   AS DATE_UPDATED
			FROM [${marketingCloud.mobilePushMainTable}] AS mpt
			LEFT JOIN [${marketingCloud.masterOfferTableName}] AS o
			ON      mpt.OFFER_ID = o.OFFER_ID
			WHERE   push_type = 'offer'
			AND     push_key = ${payloadAttributes.key}`

		console.dir(masterOfferQuery);
		console.dir(memberOfferQuery);

		const masterOfferQueryName = `Master Offer - ${dateString} - ${payloadAttributes.query_name}`;
		const memberOfferQueryName = `Member Offer - ${dateString} - ${payloadAttributes.query_name}`;
		const masterOfferQueryId = await createSQLQuery(marketingCloud.masterOfferKey, masterOfferQuery, updateTypes.AddUpdate, marketingCloud.masterOfferTableName, masterOfferQueryName, `Master Offer Assignment for ${payloadAttributes.query_name}`);
		const memberOfferQueryId = await createSQLQuery(marketingCloud.memberOfferKey, memberOfferQuery, updateTypes.AddUpdate, marketingCloud.memberOfferTableName, memberOfferQueryName, `Member Offer Assignment for ${payloadAttributes.query_name}`);
		await runSQLQuery(masterOfferQueryId, masterOfferQueryName);
		await runSQLQuery(memberOfferQueryId, memberOfferQueryName);

		let claimUniqueVoucherQueryId;
		if (claimUniqueVoucherQuery) {
			const voucherDeKey = await getKeyForVoucherDataExtensionByName(payloadAttributes.unique_code_1);

			const claimUniqueVoucherQueryName = `Claim Unique Voucher - ${dateString} - ${payloadAttributes.query_name}`;
			claimUniqueVoucherQueryId = await createSQLQuery(voucherDeKey, claimUniqueVoucherQuery, updateTypes.AddUpdate, payloadAttributes.unique_code_1, claimUniqueVoucherQueryName, claimUniqueVoucherQueryName);
			await runSQLQuery(claimUniqueVoucherQueryId, claimUniqueVoucherQueryName);
		}

		returnIds["master_offer_query_id"] = masterOfferQueryId;
		returnIds["member_offer_query_id"] = memberOfferQueryId;
		returnIds["claim_unique_voucher_query_id"] = claimUniqueVoucherQueryId;

	}
	else if (payloadAttributes.push_type.includes("message")) {

		let messageFinalQuery = CreatePushMessageQuery();

		console.dir(messageFinalQuery);

		const messageQueryName = `IF008 Message - ${dateString} - ${payloadAttributes.query_name}`;
		const messageQueryId = await createSQLQuery(marketingCloud.messageKey, messageFinalQuery, updateTypes.Append, marketingCloud.messageTableName, messageQueryName, `Message Assignment in IF008 for ${payloadAttributes.query_name}`);
		await runSQLQuery(messageQueryId, messageQueryName);
		returnIds["member_message_query_id"] = messageQueryId;
	}
	return returnIds;

	function CreatePushMessageQuery() {
		let messageUrl;
		if (payloadAttributes.push_type == "message_non_loyalty") {
			messageUrl = `CASE 	WHEN ISNULL(MPT.message_url, '') LIKE  'content://my_rewards%' 	THEN 'content://home'
								WHEN ISNULL(MPT.message_url, '') = ''							THEN 'content://home'
								ELSE MPT.message_url
							END`;
		}
		else {
			messageUrl = "ISNULL(NULLIF(MPT.message_url,''),'content://my_rewards/my_offers_list')";
		}

		const targetSendDateTime = "MPT.[message_target_send_datetime] AT TIME ZONE 'GMT Standard Time'";
		const seedSendDatetime = `CASE	WHEN MPT.[message_seed_send_datetime] AT TIME ZONE 'GMT Standard Time' < SYSDATETIMEOFFSET()
						THEN SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time'
					ELSE MPT.[message_seed_send_datetime] AT TIME ZONE 'GMT Standard Time'
			END`;

		let pushSeedQuery = `
			SELECT MPT.push_key,
			'Matalan'                   AS SCHEME_ID,
			PCD.MATALAN_CARD_NUMBER     AS LOYALTY_CARD_NUMBER,
			MPT.message_content         AS MESSAGE_CONTENT,
			FORMAT(${seedSendDatetime} AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss')	AS TARGET_SEND_DATE_TIME,
			'A'							AS STATUS,
			MPT.message_title           AS TITLE,
			${messageUrl}	            AS [URL]
			FROM [${marketingCloud.seedListTable}] AS UpdateContactDE
			INNER JOIN [${marketingCloud.seedListTable}] AS PCD ON PCD.PARTY_ID = UpdateContactDE.PARTY_ID
			INNER JOIN [${marketingCloud.mobilePushMainTable}] as MPT
			ON MPT.push_key = ${payloadAttributes.key}
			WHERE PCD.MATALAN_CARD_NUMBER IS NOT NULL`;

		let pushBroadcastQuery;
		if (payloadAttributes.push_type == "message_non_loyalty") {
			pushBroadcastQuery =
				`SELECT MPT.push_key,
				'Matalan'                   AS SCHEME_ID,
				'000000000000000'           AS LOYALTY_CARD_NUMBER,
				MPT.message_content         AS MESSAGE_CONTENT,
				FORMAT(${targetSendDateTime} AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss')	AS TARGET_SEND_DATE_TIME,
				'A'							AS STATUS,
				MPT.message_title           AS TITLE,
				${messageUrl}     			AS [URL]
				FROM [${marketingCloud.mobilePushMainTable}] as MPT
				WHERE MPT.push_key = ${payloadAttributes.key}`;
		}
		else {
			pushBroadcastQuery = `
				SELECT MPT.push_key,
				'Matalan'                   AS SCHEME_ID,
				PCD.APP_CARD_NUMBER            AS LOYALTY_CARD_NUMBER,
				MPT.message_content         AS MESSAGE_CONTENT,
				FORMAT(${targetSendDateTime} AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss')	AS TARGET_SEND_DATE_TIME,
				'A'							AS STATUS,
				MPT.message_title           AS TITLE,
				${messageUrl}	            AS [URL]
				FROM [${payloadAttributes.update_contact}] AS UpdateContactDE
				INNER JOIN [${sourceDataModel}] AS PCD ON PCD.PARTY_ID = UpdateContactDE.PARTY_ID
				INNER JOIN [${marketingCloud.mobilePushMainTable}] as MPT
				ON MPT.push_key = ${payloadAttributes.key}
				WHERE PCD.APP_CARD_NUMBER IS NOT NULL`;
		}

		let pushLiveSeedsQuery = `SELECT MPT.push_key,
			'Matalan'                   AS SCHEME_ID,
			S.MATALAN_CARD_NUMBER       AS LOYALTY_CARD_NUMBER,
			MPT.message_content         AS MESSAGE_CONTENT,
			FORMAT(${targetSendDateTime} AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss')	AS TARGET_SEND_DATE_TIME,
			'A'							AS STATUS,
			MPT.message_title           AS TITLE,
			${messageUrl}	            AS [URL]
			FROM [${marketingCloud.mobilePushMainTable}] AS MPT
			CROSS JOIN [${marketingCloud.seedListTable}] AS S
			WHERE MPT.push_key = ${payloadAttributes.key}
			AND   S.MATALAN_CARD_NUMBER IS NOT NULL`;

		let pushSubquery;
		if (seed) {
			pushSubquery = pushSeedQuery;
		}
		else {
			pushSubquery =
				`${pushBroadcastQuery}
						UNION
					${pushLiveSeedsQuery}`;
		}

		let messageFinalQuery = `SELECT A.push_key
			,		A.SCHEME_ID
			,       (CAST(DATEDIFF(SS,'2020-01-01',getdate()) AS bigint) * 100000) + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS MOBILE_MESSAGE_ID
			,       A.LOYALTY_CARD_NUMBER
			,       A.MESSAGE_CONTENT
			,       A.TARGET_SEND_DATE_TIME
			,       A.STATUS
			,       A.TITLE
			,       A.[URL]
			,       SYSDATETIME()   AS DATE_CREATED
			,       SYSDATETIME()   AS DATE_UPDATED
			FROM
			(
				${pushSubquery}
			) AS A`;
		return messageFinalQuery;
	}
};

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
	const updatedPushPayload = updatePushPayload(payload);
	const updatedPushObject = await updateDataExtension(updatedPushPayload, key);

	return updatedPushPayload;
}

async function buildAndSend(payload) {
	const incrementData = await getIncrements();
	const commCellIncrementData = await getCommCellIncrements();

	const pushPayload = buildPushPayload(payload, commCellIncrementData.communication_cell_code_id_increment);
	const pushObject = await saveToDataExtension(pushPayload, incrementData);

	const commPayload = buildCommPayload(pushPayload);
	const commObject = await saveToCommunicationDataExtension(commPayload, commCellIncrementData.communication_cell_code_id_increment);

	await updateIncrements(incrementData);
	await updateCommunicationCellIncrement(commCellIncrementData.communication_cell_code_id_increment);

	return pushPayload;
}

function getSfmcDatetimeNow() {
	const sfmcNow = moment().tz("Etc/GMT+6");
	return sfmcNow.format();
}

function buildPushPayload(payload, commCellKey) {
	let mobilePushData = {};
	for ( let i = 0; i < payload.length; i++ ) {
		//console.dir("Step is: " + payload[i].step + ", Key is: " + payload[i].key + ", Value is: " + payload[i].value + ", Type is: " + payload[i].type);
		mobilePushData[payload[i].key] = payload[i].value;

	}
	if ( mobilePushData["push_type"].includes('message') || mobilePushData["push_type"] == 'offer' && mobilePushData["offer_channel"] == '3' ) {
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
		//console.dir("Step is: " + payload[i].step + ", Key is: " + payload[i].key + ", Value is: " + payload[i].value + ", Type is: " + payload[i].type);
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

	await buildAndUpdate(payload, messageKeyToUpdate);
	return messageKeyToUpdate;

}

async function runSQLQuery(executeThisQueryId, queryName) {
	const sbClient = ServiceBusClient.createFromConnectionString(azureServiceBusConnectionString);
	const queueClient = sbClient.createQueueClient(azureQueueName);
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

	} catch(err) {
		console.dir(`Error occured: ${err}`);
	} finally {
		await sbClient.close();
	}
}

async function getKeyForVoucherDataExtensionByName(voucherDEName) {
	let tokenResponse = await getOauth2Token();

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
		console.dir("Error getting voucher DE Key");
		console.dir(error);
		reject(error);
	}
}

/**

POST /automation/v1/queries/{{queryID}}/actions/start/
Host: {{yourendpoint}}.rest.marketingcloudapis.com
Authorization: Bearer {{Oauth Key}}
Content-Type: application/json

*/
app.post('/run/query/:queryId', async function(req, res, next) {

	//res.send("Enviro is " + req.params.enviro + " | Interface is " + req.params.interface + " | Folder is " + req.params.folder);
	console.dir("Query ID sent from Automation Studio");
	console.dir(req.params.queryId);
	var executeThisQueryId = req.params.queryId;
	try {
		const returnQueryResponse = await runSQLQuery(executeThisQueryId);
		console.dir("The query response object is");
		console.dir(returnQueryResponse);
		res.send(JSON.stringify(returnQueryResponse));
	} catch (err) {
		console.dir(err);
		next(err);
	}
});

// insert data into mobile_push_main data extension
app.post('/dataextension/add/', async function (req, res, next){ 
	console.dir("Dump request body");
	console.dir(req.body);
	try {
		const returnedPayload = await sendBackPayload(req.body)
		res.send(JSON.stringify(returnedPayload));
	} catch(err) {
		console.dir(err);
		next(err);
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
		next(err);
	}
});

// create a SQL query activity
app.post('/automation/create/query', async function (req, res, next){ 
	console.dir("Dump request body");
	console.dir(req.body);
	try {
		const returnedQueryId = await addQueryActivity(req.body, false);
		res.send(JSON.stringify(returnedQueryId));
	} catch(err) {
		console.dir(err);
		next(err);
	}
	
});

app.post('/automation/create/query/seed', async function (req, res, next){ 
	console.dir("Dump request body");
	console.dir(req.body);
	try {
		const returnedQueryId = await addQueryActivity(req.body, true);
		res.send(JSON.stringify(returnedQueryId));
	} catch(err) {
		console.dir(err);
		next(err);
	}
	
});

// Fetch existing row from mobile push main table
app.get("/dataextension/lookup/mobilepushmain/:pushKey", (req, res, next) => {
	getOauth2Token().then((tokenResponse) => {
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
			next(error);
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
			next(error);
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
			next(error);
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
			next(error);
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
			next(error);
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