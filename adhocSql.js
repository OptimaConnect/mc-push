const { v4: uuidv4 } 	= require('uuid');
const salesforceApi		= require("./salesforceApi.js");

const environment = {
	seedListTable: 						process.env.seedListTable,
	partyCardDetailsTable:  			process.env.partyCardDetailsTable,
	mobilePushMainTable: 				process.env.mobilePushMainTable,
	communicationHistoryKey: 			process.env.communicationHistoryKey,
	communicationTableName: 			process.env.communicationTableName,
	assignmentKey: 						process.env.assignmentKey,
	assignmentTableName: 				process.env.assignmentTableName,
	memberOfferTableName: 				process.env.memberOfferTableName,
	masterOfferTableName: 				process.env.masterOfferTableName,
	masterOfferKey: 					process.env.masterOfferKey,
	memberOfferKey: 					process.env.memberOfferKey,
	messageKey: 						process.env.messageKey,
	messageTableName: 					process.env.messageTableName,
	partyCardDetailsTable:  			process.env.partyCardDetailsTable
};

exports.addQueryActivity = async function(payloadAttributes, seed, updateTypes){

	var returnIds = [];

	var m = new Date();
	var dateString =
		m.getUTCFullYear() +
		("0" + (m.getUTCMonth() + 1)).slice(-2) +
		("0" + m.getUTCDate()).slice(-2) +
		("0" + m.getUTCHours()).slice(-2) +
		("0" + m.getUTCMinutes()).slice(-2) +
		("0" + m.getUTCSeconds()).slice(-2);

	console.dir("The Payload Attributes");
	console.dir(payloadAttributes);
	console.dir("The Payload Attributes type is");
	console.dir(payloadAttributes.push_type);

	let partiesAndCards;
	let target_send_date_time;
	let visible_from_date_time;
	let offer_end_datetime;
	let sourceDataModel;

	if (seed) {
		payloadAttributes.query_name = payloadAttributes.query_name + " - SEEDLIST";
		partiesAndCards = `SELECT PARTY_ID, MATALAN_CARD_NUMBER AS [LOYALTY_CARD_NUMBER], 1 AS [SEED_FLAG]
							FROM [${environment.seedListTable}]`;
		target_send_date_time =
			`CASE	WHEN MPT.[message_seed_send_datetime] AT TIME ZONE 'GMT Standard Time' < SYSDATETIMEOFFSET()
						THEN SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time'
					ELSE MPT.[message_seed_send_datetime] AT TIME ZONE 'GMT Standard Time'
			END`;
		visible_from_date_time = "SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time'";
		if (payloadAttributes.offer_validity == 'true'){
			offer_end_datetime = "MPT.[offer_end_datetime] AT TIME ZONE 'GMT Standard Time'";
		} else {
			offer_end_datetime = "MPT.[offer_start_datetime] AT TIME ZONE 'GMT Standard Time'";
		}		
	} else {
		partiesAndCards = `SELECT PARTY_ID, MATALAN_CARD_NUMBER AS [LOYALTY_CARD_NUMBER], 1 AS [SEED_FLAG]
							FROM [${environment.seedListTable}]
							UNION ALL 
							SELECT UC.PARTY_ID, PCD.APP_CARD_NUMBER AS [LOYALTY_CARD_NUMBER], 0 AS [SEED_FLAG]
							FROM [${payloadAttributes.update_contact}] AS UC 
							JOIN [${environment.partyCardDetailsTable}] AS PCD 
							ON UC.PARTY_ID = PCD.PARTY_ID`;
		target_send_date_time = "MPT.[message_target_send_datetime] AT TIME ZONE 'GMT Standard Time'";
        visible_from_date_time = "MPT.[offer_start_datetime] AT TIME ZONE 'GMT Standard Time'";
		offer_end_datetime = "MPT.[offer_end_datetime] AT TIME ZONE 'GMT Standard Time'";
		sourceDataModel = environment.partyCardDetailsTable;
	}

	let communicationQuery;
	if (payloadAttributes.push_type == 'message') {

		// message data comes from message page
		communicationQuery =
			`SELECT parties.PARTY_ID,
			MPT.communication_key	AS COMMUNICATION_CELL_ID,
			CAST(${target_send_date_time} AS datetime) AS CONTACT_DATE
			FROM
			(
				SELECT  PARTY_ID
				,       LOYALTY_CARD_NUMBER
				,       ROW_NUMBER() OVER (PARTITION BY LOYALTY_CARD_NUMBER ORDER BY SEED_FLAG DESC, PARTY_ID) AS CARD_RN
				FROM    (${partiesAndCards}) AS UpdateContactDE
				WHERE   LOYALTY_CARD_NUMBER IS NOT NULL
			) AS parties
			INNER JOIN [${environment.mobilePushMainTable}] AS MPT
			ON MPT.push_key = ${payloadAttributes.key}
			WHERE parties.CARD_RN = 1`

		console.dir(communicationQuery);

	} else if (payloadAttributes.push_type == 'message_non_loyalty') {
		communicationQuery = 
			`SELECT parties.PARTY_ID,
			MPT.communication_key	AS COMMUNICATION_CELL_ID,
			CAST(${target_send_date_time} AS datetime) AS CONTACT_DATE
			FROM
			(
				SELECT  264698160 			AS PARTY_ID
				,       '000000000000000' 	AS LOYALTY_CARD_NUMBER
			) AS parties
			INNER JOIN [${environment.mobilePushMainTable}] AS MPT
			ON MPT.push_key = ${payloadAttributes.key}`

	} else if (payloadAttributes.push_type == 'offer') {
		communicationQuery =
			`SELECT parties.PARTY_ID AS PARTY_ID,
			MPT.communication_key 	AS COMMUNICATION_CELL_ID,
			CAST(${visible_from_date_time} AS datetime) AS CONTACT_DATE
			FROM
			(
				SELECT  PARTY_ID
				,       LOYALTY_CARD_NUMBER
				,       ROW_NUMBER() OVER (PARTITION BY LOYALTY_CARD_NUMBER ORDER BY SEED_FLAG DESC, PARTY_ID) AS CARD_RN
				FROM    (${partiesAndCards}) AS UpdateContactDE
				WHERE   LOYALTY_CARD_NUMBER IS NOT NULL
			) AS parties
			INNER JOIN [${environment.mobilePushMainTable}] AS MPT
			ON MPT.push_key = ${payloadAttributes.key}
			WHERE parties.CARD_RN = 1`

		console.dir(communicationQuery);
	}

	// Create and run comms history SQL - not needed for seeds
	if (!seed) {
		const communicationQueryName = `IF028 - Communication History - ${dateString} - ${payloadAttributes.query_name}`;
		const communicationQueryId = await salesforceApi.createSQLQuery(environment.communicationHistoryKey, communicationQuery, updateTypes.Append, environment.communicationTableName, communicationQueryName, `Communication Cell Assignment in IF028 for ${payloadAttributes.query_name}`);
		await salesforceApi.runSQLQuery(communicationQueryId, communicationQueryName)
		returnIds["communication_query_id"] = communicationQueryId;
	}

	// now we handle whether this is a voucher offer or a informational offer
	if (payloadAttributes.push_type == "offer") {

		if (payloadAttributes.offer_channel != "3") {
			let online_assignment_query =
				`SELECT parties.PARTY_ID	AS PARTY_ID,
				MPT.offer_mc_id_1       AS MC_UNIQUE_PROMOTION_ID,
				GETDATE()               AS ASSIGNMENT_DATETIME
				FROM
				(
					SELECT  PARTY_ID
					,       LOYALTY_CARD_NUMBER
					,       ROW_NUMBER() OVER (PARTITION BY LOYALTY_CARD_NUMBER ORDER BY SEED_FLAG DESC, PARTY_ID) AS CARD_RN
					FROM    (${partiesAndCards}) AS UpdateContactDE
					WHERE   LOYALTY_CARD_NUMBER IS NOT NULL
				) AS parties
				INNER JOIN [${environment.mobilePushMainTable}] AS MPT
				ON MPT.push_key = ${payloadAttributes.key}
				WHERE parties.CARD_RN = 1`

			let instore_assignment_query =
				`SELECT parties.PARTY_ID AS PARTY_ID,
				MPT.offer_mc_id_6       AS MC_UNIQUE_PROMOTION_ID,
				GETDATE()               AS ASSIGNMENT_DATETIME
				FROM
				(
					SELECT  PARTY_ID
					,       LOYALTY_CARD_NUMBER
					,       ROW_NUMBER() OVER (PARTITION BY LOYALTY_CARD_NUMBER ORDER BY SEED_FLAG DESC, PARTY_ID) AS CARD_RN
					FROM    (${partiesAndCards}) AS UpdateContactDE
					WHERE   LOYALTY_CARD_NUMBER IS NOT NULL
				) AS parties
				INNER JOIN [${environment.mobilePushMainTable}] AS MPT
				ON MPT.push_key = ${payloadAttributes.key}
				WHERE parties.CARD_RN = 1`

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
			const assignmentQueryId = await salesforceApi.createSQLQuery(environment.assignmentKey, assignmentQuery, updateTypes.Append, environment.assignmentTableName, assignmentQueryName, `Assignment in PROMOTION_ASSIGNMENT in IF024 for ${payloadAttributes.query_name}`);
			await salesforceApi.runSQLQuery(assignmentQueryId, assignmentQueryName);
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
					parties.LOYALTY_CARD_NUMBER,
					parties.PARTY_ID,
					MPT.offer_id AS OFFER_ID,
					NULLIF(MPT.offer_instore_code_1, 'no-code') AS VOUCHER_IN_STORE_CODE,
					mo.VOUCHER_ON_LINE_CODE						AS EXISTING_ON_LINE_CODE,
					CASE 	WHEN mo.[VISIBLE_FROM_DATE_TIME] <> mo.START_DATE_TIME THEN mo.[VISIBLE_FROM_DATE_TIME]  /* If a seed, keep the existing visible from datetime */
							ELSE FORMAT(${visible_from_date_time} AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss')
							END AS [VISIBLE_FROM_DATE_TIME],
					FORMAT(MPT.[offer_start_datetime] AT TIME ZONE 'GMT Standard Time' AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss')  AS [START_DATE_TIME],
					FORMAT(${offer_end_datetime} AT TIME ZONE 'GMT Standard Time' AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss')    AS [END_DATE_TIME],
					MPT.offer_redemptions                           AS NO_REDEMPTIONS_ALLOWED,
					CASE    WHEN mo.STATUS IS NULL THEN 'A'
							WHEN mo.STATUS = 'A' AND (mo.DATE_MOBILIZE_SYNC < mo.DATE_UPDATED OR mo.DATE_MOBILIZE_SYNC IS NULL) THEN 'A'
							ELSE 'C' END			    AS STATUS,
					SYSDATETIME()                       AS DATE_UPDATED,
					ROW_NUMBER() OVER (ORDER BY (SELECT NULL))      AS RN
					FROM 
					(
						SELECT  PARTY_ID
						,       LOYALTY_CARD_NUMBER
						,       ROW_NUMBER() OVER (PARTITION BY LOYALTY_CARD_NUMBER ORDER BY SEED_FLAG DESC, PARTY_ID) AS CARD_RN
						FROM    (${partiesAndCards}) AS UpdateContactDE						
						WHERE  LOYALTY_CARD_NUMBER  IS NOT NULL
					) AS parties
					INNER JOIN [${environment.mobilePushMainTable}] AS MPT
					ON MPT.push_key = ${payloadAttributes.key}
					LEFT JOIN [${environment.memberOfferTableName}] AS mo
					ON  parties.LOYALTY_CARD_NUMBER = mo.LOYALTY_CARD_NUMBER
					AND MPT.OFFER_ID = mo.OFFER_ID
					WHERE parties.CARD_RN = 1
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
				FROM    [${environment.memberOfferTableName}] AS mo
				INNER JOIN [${environment.mobilePushMainTable}] AS mpt
				ON      mo.OFFER_ID = mpt.OFFER_ID
				INNER JOIN [${payloadAttributes.unique_code_1}] AS uv 
				ON      mo.VOUCHER_ON_LINE_CODE = uv.CouponCode
				WHERE   mpt.push_key = ${payloadAttributes.key}
				AND 	uv.IsClaimed = 0`
		}
		else {

			// Query to handle all other member offer types - global vouchers and informational
			memberOfferQuery =
				`SELECT 'Matalan'                AS SCHEME_ID,
				parties.LOYALTY_CARD_NUMBER     AS LOYALTY_CARD_NUMBER,
				parties.PARTY_ID,
				MPT.offer_id        AS OFFER_ID,
				NULLIF(MPT.offer_online_code_1, 'no-code')  AS VOUCHER_ON_LINE_CODE,
				NULLIF(MPT.offer_instore_code_1, 'no-code') AS VOUCHER_IN_STORE_CODE,
				CASE 	WHEN mo.[VISIBLE_FROM_DATE_TIME] <> mo.START_DATE_TIME THEN mo.[VISIBLE_FROM_DATE_TIME]  /* If a seed, keep the existing visible from datetime */
						ELSE FORMAT(${visible_from_date_time} AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss')
						END AS [VISIBLE_FROM_DATE_TIME],
				FORMAT(MPT.[offer_start_datetime] AT TIME ZONE 'GMT Standard Time' AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss')  AS [START_DATE_TIME],
				FORMAT(${offer_end_datetime} AT TIME ZONE 'GMT Standard Time' AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss')    AS [END_DATE_TIME],
				ISNULL(MPT.offer_redemptions, 1)    AS NO_REDEMPTIONS_ALLOWED,
				CASE    WHEN mo.STATUS IS NULL THEN 'A'
						WHEN mo.STATUS = 'A' AND (mo.DATE_MOBILIZE_SYNC < mo.DATE_UPDATED OR mo.DATE_MOBILIZE_SYNC IS NULL) THEN 'A'
						ELSE 'C' END			    AS STATUS,
				SYSDATETIME()   AS DATE_UPDATED
				FROM
				(
					SELECT  PARTY_ID
					,       LOYALTY_CARD_NUMBER
					,       ROW_NUMBER() OVER (PARTITION BY LOYALTY_CARD_NUMBER ORDER BY SEED_FLAG DESC, PARTY_ID) AS CARD_RN
					FROM    (${partiesAndCards}) AS UpdateContactDE
					WHERE   LOYALTY_CARD_NUMBER IS NOT NULL
				) AS parties
				INNER JOIN [${environment.mobilePushMainTable}] AS MPT
				ON MPT.push_key = ${payloadAttributes.key}
				LEFT JOIN [${environment.memberOfferTableName}] AS mo
				ON  parties.LOYALTY_CARD_NUMBER = mo.LOYALTY_CARD_NUMBER
				AND MPT.OFFER_ID = mo.OFFER_ID
				WHERE parties.CARD_RN = 1`;
		}

		let masterOfferQuery =
			`SELECT	'Matalan' AS SCHEME_ID,
			mpt.offer_id AS OFFER_ID,
			mpt.offer_instore_code_1 									AS VOUCHER_IN_STORE_CODE,
			mpt.offer_short_content 									AS SHORT_DESCRIPTION,
			ISNULL(NULLIF(mpt.offer_long_description, ''), ' ') 		AS LONG_DESCRIPTION,
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
			FROM [${environment.mobilePushMainTable}] AS mpt
			LEFT JOIN [${environment.masterOfferTableName}] AS o
			ON      mpt.OFFER_ID = o.OFFER_ID
			WHERE   push_type = 'offer'
			AND     push_key = ${payloadAttributes.key}`

		console.dir(masterOfferQuery);
		console.dir(memberOfferQuery);

		const masterOfferQueryName = `Master Offer - ${dateString} - ${payloadAttributes.query_name}`;
		const memberOfferQueryName = `Member Offer - ${dateString} - ${payloadAttributes.query_name}`;
		const masterOfferQueryId = await salesforceApi.createSQLQuery(environment.masterOfferKey, masterOfferQuery, updateTypes.AddUpdate, environment.masterOfferTableName, masterOfferQueryName, `Master Offer Assignment for ${payloadAttributes.query_name}`);
		const memberOfferQueryId = await salesforceApi.createSQLQuery(environment.memberOfferKey, memberOfferQuery, updateTypes.AddUpdate, environment.memberOfferTableName, memberOfferQueryName, `Member Offer Assignment for ${payloadAttributes.query_name}`);
		await salesforceApi.runSQLQuery(masterOfferQueryId, masterOfferQueryName);
		await salesforceApi.runSQLQuery(memberOfferQueryId, memberOfferQueryName);

		let claimUniqueVoucherQueryId;
		if (claimUniqueVoucherQuery) {
			const voucherDeKey = await getKeyForVoucherDataExtensionByName(payloadAttributes.unique_code_1);

			const claimUniqueVoucherQueryName = `Claim Unique Voucher - ${dateString} - ${payloadAttributes.query_name}`;
			claimUniqueVoucherQueryId = await salesforceApi.createSQLQuery(voucherDeKey, claimUniqueVoucherQuery, updateTypes.AddUpdate, payloadAttributes.unique_code_1, claimUniqueVoucherQueryName, claimUniqueVoucherQueryName);
			await salesforceApi.runSQLQuery(claimUniqueVoucherQueryId, claimUniqueVoucherQueryName);
		}

		returnIds["master_offer_query_id"] = masterOfferQueryId;
		returnIds["member_offer_query_id"] = memberOfferQueryId;
		returnIds["claim_unique_voucher_query_id"] = claimUniqueVoucherQueryId;

	}
	else if (payloadAttributes.push_type.includes("message")) {

		let messageFinalQuery = CreatePushMessageQuery(payloadAttributes);

		console.dir(messageFinalQuery);

		const messageQueryName = `IF008 Message - ${dateString} - ${payloadAttributes.query_name}`;
		const messageQueryId = await salesforceApi.createSQLQuery(environment.messageKey, messageFinalQuery, updateTypes.Append, environment.messageTableName, messageQueryName, `Message Assignment in IF008 for ${payloadAttributes.query_name}`);
		await salesforceApi.runSQLQuery(messageQueryId, messageQueryName);
		returnIds["member_message_query_id"] = messageQueryId;
	}
    return returnIds;
}

function CreatePushMessageQuery(payloadAttributes) {
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
        FROM [${environment.seedListTable}] AS PCD
        INNER JOIN [${environment.mobilePushMainTable}] as MPT
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
            FROM [${environment.mobilePushMainTable}] as MPT
            WHERE MPT.push_key = ${payloadAttributes.key}`;
    }
    else {
        pushBroadcastQuery = `
            SELECT MPT.push_key,
            'Matalan'                   AS SCHEME_ID,
            parties.LOYALTY_CARD_NUMBER,
            MPT.message_content         AS MESSAGE_CONTENT,
            FORMAT(${targetSendDateTime} AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss')	AS TARGET_SEND_DATE_TIME,
            'A'							AS STATUS,
            MPT.message_title           AS TITLE,
            ${messageUrl}	            AS [URL]
            FROM
            (
                SELECT  PCD.PARTY_ID
                ,       PCD.APP_CARD_NUMBER AS LOYALTY_CARD_NUMBER
                ,       ROW_NUMBER() OVER (PARTITION BY PCD.APP_CARD_NUMBER ORDER BY PCD.PARTY_ID) AS CARD_RN
                FROM    [${payloadAttributes.update_contact}] AS UpdateContactDE
                INNER JOIN [${environment.partyCardDetailsTable}] AS PCD
                ON      PCD.PARTY_ID = UpdateContactDE.PARTY_ID
                WHERE   PCD.APP_CARD_NUMBER IS NOT NULL
            ) AS parties
            INNER JOIN [${environment.mobilePushMainTable}] as MPT
            ON MPT.push_key = ${payloadAttributes.key}
            WHERE parties.CARD_RN = 1`;
    }

    let pushLiveSeedsQuery = `SELECT MPT.push_key,
        'Matalan'                   AS SCHEME_ID,
        S.MATALAN_CARD_NUMBER       AS LOYALTY_CARD_NUMBER,
        MPT.message_content         AS MESSAGE_CONTENT,
        FORMAT(${targetSendDateTime} AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss')	AS TARGET_SEND_DATE_TIME,
        'A'							AS STATUS,
        MPT.message_title           AS TITLE,
        ${messageUrl}	            AS [URL]
        FROM [${environment.mobilePushMainTable}] AS MPT
        CROSS JOIN [${environment.seedListTable}] AS S
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