/////////////////////////////////////////////////////////////
// 1. The query to insert into party communication
////////////////////////////////////////////////////////////
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
            FROM    (${PartiesAndCards}) AS UpdateContactDE
            WHERE   LOYALTY_CARD_NUMBER IS NOT NULL
        ) AS parties
        INNER JOIN [${marketingCloud.mobilePushMainTable}] AS MPT
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
        INNER JOIN [${marketingCloud.mobilePushMainTable}] AS MPT
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
            FROM    (${PartiesAndCards}) AS UpdateContactDE
            WHERE   LOYALTY_CARD_NUMBER IS NOT NULL
        ) AS parties
        INNER JOIN [${marketingCloud.mobilePushMainTable}] AS MPT
        ON MPT.push_key = ${payloadAttributes.key}
        WHERE parties.CARD_RN = 1`

    console.dir(communicationQuery);
}

/////////////////////////////////////////////////////////////
// 2. The query for assignments
// Need an MC_UNIQUE_PROMOTION_ID
////////////////////////////////////////////////////////////
let online_assignment_query =
	`SELECT parties.PARTY_ID	AS PARTY_ID,
	MPT.offer_mc_id_1       AS MC_UNIQUE_PROMOTION_ID,
	GETDATE()               AS ASSIGNMENT_DATETIME
	FROM
	(
		SELECT  PARTY_ID
		,       LOYALTY_CARD_NUMBER
		,       ROW_NUMBER() OVER (PARTITION BY LOYALTY_CARD_NUMBER ORDER BY SEED_FLAG DESC, PARTY_ID) AS CARD_RN
		FROM    (${PartiesAndCards}) AS UpdateContactDE
		WHERE   LOYALTY_CARD_NUMBER IS NOT NULL
	) AS parties
	INNER JOIN [${marketingCloud.mobilePushMainTable}] AS MPT
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
		FROM    (${PartiesAndCards}) AS UpdateContactDE
		WHERE   LOYALTY_CARD_NUMBER IS NOT NULL
	) AS parties
	INNER JOIN [${marketingCloud.mobilePushMainTable}] AS MPT
	ON MPT.push_key = ${payloadAttributes.key}
    WHERE parties.CARD_RN = 1`
    
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
                    FROM    (${PartiesAndCards}) AS UpdateContactDE						
                    WHERE  LOYALTY_CARD_NUMBER  IS NOT NULL
                ) AS parties
                INNER JOIN [${marketingCloud.mobilePushMainTable}] AS MPT
                ON MPT.push_key = ${payloadAttributes.key}
                LEFT JOIN [${marketingCloud.memberOfferTableName}] AS mo
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
                FROM    (${PartiesAndCards}) AS UpdateContactDE
                WHERE   LOYALTY_CARD_NUMBER IS NOT NULL
            ) AS parties
            INNER JOIN [${marketingCloud.mobilePushMainTable}] AS MPT
            ON MPT.push_key = ${payloadAttributes.key}
            LEFT JOIN [${marketingCloud.memberOfferTableName}] AS mo
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
        FROM [${marketingCloud.mobilePushMainTable}] AS mpt
        LEFT JOIN [${marketingCloud.masterOfferTableName}] AS o
        ON      mpt.OFFER_ID = o.OFFER_ID
        WHERE   push_type = 'offer'
        AND     push_key = ${payloadAttributes.key}`