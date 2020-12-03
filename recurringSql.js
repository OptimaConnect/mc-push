const { v4: uuidv4 } 	= require('uuid');
const salesforceApi		= require("./salesforceApi.js");

exports.recurringCamapign = async function(marketingCloud, payloadAttributes){
	const updateTypes = {
		Overwrite: 0,
		AddUpdate: 1,
		Append: 2
	};

	const m = new Date();
	const dateString =
	m.getUTCFullYear() +
	("0" + (m.getUTCMonth() + 1)).slice(-2) +
	("0" + m.getUTCDate()).slice(-2) +
	("0" + m.getUTCHours()).slice(-2) +
	("0" + m.getUTCMinutes()).slice(-2) +
	("0" + m.getUTCSeconds()).slice(-2);

	// overwrite staging communication cell
	const stagingCommunicationCellQuery = `select  pi.communication_cell_code_id_increment + 1 AS COMMUNICATION_CELL_ID
	,   mpmt.OFFER_CAMPAIGN_CODE AS CAMPAIGN_CODE
	,   mpmt.OFFER_CELL_CODE AS CELL_CODE
	,   mpmt.OFFER_CELL_NAME AS CELL_NAME
	,   mpmt.OFFER_CAMPAIGN_NAME AS CAMPAIGN_NAME
	,   mpmt.OFFER_CAMPAIGN_ID AS CAMPAIGN_ID
	,   1 AS CELL_TYPE
	,   CASE WHEN mpmt.PUSH_TYPE = 'offer' THEN 5 ELSE 6 END AS CHANNEL
	,   1 AS IS_PUTPUT_FLAG
	,   DATEADD(DAY, mpmt.RECURRING_OFFER_DELAY_DAYS, CAST(GETUTCDATE() AS DATE)) AS BASE_CONTACT_DATE
	from [${marketingCloud.mobilePushMainTable}] mpmt
	CROSS JOIN [${marketingCloud.promotionIncrementsName}] pi
	where mpmt.push_key = ${payloadAttributes.key}`;

	const stagingCommunicationCellQueryName = `${payloadAttributes.query_name} - staging comm cell - ${dateString}`;
	const stagingCommunicationCellQueryId = await salesforceApi.createSQLQuery(marketingCloud.stagingCommunicationCellId
													, stagingCommunicationCellQuery
													, updateTypes.Overwrite
													, marketingCloud.stagingCommunicationCellName
													, stagingCommunicationCellQueryName
													, `${payloadAttributes.query_name} - staging comm cell`);

	// overwirte staging vouceher subsets
	const stagingVoucherSubsetsQuery = `SELECT	vsubset.PUSH_KEY
	,	vsubset.VOUCHER_SUBSET_ID
	,	vsubset.BARCODE_FLAG
	FROM
	(
		SELECT	vset.PUSH_KEY
			,	vss1.VOUCHER_SUBSET_ID
			,	ROW_NUMBER() OVER (PARTITION BY vss1.BARCODE_FLAG ORDER BY vss1.VOUCHER_SUBSET_ID) AS rn
			,	vss1.BARCODE_FLAG
		FROM
		(
			SELECT	TOP 1 vs.VOUCHER_SET_ID
				,	mpmt.PUSH_KEY
			FROM	[${marketingCloud.voucherSetName}] vs 
			JOIN 	[${marketingCloud.mobilePushMainTable}] mpmt
			ON		vs.VOUCHER_GROUP_ID = mpmt.RECURRING_VOUCHER_GROUP_ID 
			WHERE	mpmt.push_key = ${payloadAttributes.key}
			AND		vs.ALLOCATION_DATE <= DATEADD(DAY, mpmt.RECURRING_OFFER_DELAY_DAYS, CAST(GETUTCDATE() AS DATE))
			ORDER BY vs.ALLOCATION_DATE DESC
		) vset
		JOIN [${marketingCloud.voucherSubsetName}] vss1
		ON 	vset.VOUCHER_SET_ID = vss1.VOUCHER_SET_ID
	) AS vsubset
	WHERE	vsubset.rn = 1`;

	const stagingVoucherSubsetsQueryName = `${payloadAttributes.query_name} - staging voucher subsets - ${dateString}`;
	const stagingVoucherSubsetsQueryId = await salesforceApi.createSQLQuery(marketingCloud.stagingCommunicationCellId
													, stagingVoucherSubsetsQuery
													, updateTypes.Overwrite
													, marketingCloud.recurringVoucherSubsetsName
													, stagingVoucherSubsetsQueryName
													, `${payloadAttributes.query_name} - staging voucher subsets`);

	// overwrite staging promotion desc
	const stagingPromoDescriptionQuery = `SELECT	pi.mc_unique_promotion_id_increment + 1 AS MC_UNIQUE_PROMOTION_ID
	,	vss.PROMOTION_ID
	,	vss.PROMOTION_ID AS PROMOTION_GROUP_ID
	,	mpmt.offer_redemptions AS NUMBER_OF_REDEMPTIONS_ALLOWED
	,	0 AS PRINT_AT_TILL_FLAG
	,	0 AS INSTANT_WIN_FLAG
	,	DATEADD(DAY,mpmt.RECURRING_OFFER_DELAY_DAYS, CAST(GETUTCDATE() AS DATE))AS [VISIBLE_FROM_DATETIME]
	,	DATEADD(DAY,mpmt.RECURRING_OFFER_DELAY_DAYS + mpmt.RECURRING_OFFER_VALIDITY_DAYS, CAST(GETUTCDATE() AS DATE)) AS [VISIBLE_TO_DATETIME]
	,	vss.[VALID_FROM_DATE] AS [VALID_FROM_DATETIME]
	,	vss.[VALID_TO_DATE] AS [VALID_TO_DATETIME]
	,	vss.VOUCHER_SUBSET_DESCRIPTION AS OFFER_DESCRIPTION
	,	'Online' AS OFFER_CHANNEL
	,	'APP' AS OFFER_MEDIUM
	,	scc.COMMUNICATION_CELL_ID
	,	'' AS BARCODE
	FROM	[${marketingCloud.recurringVoucherSubsetsName}] arvs
	JOIN 	[${marketingCloud.voucherSubsetName}] vss
	ON		arvs.VOUCHER_SUBSET_ID = vss.VOUCHER_SUBSET_ID
	CROSS JOIN	[${marketingCloud.stagingCommunicationCellName}] scc
	CROSS JOIN	[${marketingCloud.promotionIncrementsName}] pi
	JOIN 	[${marketingCloud.mobilePushMainTable}] mpmt	
	ON		mpmt.PUSH_KEY = ${payloadAttributes.key}
	WHERE	arvs.BARCODE_FLAG = 0
	UNION ALL 
	SELECT	pi.mc_unique_promotion_id_increment + 2 AS MC_UNIQUE_PROMOTION_ID
	,	vss.BARCODE_REDEEMING_ID AS PROMOTION_ID
	,	vss.BARCODE_REDEEMING_ID AS PROMOTION_GROUP_ID
	,	mpmt.offer_redemptions AS NUMBER_OF_REDEMPTIONS_ALLOWED
	,	0 AS PRINT_AT_TILL_FLAG
	,	0 AS INSTANT_WIN_FLAG
	,	DATEADD(DAY,mpmt.RECURRING_OFFER_DELAY_DAYS, CAST(GETUTCDATE() AS DATE))AS [VISIBLE_FROM_DATETIME]
	,	DATEADD(DAY,mpmt.RECURRING_OFFER_DELAY_DAYS + mpmt.RECURRING_OFFER_VALIDITY_DAYS, CAST(GETUTCDATE() AS DATE)) AS [VISIBLE_TO_DATETIME]
	,	vss.[VALID_FROM_DATE] AS [VALID_FROM_DATETIME]
	,	vss.[VALID_TO_DATE] AS [VALID_TO_DATETIME]
	,	vss.VOUCHER_SUBSET_DESCRIPTION AS OFFER_DESCRIPTION
	,	'Online' AS OFFER_CHANNEL
	,	'APP' AS OFFER_MEDIUM
	,	scc.COMMUNICATION_CELL_ID
	,	gv.GLOBAL_VOUCHER_CODE AS BARCODE
	FROM	[${marketingCloud.recurringVoucherSubsetsName}] arvs
	JOIN 	[${marketingCloud.voucherSubsetName}] vss
	ON		arvs.VOUCHER_SUBSET_ID = vss.VOUCHER_SUBSET_ID
	LEFT JOIN 	[${marketingCloud.globalVoucherName}] gv
	ON		vss.VOUCHER_SUBSET_ID = gv.VOUCHER_SUBSET_ID
	CROSS JOIN	[${marketingCloud.stagingCommunicationCellName}] scc
	CROSS JOIN	[${marketingCloud.promotionIncrementsName}] pi
	JOIN 	[${marketingCloud.mobilePushMainTable}] mpmt	
	ON		mpmt.PUSH_KEY = ${payloadAttributes.key}
	WHERE	arvs.BARCODE_FLAG = 1`;

	const stagingPromoDescriptionQueryName = `${payloadAttributes.query_name} - staging promo desc - ${dateString}`;
	const stagingPromoDescriptionQueryId = await salesforceApi.createSQLQuery(marketingCloud.stagingPromotionDescriptionId
													, stagingPromoDescriptionQuery
													, updateTypes.Overwrite
													, marketingCloud.stagingPromotionDescriptionName
													, stagingPromoDescriptionQueryName
													, `${payloadAttributes.query_name} - staging promo desc`);

	//from staging into comm cell and promo desc
	const appendCommunicationCellQuery = `SELECT	*
	FROM	[${marketingCloud.stagingCommunicationCellName}]`;

	const appendCommunicationCellQueryName = `${payloadAttributes.query_name} - append comm cell - ${dateString}`;
	const appendCommunicationCellQueryId = await salesforceApi.createSQLQuery(marketingCloud.communicationCellDataExtension
													, appendCommunicationCellQuery
													, updateTypes.Append
													, marketingCloud.communicationCellName
													, appendCommunicationCellQueryName
													, `${payloadAttributes.query_name} - append comm cell`);

	const appendPromoDescriptionQuery = `SELECT	*
	FROM	[${marketingCloud.stagingPromotionDescriptionName}]`;

	const appendPromoDescriptionQueryName = `${payloadAttributes.query_name} - append promo desc - ${dateString}`;
	const appendPromoDescriptionQueryId = await salesforceApi.createSQLQuery(marketingCloud.promotionDescriptionId
													, appendPromoDescriptionQuery
													, updateTypes.Append
													, marketingCloud.promotionDescriptionTable
													, appendPromoDescriptionQueryName
													, `${payloadAttributes.query_name} - append promo desc`);

	//update increments
	const incrementUpdateQuery = `SELECT	1 AS INCREMENT_KEY
		,	newcommid AS communication_cell_code_id_increment
		,	newmcid AS mc_unique_promotion_id_increment
	FROM
	(
		SELECT MAX(cc_id) + 1 AS newcommid
		FROM
		(
			SELECT MAX(CAST(COMMUNICATION_CELL_ID AS INT)) AS cc_id
			FROM [${marketingCloud.communicationCellName}]  
			UNION ALL 
			SELECT communication_cell_code_id_increment AS cc_id
			FROM [${marketingCloud.promotionIncrementsName}]
		) AS comm
	) AS maxcomm
	CROSS JOIN 
	(
		SELECT MAX(mc_id) + 1 AS newmcid
		FROM
		(
			SELECT MAX(CAST(MC_UNIQUE_PROMOTION_ID AS INT)) AS mc_id
			FROM [${marketingCloud.promotionDescriptionTable}]
			UNION ALL 
			SELECT mc_unique_promotion_id_increment AS mc_id
			FROM [${marketingCloud.promotionIncrementsName}]
		) AS promo
	) AS maxpromo`;

	const incrementUpdateQueryName = `${payloadAttributes.query_name} - update increments - ${dateString}`;
	const incrementUpdateQueryId = await salesforceApi.createSQLQuery(marketingCloud.commCellIncrementDataExtension
													, incrementUpdateQuery
													, updateTypes.AddUpdate
													, marketingCloud.promotionIncrementsName
													, incrementUpdateQueryName
													, `${payloadAttributes.query_name} - update increments`);

	//Populate APP_RECURRING_CAMPAIGNS (Append)
	const recurringCampaignQuery = `SELECT	scc.CAMPAIGN_CODE
		,	scc.CELL_CODE AS CAMPAIGN_CELL_CODE
		,	CAST(GETUTCDATE() AS DATE) AS SELECTION_DATE
		,	CAST(DATEADD(DAY,mpmt.RECURRING_OFFER_DELAY_DAYS, CAST(GETUTCDATE() AS DATE)) AS DATETIME) + CAST(mpmt.RECURRING_OFFER_TIME AS DATETIME) AS [VALID_FROM_TIME]
		,	CAST(DATEADD(DAY,mpmt.RECURRING_OFFER_DELAY_DAYS + mpmt.RECURRING_OFFER_VALIDITY_DAYS, CAST(GETUTCDATE() AS DATE))AS DATETIME) + CAST('23:59' AS DATETIME) AS [VALID_TO_TIME]
		,	storevss.VOUCHER_SUBSET_ID AS INSTORE_VSS_ID
		,	onlinevss.VOUCHER_SUBSET_ID AS ONLINE_VSS_ID
		,	storespd.PROMOTION_ID AS INSTORE_PROMOTION_ID
		,	onlinespd.PROMOTION_ID AS ONLINE_PROMOTION_ID
		,	storespd.MC_UNIQUE_PROMOTION_ID AS INSTORE_MC_ID
		,	onlinespd.MC_UNIQUE_PROMOTION_ID AS ONLINE_MC_ID
		,	scc.COMMUNICATION_CELL_ID
		,	GETUTCDATE() AS LAST_UPDATE_AUDIT
		,	mpmt.PUSH_KEY
	FROM	[${marketingCloud.mobilePushMainTable}] mpmt	
	LEFT JOIN
	(
		SELECT	arvs1.VOUCHER_SUBSET_ID AS ONLINE_VOUCHER_SUBSET_ID
			,	arvs2.VOUCHER_SUBSET_ID AS INSTORE_VOUCHER_SUBSET_ID
		FROM	
		(
			SELECT	VOUCHER_SUBSET_ID
			FROM	[${marketingCloud.recurringVoucherSubsetsName}]
			WHERE	BARCODE_FLAG = 0
		) AS arvs1
		FULL JOIN 
		(
			SELECT	VOUCHER_SUBSET_ID
			FROM	[${marketingCloud.recurringVoucherSubsetsName}]
			WHERE	BARCODE_FLAG = 1
		) AS arvs2
		ON		1 = 1
	) vsubset
	ON			1 = 1
	LEFT JOIN	[${marketingCloud.voucherSubsetName}] onlinevss
	ON			vsubset.ONLINE_VOUCHER_SUBSET_ID = onlinevss.VOUCHER_SUBSET_ID
	LEFT JOIN	[${marketingCloud.voucherSubsetName}] storevss
	ON			vsubset.INSTORE_VOUCHER_SUBSET_ID = storevss.VOUCHER_SUBSET_ID
	CROSS JOIN	[${marketingCloud.stagingCommunicationCellName}] scc
	LEFT JOIN	[${marketingCloud.stagingPromotionDescriptionName}] onlinespd
	ON			scc.COMMUNICATION_CELL_ID = onlinespd.COMMUNICATION_CELL_ID
	AND			onlinevss.PROMOTION_ID = onlinespd.PROMOTION_ID
	LEFT JOIN	[${marketingCloud.stagingPromotionDescriptionName}] storespd
	ON			scc.COMMUNICATION_CELL_ID = storespd.COMMUNICATION_CELL_ID
	AND			storevss.BARCODE_REDEEMING_ID = storespd.PROMOTION_ID`;

	const recurringCampaignQueryName = `${payloadAttributes.query_name} - add to recurring campaigns - ${dateString}`;
	const recurringCampaignQueryId = await salesforceApi.createSQLQuery(marketingCloud.recurringCampaignsId
													, recurringCampaignQuery
													, updateTypes.Append
													, marketingCloud.recurringCampaignsName
													, recurringCampaignQueryName
													, `${payloadAttributes.query_name} - add to recurring campaigns`);

	//Populate PARTY_COMMUNICATION_HISTORY (Append)
	const partyCommunicationQuery = `SELECT	parties.PARTY_ID
		,	arc.COMMUNICATION_CELL_ID
		,	arc.[VALID_FROM_TIME] AS CONTACT_DATE
	FROM
	(
		SELECT	PARTY_ID
		,		LOYALTY_CARD_NUMBER
		,		ROW_NUMBER() OVER (PARTITION BY LOYALTY_CARD_NUMBER ORDER BY SEED_FLAG DESC, PARTY_ID) AS CARD_RN
		FROM
		(
			SELECT PARTY_ID, MATALAN_CARD_NUMBER AS [LOYALTY_CARD_NUMBER], 1 AS [SEED_FLAG]
			FROM [${marketingCloud.seedListTable}]
			UNION ALL 
			SELECT UC.PARTY_ID, PCD.APP_CARD_NUMBER AS [LOYALTY_CARD_NUMBER], 0 AS [SEED_FLAG]
			FROM [${payloadAttributes.update_contact}]  AS UC 
			JOIN [${marketingCloud.partyCardDetailsTable}] AS PCD
			ON UC.PARTY_ID = PCD.PARTY_ID
		) AS UpdateContactDE
		WHERE	UpdateContactDE.LOYALTY_CARD_NUMBER IS NOT NULL
	) AS parties
	JOIN [${marketingCloud.recurringCampaignsName}] AS arc
	ON		arc.push_key = ${payloadAttributes.key}
	WHERE	arc.SELECTION_DATE = CAST(GETUTCDATE() AS DATE)
	AND		parties.CARD_RN = 1`;

	const partyCommunicationQueryName = `${payloadAttributes.query_name} - add to party communication - ${dateString}`;
	const partyCommunicationQueryId = await salesforceApi.createSQLQuery(marketingCloud.communicationHistoryKey
													, partyCommunicationQuery
													, updateTypes.Append
													, marketingCloud.communicationTableName
													, partyCommunicationQueryName
													, `${payloadAttributes.query_name} - add to party communication`);

	//Populate Promotion Assignemnt (Append)
	const assignmentQuery = `SELECT	parties.PARTY_ID
	,	arc.MC_ID	AS MC_UNIQUE_PROMOTION_ID
	,	GETDATE()	AS ASSIGNMENT_DATETIME
	FROM
	(
		SELECT	PARTY_ID
		,		LOYALTY_CARD_NUMBER
		,		ROW_NUMBER() OVER (PARTITION BY LOYALTY_CARD_NUMBER ORDER BY SEED_FLAG DESC, PARTY_ID) AS CARD_RN
		FROM
		(
			SELECT PARTY_ID, MATALAN_CARD_NUMBER AS [LOYALTY_CARD_NUMBER], 1 AS [SEED_FLAG]
			FROM [${marketingCloud.seedListTable}]
			UNION ALL 
			SELECT UC.PARTY_ID, PCD.APP_CARD_NUMBER AS [LOYALTY_CARD_NUMBER], 0 AS [SEED_FLAG]
			FROM [${payloadAttributes.update_contact}] AS UC 
			JOIN [${marketingCloud.partyCardDetailsTable}] AS PCD 
			ON UC.PARTY_ID = PCD.PARTY_ID
		) AS UpdateContactDE
		WHERE   LOYALTY_CARD_NUMBER IS NOT NULL
	) AS parties
	CROSS JOIN 
	(
		SELECT	INSTORE_MC_ID AS MC_ID
		FROM	[${marketingCloud.recurringCampaignsName}]
		WHERE	push_key = ${payloadAttributes.key}
		AND		INSTORE_MC_ID IS NOT NULL
		AND		SELECTION_DATE = CAST(GETUTCDATE() AS DATE)
		UNION ALL 
		SELECT	ONLINE_MC_ID AS MC_ID
		FROM	[${marketingCloud.recurringCampaignsName}]
		WHERE	push_key = ${payloadAttributes.key}
		AND		ONLINE_MC_ID IS NOT NULL
		AND		SELECTION_DATE = CAST(GETUTCDATE() AS DATE)
	) AS arc
	WHERE parties.CARD_RN = 1`;

	const assignmentQueryName = `${payloadAttributes.query_name} - add to promo assignment - ${dateString}`;
	const assignmentQueryId = await salesforceApi.createSQLQuery(marketingCloud.assignmentKey
													, assignmentQuery
													, updateTypes.Append
													, marketingCloud.assignmentTableName
													, assignmentQueryName
													, `${payloadAttributes.query_name} - add to promo assignment`);

	//master offer query
	const masterOfferQuery = `SELECT	gv.GLOBAL_VOUCHER_CODE 									AS VOUCHER_IN_STORE_CODE
		,	ISNULL(mo.START_DATE_TIME, FORMAT(arc.[VALID_FROM_TIME] AT TIME ZONE 'GMT Standard Time' AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss')) AS START_DATE_TIME
		,	FORMAT(arc.VALID_TO_TIME AT TIME ZONE 'GMT Standard Time' AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss') AS END_DATE_TIME
		,	CASE    WHEN o.STATUS IS NULL THEN 'A'
					WHEN o.STATUS = 'A' AND (o.DATE_MOBILIZE_SYNC < o.DATE_UPDATED OR o.DATE_MOBILIZE_SYNC IS NULL) THEN 'A'
					ELSE 'C' END			AS STATUS
		,	SYSDATETIME()                   AS DATE_UPDATED
		,	'Matalan' AS SCHEME_ID
		,	mpt.offer_id AS OFFER_ID
		,	mpt.offer_short_content 									AS SHORT_DESCRIPTION
		,	ISNULL(NULLIF(mpt.offer_long_description, ''), ' ') 		AS LONG_DESCRIPTION
		,	mpt.offer_type 					AS OFFER_TYPE
		,	mpt.offer_image_url 			AS IMAGE_URL_1
		,	mpt.offer_more_info 			AS MORE_INFO_TEXT
		,	mpt.offer_click_through_url 	AS ONLINE_OFFER_CLICKTHROUGH_URL
		,	mpt.offer_channel 				AS OFFER_CHANNEL
		,	'501' 							AS OFFER_STORES
		,	mpt.offer_validity 				AS SHOW_VALIDITY
		,	mpt.offer_info_button_text 		AS INFO_BUTTON_TEXT
	FROM	[${marketingCloud.mobilePushMainTable}] AS mpt
	LEFT JOIN [${marketingCloud.masterOfferTableName}]  AS o 
	ON      mpt.OFFER_ID = o.OFFER_ID
	JOIN 	[${marketingCloud.recurringCampaignsName}] arc
	ON 		arc.PUSH_KEY = mpt.PUSH_KEY
	AND		arc.SELECTION_DATE = CAST(GETUTCDATE() AS DATE)
	LEFT JOIN [${marketingCloud.globalVoucherName}] gv
	ON		arc.INSTORE_VSS_ID = gv.VOUCHER_SUBSET_ID
	WHERE	mpt.PUSH_KEY = ${payloadAttributes.key}`;

	const masterOfferQueryName = `${payloadAttributes.query_name} - master offer - ${dateString}`;
	const masterOfferQueryId = await salesforceApi.createSQLQuery(marketingCloud.masterOfferKey
													, masterOfferQuery
													, updateTypes.AddUpdate
													, marketingCloud.masterOfferTableName
													, masterOfferQueryName
													, `${payloadAttributes.query_name} - master offer`);

	//Member Offer Query Part 1
	const memberQueryPart1 = `SELECT	'Matalan' AS SCHEME_ID
	,	parties.LOYALTY_CARD_NUMBER
	,	parties.PARTY_ID
	,	mpt.offer_id AS OFFER_ID
	,	gvi.GLOBAL_VOUCHER_CODE						 																	AS VOUCHER_IN_STORE_CODE
	,	CASE	
			WHEN gvo.GLOBAL_VOUCHER_CODE IS NOT NULL
				THEN gvo.GLOBAL_VOUCHER_CODE
			WHEN FORMAT(arc.[VALID_TO_TIME] AT TIME ZONE 'GMT Standard Time' AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss') = mo.[END_DATE_TIME]
				THEN mo.VOUCHER_ON_LINE_CODE
			ELSE NULL
		END 																										AS GLOBAL_OR_EXISTING_ONLINE_CODE
	,	CASE 	WHEN mo.[VISIBLE_FROM_DATE_TIME] <> mo.START_DATE_TIME THEN mo.[VISIBLE_FROM_DATE_TIME]
	ELSE FORMAT(arc.[VALID_FROM_TIME] AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss')
	END 																											AS [VISIBLE_FROM_DATE_TIME]		
	,	FORMAT(arc.[VALID_FROM_TIME] AT TIME ZONE 'GMT Standard Time' AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss')			AS [START_DATE_TIME]
	,	FORMAT(arc.[VALID_TO_TIME] AT TIME ZONE 'GMT Standard Time' AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss')			AS [END_DATE_TIME]
	,	mpt.offer_redemptions																							AS NO_REDEMPTIONS_ALLOWED
	,	CASE	WHEN mo.STATUS IS NULL THEN 'A'
			WHEN mo.STATUS = 'A' AND (mo.DATE_MOBILIZE_SYNC < mo.DATE_UPDATED OR mo.DATE_MOBILIZE_SYNC IS NULL) THEN 'A'
			ELSE 'C'
	END 																											AS STATUS
	,	SYSDATETIME()																									AS DATE_UPDATED
	,	ROW_NUMBER() OVER (ORDER BY (SELECT NULL))																		AS RN
	FROM 
	(
		SELECT	PARTY_ID
		,	LOYALTY_CARD_NUMBER
		,	ROW_NUMBER() OVER (PARTITION BY LOYALTY_CARD_NUMBER ORDER BY SEED_FLAG DESC, PARTY_ID) AS CARD_RN
		FROM
		(
			SELECT PARTY_ID, MATALAN_CARD_NUMBER AS [LOYALTY_CARD_NUMBER], 1 AS [SEED_FLAG]
			FROM [${marketingCloud.seedListTable}]
			UNION ALL 
			SELECT UC.PARTY_ID, PCD.APP_CARD_NUMBER AS [LOYALTY_CARD_NUMBER], 0 AS [SEED_FLAG]
			FROM [${payloadAttributes.update_contact}] AS UC 
			JOIN [${marketingCloud.partyCardDetailsTable}] AS PCD 
			ON UC.PARTY_ID = PCD.PARTY_ID
		) AS UpdateContactDE
		WHERE  LOYALTY_CARD_NUMBER  IS NOT NULL
	) AS parties
	INNER JOIN [${marketingCloud.mobilePushMainTable}] AS mpt
	ON mpt.push_key = ${payloadAttributes.key}
	INNER JOIN [${marketingCloud.recurringCampaignsName}] arc
	ON 		arc.PUSH_KEY = ${payloadAttributes.key}
	AND		arc.SELECTION_DATE = CAST(GETUTCDATE() AS DATE)
	LEFT JOIN [${marketingCloud.memberOfferTableName}] AS mo
	ON  parties.LOYALTY_CARD_NUMBER = mo.LOYALTY_CARD_NUMBER
	AND mpt.OFFER_ID = mo.OFFER_ID
	LEFT JOIN [${marketingCloud.globalVoucherName}] gvi
	ON		gvi.VOUCHER_SUBSET_ID = arc.INSTORE_VSS_ID
	LEFT JOIN [${marketingCloud.globalVoucherName}] gvo 
	ON		gvo.VOUCHER_SUBSET_ID = arc.ONLINE_VSS_ID
	WHERE parties.CARD_RN = 1`;

	const memberQueryPart1Name = `${payloadAttributes.query_name} - part 1 member offer - ${dateString}`;
	const memberQueryPart1Id = await salesforceApi.createSQLQuery(marketingCloud.stagingMemberOfferId
													, memberQueryPart1
													, updateTypes.Overwirte
													, marketingCloud.stagingMemberOfferName
													, memberQueryPart1Name
													, `${payloadAttributes.query_name} - part 1 member offer`);

	//Member Offer Query Part 2
	const memberQueryPart2 = `SELECT pt1.SCHEME_ID,
	pt1.LOYALTY_CARD_NUMBER,
	pt1.PARTY_ID,
	pt1.OFFER_ID,
	ISNULL(pt1.GLOBAL_OR_EXISTING_ONLINE_CODE, vp.VOUCHER_CODE) AS VOUCHER_ON_LINE_CODE,
	pt1.VOUCHER_IN_STORE_CODE AS VOUCHER_IN_STORE_CODE,
	pt1.[VISIBLE_FROM_DATE_TIME],
	pt1.[START_DATE_TIME],
	pt1.[END_DATE_TIME],
	pt1.NO_REDEMPTIONS_ALLOWED,
	pt1.STATUS,
	pt1.DATE_UPDATED
	FROM	[${marketingCloud.stagingMemberOfferName}] pt1
	LEFT JOIN 
	(
		SELECT  VOUCHER_CODE
		,       ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RN
		FROM    [${marketingCloud.recurringCampaignsName}] arc
		JOIN	[${marketingCloud.uniqueVoucherName}] v 
		ON		arc.ONLINE_VSS_ID = v.VOUCHER_SUBSET_ID
		WHERE	arc.PUSH_KEY = ${payloadAttributes.key}
		AND		arc.SELECTION_DATE = CAST(GETUTCDATE() AS DATE)
		AND		v.ASSIGNED_FLAG = 0
	) VP
	ON pt1.RN = VP.RN`;

	const memberQueryPart2Name = `${payloadAttributes.query_name} - part 2 member offer - ${dateString}`;
	const memberQueryPart2Id = await salesforceApi.createSQLQuery(marketingCloud.memberOfferKey
													, memberQueryPart2
													, updateTypes.AddUpdate
													, marketingCloud.memberOfferTableName
													, memberQueryPart2Name
													, `${payloadAttributes.query_name} - part 2 member offer`);

	//Claim Vouchers
	const claimVoucherQuery = `SELECT	uv.VOUCHER_CODE
	    ,   uv.VOUCHER_POT_NAME
	    ,   uv.VOUCHER_SUBSET_ID
	    ,   uv.TEMP_COUPON_NUMBER
	    ,   uv.[VALID_FROM_DATE]
	    ,   uv.VALID_TO_DATE
	    ,   uv.VOUCHER_SET_ID
		,	1 AS ASSIGNED_FLAG
	FROM	[${marketingCloud.memberOfferTableName}] AS mo
	INNER JOIN [${marketingCloud.mobilePushMainTable}] AS mpt
	ON		mo.OFFER_ID = mpt.OFFER_ID
	INNER JOIN [${marketingCloud.uniqueVoucherName}] AS uv 
	ON		mo.VOUCHER_ON_LINE_CODE = uv.VOUCHER_CODE
	WHERE	uv.ASSIGNED_FLAG = 0
	AND	 	mpt.push_key = ${payloadAttributes.key}`;

	const claimVoucherQueryName = `${payloadAttributes.query_name} - claim vouchers - ${dateString}`;
	const claimVoucherQueryId = await salesforceApi.createSQLQuery(marketingCloud.uniqueVoucherId
													, claimVoucherQuery
													, updateTypes.AddUpdate
													, marketingCloud.uniqueVoucherName
													, claimVoucherQueryName
													, `${payloadAttributes.query_name} - set assigned flag for claimed vouchers`);


	const AutomationKey = uuidv4();												
	const createAutomationBody = `{
	    "name": "App Recurring Campaign - ${payloadAttributes.query_name}",
	    "key": "${AutomationKey}",
	    "steps": [
	        {
	            "annotation": "",
	            "stepNumber": 0,
	            "activities": [
	                {
	                    "name": "${stagingCommunicationCellQueryName}",
	                    "objectTypeId": 300,
	                    "displayOrder": 0,
	                    "activityObjectId": "${stagingCommunicationCellQueryId}"
	                }
	            ]
			},
			{
	            "annotation": "",
	            "stepNumber": 1,
	            "activities": [
	                {
	                    "name": "${stagingVoucherSubsetsQueryName}",
	                    "objectTypeId": 300,
	                    "displayOrder": 0,
	                    "activityObjectId": "${stagingVoucherSubsetsQueryId}"
	                }
	            ]
			},
			{
	            "annotation": "",
	            "stepNumber": 2,
	            "activities": [
	                {
	                    "name": "${stagingPromoDescriptionQueryName}",
	                    "objectTypeId": 300,
	                    "displayOrder": 0,
	                    "activityObjectId": "${stagingPromoDescriptionQueryId}"
	                }
	            ]
			},
			{
	            "annotation": "",
	            "stepNumber": 3,
	            "activities": [
	                {
	                    "name": "${appendCommunicationCellQueryName}",
	                    "objectTypeId": 300,
	                    "displayOrder": 0,
	                    "activityObjectId": "${appendCommunicationCellQueryId}"
					},
					{
	                    "name": "${appendPromoDescriptionQueryName}",
	                    "objectTypeId": 300,
	                    "displayOrder": 1,
	                    "activityObjectId": "${appendPromoDescriptionQueryId}"
	                },
	            ]
			},
			{
	            "annotation": "",
	            "stepNumber": 4,
	            "activities": [
	                {
	                    "name": "${incrementUpdateQueryName}",
	                    "objectTypeId": 300,
	                    "displayOrder": 0,
	                    "activityObjectId": "${incrementUpdateQueryId}"
	                }
	            ]
			},
			{
	            "annotation": "",
	            "stepNumber": 5,
	            "activities": [
	                {
	                    "name": "${recurringCampaignQueryName}",
	                    "objectTypeId": 300,
	                    "displayOrder": 0,
	                    "activityObjectId": "${recurringCampaignQueryId}"
	                }
	            ]
			},
			{
	            "annotation": "",
	            "stepNumber": 6,
	            "activities": [
	                {
	                    "name": "${partyCommunicationQueryName}",
	                    "objectTypeId": 300,
	                    "displayOrder": 0,
	                    "activityObjectId": "${partyCommunicationQueryId}"
	                }
	            ]
			},
			{
	            "annotation": "",
	            "stepNumber": 7,
	            "activities": [
	                {
	                    "name": "${assignmentQueryName}",
	                    "objectTypeId": 300,
	                    "displayOrder": 0,
	                    "activityObjectId": "${assignmentQueryId}"
	                }
	            ]
			},
			{
	            "annotation": "",
	            "stepNumber": 8,
	            "activities": [
	                {
	                    "name": "${masterOfferQueryName}",
	                    "objectTypeId": 300,
	                    "displayOrder": 0,
	                    "activityObjectId": "${masterOfferQueryId}"
	                }
	            ]
			},
			{
	            "annotation": "",
	            "stepNumber": 9,
	            "activities": [
	                {
	                    "name": "${memberQueryPart1Name}",
	                    "objectTypeId": 300,
	                    "displayOrder": 0,
	                    "activityObjectId": "${memberQueryPart1Id}"
	                }
	            ]
			},
			{
	            "annotation": "",
	            "stepNumber": 10,
	            "activities": [
	                {
	                    "name": "${memberQueryPart2Name}",
	                    "objectTypeId": 300,
	                    "displayOrder": 0,
	                    "activityObjectId": "${memberQueryPart2Id}"
	                }
	            ]
			},
			{
	            "annotation": "",
	            "stepNumber": 11,
	            "activities": [
	                {
	                    "name": "${claimVoucherQueryName}",
	                    "objectTypeId": 300,
	                    "displayOrder": 0,
	                    "activityObjectId": "${claimVoucherQueryId}"
	                }
	            ]
	        }
	    ],
	    "startSource": {
	        "typeId": 1
	    },
	    "categoryId": 3579
	}`;
}
