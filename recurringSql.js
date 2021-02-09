const { v4: uuidv4 } 	= require('uuid');
const salesforceApi		= require("./salesforceApi.js");

const updateTypes = {
	Overwrite: 0,
	AddUpdate: 1,
	Append: 2
};

const environment = {
	assignmentKey:						process.env.assignmentKey,
	assignmentName:						process.env.assignmentTableName,
	communicationCellId:				process.env.communicationCellDataExtension,		
	communicationCellName:				process.env.communicationCellName,		
	communicationHistoryKey:			process.env.communicationHistoryKey,			
	communicationHistoryName:			process.env.communicationTableName,			
	globalVoucherName:					process.env.globalVoucherName,	
	masterOfferKey:						process.env.masterOfferKey,
	masterOfferName:					process.env.masterOfferTableName,	
	memberOfferKey:						process.env.memberOfferKey,
	memberOfferName:					process.env.memberOfferTableName,	
	mobilePushMainTable:				process.env.mobilePushMainTable,		
	partyCardDetailsTable:				process.env.partyCardDetailsTable,		
	promotionIncrementsId:				process.env.commCellIncrementDataExtension,		
	promotionIncrementsName:			process.env.promotionIncrementsName,			
	promotionDescriptionId:				process.env.promotionDescriptionId,		
	promotionDescriptionName:			process.env.promotionDescriptionTable,			
	recurringCampaignsId:				process.env.recurringCampaignsId,		
	recurringCampaignsName:				process.env.recurringCampaignsName,		
	recurringVoucherSubsetsId:			process.env.recurringVoucherSubsetsId,			
	recurringVoucherSubsetsName:		process.env.recurringVoucherSubsetsName,				
	seedListTable:						process.env.seedListTable,
	seedMemberStagingId:				process.env.seedMemberStagingId,
	seedMemberStagingName:				process.env.seedMemberStagingName,
	seedRecurringCampaignsId:			process.env.seedRecurringCampaignsId,
	seedRecurringCampaignsName:			process.env.seedRecurringCampaignsName,
	seedVoucherId:						process.env.seedVoucherId,
	seedVoucherName:					process.env.seedVoucherName,
	stagingCommunicationCellId:			process.env.stagingCommunicationCellId,			
	stagingCommunicationCellName:		process.env.stagingCommunicationCellName,				
	stagingPromotionDescriptionId:		process.env.stagingPromotionDescriptionId,				
	stagingPromotionDescriptionName:	process.env.stagingPromotionDescriptionName,					
	stagingMemberOfferId:				process.env.stagingMemberOfferId,		
	stagingMemberOfferName:				process.env.stagingMemberOfferName,		
	uniqueVoucherId:					process.env.uniqueVoucherId,	
	uniqueVoucherName:					process.env.uniqueVoucherName,	
	voucherSetName:						process.env.voucherSetName,
	voucherSubsetName:					process.env.voucherSubsetName,
};

exports.recurringCamapign = async function(payloadAttributes){
	
	const m = new Date();
	const dateString =
	("0" + m.getUTCFullYear()).slice(-2) +
	("0" + (m.getUTCMonth() + 1)).slice(-2) +
	("0" + m.getUTCDate()).slice(-2) +
	("0" + m.getUTCHours()).slice(-2) +
	("0" + m.getUTCMinutes()).slice(-2);

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
	from [${environment.mobilePushMainTable}] mpmt
	CROSS JOIN [${environment.promotionIncrementsName}] pi
	where mpmt.push_key = ${payloadAttributes.key}`;

	const stagingCommunicationCellQueryName = `Staging comm cell - ${dateString} - ${payloadAttributes.query_name}`;
	const stagingCommunicationCellQueryId = await salesforceApi.createSQLQuery(environment.stagingCommunicationCellId
													, stagingCommunicationCellQuery
													, updateTypes.Overwrite
													, environment.stagingCommunicationCellName
													, stagingCommunicationCellQueryName
													, `${payloadAttributes.query_name} - staging comm cell`);

	const stagingCommunicationCellActivity = {
		name: stagingCommunicationCellQueryName,
		objectTypeId: 300,
		displayOrder: 0,
		activityObjectId: stagingCommunicationCellQueryId
	};
	const automationStep0 = {
		annotation: "",
		stepnumber: 0,
		activities: [stagingCommunicationCellActivity],

	};

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
			FROM	[${environment.voucherSetName}] vs 
			JOIN 	[${environment.mobilePushMainTable}] mpmt
			ON		vs.VOUCHER_GROUP_ID = mpmt.RECURRING_VOUCHER_GROUP_ID 
			WHERE	mpmt.push_key = ${payloadAttributes.key}
			AND		vs.ALLOCATION_DATE <= DATEADD(DAY, mpmt.RECURRING_OFFER_DELAY_DAYS, CAST(GETUTCDATE() AS DATE))
			ORDER BY vs.ALLOCATION_DATE DESC
		) vset
		JOIN [${environment.voucherSubsetName}] vss1
		ON 	vset.VOUCHER_SET_ID = vss1.VOUCHER_SET_ID
	) AS vsubset
	WHERE	vsubset.rn = 1`;

	const stagingVoucherSubsetsQueryName = `Staging voucher subsets - ${dateString} - ${payloadAttributes.query_name}`;
	const stagingVoucherSubsetsQueryId = await salesforceApi.createSQLQuery(environment.recurringVoucherSubsetsId
													, stagingVoucherSubsetsQuery
													, updateTypes.Overwrite
													, environment.recurringVoucherSubsetsName
													, stagingVoucherSubsetsQueryName
													, `${payloadAttributes.query_name} - staging voucher subsets`);

	const stagingVoucherSubsetsActivity = {
		name: stagingVoucherSubsetsQueryName,
		objectTypeId: 300,
		displayOrder: 0,
		activityObjectId: stagingVoucherSubsetsQueryId
	};
	const automationStep1 = {
		annotation: "",
		stepnumber: 1,
		activities: [stagingVoucherSubsetsActivity],

	};

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
	FROM	[${environment.recurringVoucherSubsetsName}] arvs
	JOIN 	[${environment.voucherSubsetName}] vss
	ON		arvs.VOUCHER_SUBSET_ID = vss.VOUCHER_SUBSET_ID
	CROSS JOIN	[${environment.stagingCommunicationCellName}] scc
	CROSS JOIN	[${environment.promotionIncrementsName}] pi
	JOIN 	[${environment.mobilePushMainTable}] mpmt	
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
	FROM	[${environment.recurringVoucherSubsetsName}] arvs
	JOIN 	[${environment.voucherSubsetName}] vss
	ON		arvs.VOUCHER_SUBSET_ID = vss.VOUCHER_SUBSET_ID
	LEFT JOIN 	[${environment.globalVoucherName}] gv
	ON		vss.VOUCHER_SUBSET_ID = gv.VOUCHER_SUBSET_ID
	CROSS JOIN	[${environment.stagingCommunicationCellName}] scc
	CROSS JOIN	[${environment.promotionIncrementsName}] pi
	JOIN 	[${environment.mobilePushMainTable}] mpmt	
	ON		mpmt.PUSH_KEY = ${payloadAttributes.key}
	WHERE	arvs.BARCODE_FLAG = 1`;

	const stagingPromoDescriptionQueryName = `Staging promo desc - ${dateString} - ${payloadAttributes.query_name}`;
	const stagingPromoDescriptionQueryId = await salesforceApi.createSQLQuery(environment.stagingPromotionDescriptionId
													, stagingPromoDescriptionQuery
													, updateTypes.Overwrite
													, environment.stagingPromotionDescriptionName
													, stagingPromoDescriptionQueryName
													, `${payloadAttributes.query_name} - staging promo desc`);

	const stagingPromoDescriptionActivity = {
		name: stagingPromoDescriptionQueryName,
		objectTypeId: 300,
		displayOrder: 0,
		activityObjectId: stagingPromoDescriptionQueryId
	};
	const automationStep2 = {
		annotation: "",
		stepnumber: 2,
		activities: [stagingPromoDescriptionActivity],

	};

	//from staging into comm cell and promo desc
	const appendCommunicationCellQuery = `SELECT	*
	FROM	[${environment.stagingCommunicationCellName}]`;

	const appendCommunicationCellQueryName = `Append comm cell - ${dateString} - ${payloadAttributes.query_name}`;
	const appendCommunicationCellQueryId = await salesforceApi.createSQLQuery(environment.communicationCellId
													, appendCommunicationCellQuery
													, updateTypes.Append
													, environment.communicationCellName
													, appendCommunicationCellQueryName
													, `${payloadAttributes.query_name} - append comm cell`);

	const appendCommunicationCellActivity = {
		name: appendCommunicationCellQueryName,
		objectTypeId: 300,
		displayOrder: 0,
		activityObjectId: appendCommunicationCellQueryId
	};												

	const appendPromoDescriptionQuery = `SELECT	*
	FROM	[${environment.stagingPromotionDescriptionName}]`;

	const appendPromoDescriptionQueryName = `Append promo desc - ${dateString} - ${payloadAttributes.query_name}`;
	const appendPromoDescriptionQueryId = await salesforceApi.createSQLQuery(environment.promotionDescriptionId
													, appendPromoDescriptionQuery
													, updateTypes.Append
													, environment.promotionDescriptionName
													, appendPromoDescriptionQueryName
													, `${payloadAttributes.query_name} - append promo desc`);

	const appendPromoDescriptionActivity = {
		name: appendPromoDescriptionQueryName,
		objectTypeId: 300,
		displayOrder: 1,
		activityObjectId: appendPromoDescriptionQueryId
	};
	const automationStep3 = {
		annotation: "",
		stepnumber: 3,
		activities: [appendCommunicationCellActivity, appendPromoDescriptionActivity]
	};

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
			FROM [${environment.communicationCellName}]  
			UNION ALL 
			SELECT communication_cell_code_id_increment AS cc_id
			FROM [${environment.promotionIncrementsName}]
		) AS comm
	) AS maxcomm
	CROSS JOIN 
	(
		SELECT MAX(mc_id) + 1 AS newmcid
		FROM
		(
			SELECT MAX(CAST(MC_UNIQUE_PROMOTION_ID AS INT)) AS mc_id
			FROM [${environment.promotionDescriptionName}]
			UNION ALL 
			SELECT mc_unique_promotion_id_increment AS mc_id
			FROM [${environment.promotionIncrementsName}]
		) AS promo
	) AS maxpromo`;

	const incrementUpdateQueryName = `Update increments - ${dateString} - ${payloadAttributes.query_name}`;
	const incrementUpdateQueryId = await salesforceApi.createSQLQuery(environment.promotionIncrementsId
													, incrementUpdateQuery
													, updateTypes.AddUpdate
													, environment.promotionIncrementsName
													, incrementUpdateQueryName
													, `${payloadAttributes.query_name} - update increments`);

	const incrementUpdateActivity = {
		name: incrementUpdateQueryName,
		objectTypeId: 300,
		displayOrder: 0,
		activityObjectId: incrementUpdateQueryId
	};
	const automationStep4 = {
		annotation: "",
		stepnumber: 4,
		activities: [incrementUpdateActivity]
	};

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
	FROM	[${environment.mobilePushMainTable}] mpmt	
	LEFT JOIN
	(
		SELECT	arvs1.VOUCHER_SUBSET_ID AS ONLINE_VOUCHER_SUBSET_ID
			,	arvs2.VOUCHER_SUBSET_ID AS INSTORE_VOUCHER_SUBSET_ID
		FROM	
		(
			SELECT	VOUCHER_SUBSET_ID
			FROM	[${environment.recurringVoucherSubsetsName}]
			WHERE	BARCODE_FLAG = 0
		) AS arvs1
		FULL JOIN 
		(
			SELECT	VOUCHER_SUBSET_ID
			FROM	[${environment.recurringVoucherSubsetsName}]
			WHERE	BARCODE_FLAG = 1
		) AS arvs2
		ON		1 = 1
	) vsubset
	ON			1 = 1
	LEFT JOIN	[${environment.voucherSubsetName}] onlinevss
	ON			vsubset.ONLINE_VOUCHER_SUBSET_ID = onlinevss.VOUCHER_SUBSET_ID
	LEFT JOIN	[${environment.voucherSubsetName}] storevss
	ON			vsubset.INSTORE_VOUCHER_SUBSET_ID = storevss.VOUCHER_SUBSET_ID
	CROSS JOIN	[${environment.stagingCommunicationCellName}] scc
	LEFT JOIN	[${environment.stagingPromotionDescriptionName}] onlinespd
	ON			scc.COMMUNICATION_CELL_ID = onlinespd.COMMUNICATION_CELL_ID
	AND			onlinevss.PROMOTION_ID = onlinespd.PROMOTION_ID
	LEFT JOIN	[${environment.stagingPromotionDescriptionName}] storespd
	ON			scc.COMMUNICATION_CELL_ID = storespd.COMMUNICATION_CELL_ID
	AND			storevss.BARCODE_REDEEMING_ID = storespd.PROMOTION_ID
	WHERE		mpmt.PUSH_KEY = ${payloadAttributes.key}`;

	const recurringCampaignQueryName = `Add to recurring campaigns - ${dateString} - ${payloadAttributes.query_name}`;
	const recurringCampaignQueryId = await salesforceApi.createSQLQuery(environment.recurringCampaignsId
													, recurringCampaignQuery
													, updateTypes.Append
													, environment.recurringCampaignsName
													, recurringCampaignQueryName
													, `${payloadAttributes.query_name} - add to recurring campaigns`);
											
	const recurringCampaignActivity = {
		name: recurringCampaignQueryName,
		objectTypeId: 300,
		displayOrder: 0,
		activityObjectId: recurringCampaignQueryId
	};
	const automationStep5 = {
		annotation: "",
		stepnumber: 5,
		activities: [recurringCampaignActivity]
	};

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
			FROM [${environment.seedListTable}]
			UNION ALL 
			SELECT UC.PARTY_ID, PCD.APP_CARD_NUMBER AS [LOYALTY_CARD_NUMBER], 0 AS [SEED_FLAG]
			FROM [${payloadAttributes.update_contact}]  AS UC 
			JOIN [${environment.partyCardDetailsTable}] AS PCD
			ON UC.PARTY_ID = PCD.PARTY_ID
		) AS UpdateContactDE
		WHERE	UpdateContactDE.LOYALTY_CARD_NUMBER IS NOT NULL
	) AS parties
	JOIN [${environment.recurringCampaignsName}] AS arc
	ON		arc.push_key = ${payloadAttributes.key}
	WHERE	arc.SELECTION_DATE = CAST(GETUTCDATE() AS DATE)
	AND		parties.CARD_RN = 1`;

	const partyCommunicationQueryName = `Add to party communication - ${dateString} - ${payloadAttributes.query_name}`;
	const partyCommunicationQueryId = await salesforceApi.createSQLQuery(environment.communicationHistoryKey
													, partyCommunicationQuery
													, updateTypes.Append
													, environment.communicationHistoryName
													, partyCommunicationQueryName
													, `${payloadAttributes.query_name} - add to party communication`);

	const partyCommunicationActivity = {
		name: partyCommunicationQueryName,
		objectTypeId: 300,
		displayOrder: 0,
		activityObjectId: partyCommunicationQueryId
	};
	const automationStep6 = {
		annotation: "",
		stepnumber: 6,
		activities: [partyCommunicationActivity]
	};

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
			FROM [${environment.seedListTable}]
			UNION ALL 
			SELECT UC.PARTY_ID, PCD.APP_CARD_NUMBER AS [LOYALTY_CARD_NUMBER], 0 AS [SEED_FLAG]
			FROM [${payloadAttributes.update_contact}] AS UC 
			JOIN [${environment.partyCardDetailsTable}] AS PCD 
			ON UC.PARTY_ID = PCD.PARTY_ID
		) AS UpdateContactDE
		WHERE   LOYALTY_CARD_NUMBER IS NOT NULL
	) AS parties
	CROSS JOIN 
	(
		SELECT	INSTORE_MC_ID AS MC_ID
		FROM	[${environment.recurringCampaignsName}]
		WHERE	push_key = ${payloadAttributes.key}
		AND		INSTORE_MC_ID IS NOT NULL
		AND		SELECTION_DATE = CAST(GETUTCDATE() AS DATE)
		UNION ALL 
		SELECT	ONLINE_MC_ID AS MC_ID
		FROM	[${environment.recurringCampaignsName}]
		WHERE	push_key = ${payloadAttributes.key}
		AND		ONLINE_MC_ID IS NOT NULL
		AND		SELECTION_DATE = CAST(GETUTCDATE() AS DATE)
	) AS arc
	WHERE parties.CARD_RN = 1`;

	const assignmentQueryName = `Add to promo assignment - ${dateString} - ${payloadAttributes.query_name}`;
	const assignmentQueryId = await salesforceApi.createSQLQuery(environment.assignmentKey
													, assignmentQuery
													, updateTypes.Append
													, environment.assignmentName
													, assignmentQueryName
													, `${payloadAttributes.query_name} - add to promo assignment`);

	const assignmentActivity = {
		name: assignmentQueryName,
		objectTypeId: 300,
		displayOrder: 0,
		activityObjectId: assignmentQueryId
	};
	const automationStep7 = {
		annotation: "",
		stepnumber: 7,
		activities: [assignmentActivity]
	};
											

	//master offer query
	const masterOfferQuery = `SELECT	gv.GLOBAL_VOUCHER_CODE 									AS VOUCHER_IN_STORE_CODE
		,	ISNULL(o.START_DATE_TIME, FORMAT(arc.[VALID_FROM_TIME] AT TIME ZONE 'GMT Standard Time' AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss')) AS START_DATE_TIME
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
	FROM	[${environment.mobilePushMainTable}] AS mpt
	LEFT JOIN [${environment.masterOfferName}]  AS o 
	ON      mpt.OFFER_ID = o.OFFER_ID
	JOIN 	[${environment.recurringCampaignsName}] arc
	ON 		arc.PUSH_KEY = mpt.PUSH_KEY
	AND		arc.SELECTION_DATE = CAST(GETUTCDATE() AS DATE)
	LEFT JOIN [${environment.globalVoucherName}] gv
	ON		arc.INSTORE_VSS_ID = gv.VOUCHER_SUBSET_ID
	WHERE	mpt.PUSH_KEY = ${payloadAttributes.key}
	AND	(	o.STATUS IS NULL
		OR 	o.STATUS <> 'D')`;

	const masterOfferQueryName = `Master offer - ${dateString} - ${payloadAttributes.query_name}`;
	const masterOfferQueryId = await salesforceApi.createSQLQuery(environment.masterOfferKey
													, masterOfferQuery
													, updateTypes.AddUpdate
													, environment.masterOfferName
													, masterOfferQueryName
													, `${payloadAttributes.query_name} - master offer`);

	const masterOfferActivity = {
		name: masterOfferQueryName,
		objectTypeId: 300,
		displayOrder: 0,
		activityObjectId: masterOfferQueryId
	};
	const automationStep8 = {
		annotation: "",
		stepnumber: 8,
		activities: [masterOfferActivity]
	};

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
			SELECT UC.PARTY_ID, PCD.APP_CARD_NUMBER AS [LOYALTY_CARD_NUMBER], 0 AS [SEED_FLAG]
			FROM [${payloadAttributes.update_contact}] AS UC 
			JOIN [${environment.partyCardDetailsTable}] AS PCD 
			ON UC.PARTY_ID = PCD.PARTY_ID
		) AS UpdateContactDE
		WHERE  LOYALTY_CARD_NUMBER  IS NOT NULL
	) AS parties
	INNER JOIN [${environment.mobilePushMainTable}] AS mpt
	ON mpt.push_key = ${payloadAttributes.key}
	INNER JOIN [${environment.recurringCampaignsName}] arc
	ON 		arc.PUSH_KEY = ${payloadAttributes.key}
	AND		arc.SELECTION_DATE = CAST(GETUTCDATE() AS DATE)
	LEFT JOIN [${environment.memberOfferName}] AS mo
	ON  parties.LOYALTY_CARD_NUMBER = mo.LOYALTY_CARD_NUMBER
	AND mpt.OFFER_ID = mo.OFFER_ID
	LEFT JOIN [${environment.globalVoucherName}] gvi
	ON		gvi.VOUCHER_SUBSET_ID = arc.INSTORE_VSS_ID
	LEFT JOIN [${environment.globalVoucherName}] gvo 
	ON		gvo.VOUCHER_SUBSET_ID = arc.ONLINE_VSS_ID
	WHERE parties.CARD_RN = 1`;

	const memberQueryPart1Name = `Part 1 member offer - ${dateString} - ${payloadAttributes.query_name}`;
	const memberQueryPart1Id = await salesforceApi.createSQLQuery(environment.stagingMemberOfferId
													, memberQueryPart1
													, updateTypes.Overwrite
													, environment.stagingMemberOfferName
													, memberQueryPart1Name
													, `${payloadAttributes.query_name} - part 1 member offer`);

	const memberQueryPart1Activity = {
		name: memberQueryPart1Name,
		objectTypeId: 300,
		displayOrder: 0,
		activityObjectId: memberQueryPart1Id
	};
	const automationStep9 = {
		annotation: "",
		stepnumber: 9,
		activities: [memberQueryPart1Activity]
	};

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
	FROM	[${environment.stagingMemberOfferName}] pt1
	LEFT JOIN 
	(
		SELECT  VOUCHER_CODE
		,       ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RN
		FROM    [${environment.recurringCampaignsName}] arc
		JOIN	[${environment.uniqueVoucherName}] v 
		ON		arc.ONLINE_VSS_ID = v.VOUCHER_SUBSET_ID
		WHERE	arc.PUSH_KEY = ${payloadAttributes.key}
		AND		arc.SELECTION_DATE = CAST(GETUTCDATE() AS DATE)
		AND		v.ASSIGNED_FLAG = 0
	) VP
	ON pt1.RN = VP.RN`;

	const memberQueryPart2Name = `Part 2 member offer - ${dateString} - ${payloadAttributes.query_name}`;
	const memberQueryPart2Id = await salesforceApi.createSQLQuery(environment.memberOfferKey
													, memberQueryPart2
													, updateTypes.AddUpdate
													, environment.memberOfferName
													, memberQueryPart2Name
													, `${payloadAttributes.query_name} - part 2 member offer`);

	const memberQueryPart2Activity = {
		name: memberQueryPart2Name,
		objectTypeId: 300,
		displayOrder: 0,
		activityObjectId: memberQueryPart2Id
	};
	const automationStep10 = {
		annotation: "",
		stepnumber: 10,
		activities: [memberQueryPart2Activity]
	};

	//Claim Vouchers
	const claimVoucherQuery = `SELECT	uv.VOUCHER_CODE
	    ,   uv.VOUCHER_POT_NAME
	    ,   uv.VOUCHER_SUBSET_ID
	    ,   uv.TEMP_COUPON_NUMBER
	    ,   uv.[VALID_FROM_DATE]
	    ,   uv.VALID_TO_DATE
	    ,   uv.VOUCHER_SET_ID
		,	1 AS ASSIGNED_FLAG
	FROM	[${environment.memberOfferName}] AS mo
	INNER JOIN [${environment.mobilePushMainTable}] AS mpt
	ON		mo.OFFER_ID = mpt.OFFER_ID
	INNER JOIN [${environment.uniqueVoucherName}] AS uv 
	ON		mo.VOUCHER_ON_LINE_CODE = uv.VOUCHER_CODE
	WHERE	uv.ASSIGNED_FLAG = 0
	AND	 	mpt.push_key = ${payloadAttributes.key}`;

	const claimVoucherQueryName = `Claim vouchers - ${dateString} - ${payloadAttributes.query_name}`;
	const claimVoucherQueryId = await salesforceApi.createSQLQuery(environment.uniqueVoucherId
													, claimVoucherQuery
													, updateTypes.AddUpdate
													, environment.uniqueVoucherName
													, claimVoucherQueryName
													, `${payloadAttributes.query_name} - set assigned flag for claimed vouchers`);

	const claimVoucherActivity = {
		name: claimVoucherQueryName,
		objectTypeId: 300,
		displayOrder: 0,
		activityObjectId: claimVoucherQueryId
	};
	const automationStep11 = {
		annotation: "",
		stepnumber: 11,
		activities: [claimVoucherActivity]
	};


	const AutomationKey = uuidv4();	
	const sourceType = {typeId: 1};
	const queriesInAutomation =[automationStep0, automationStep1, automationStep2, automationStep3, automationStep4, automationStep5, automationStep6, automationStep7, automationStep8, automationStep9, automationStep10, automationStep11];

	const createAutomationBody = {
	    name: "App Recurring Campaign - " + dateString + " - " + payloadAttributes.query_name,
	    key: AutomationKey,
	    steps: queriesInAutomation,
	    startSource: sourceType,
		categoryId: 3579
	};
	
	await salesforceApi.createSQLAutomation(createAutomationBody);
}

exports.recurringCamapignToSeeds = async function(payloadAttributes){
	const returnIds = [];
	const m = new Date();
	const dateString =
	("0" + m.getUTCFullYear()).slice(-2) +
	("0" + (m.getUTCMonth() + 1)).slice(-2) +
	("0" + m.getUTCDate()).slice(-2) +
	("0" + m.getUTCHours()).slice(-2) +
	("0" + m.getUTCMinutes()).slice(-2);

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
				FROM	[${environment.voucherSetName}] vs 
				JOIN 	[${environment.mobilePushMainTable}] mpmt
				ON		vs.VOUCHER_GROUP_ID = mpmt.RECURRING_VOUCHER_GROUP_ID 
				WHERE	mpmt.push_key = ${payloadAttributes.key}
				AND		vs.ALLOCATION_DATE <= DATEADD(DAY, mpmt.RECURRING_OFFER_DELAY_DAYS, CASE WHEN mpmt.SEED_SELECTION_TYPE = 'today' THEN CAST(GETUTCDATE() AS DATE) ELSE CAST(mpmt.SEED_SELECTION_DATE AS DATE) END)
				ORDER BY vs.ALLOCATION_DATE DESC
			) vset
			JOIN [${environment.voucherSubsetName}] vss1
			ON 	vset.VOUCHER_SET_ID = vss1.VOUCHER_SET_ID
		) AS vsubset
		WHERE	vsubset.rn = 1`;

	const stagingVoucherSubsetsQueryName = `SEED - Staging voucher subsets - ${dateString} - ${payloadAttributes.query_name}`;
	const stagingVoucherSubsetsQueryId = await salesforceApi.createSQLQuery(environment.seedVoucherId
													, stagingVoucherSubsetsQuery
													, updateTypes.Overwrite
													, environment.seedVoucherName
													, stagingVoucherSubsetsQueryName
													, `SEED - ${payloadAttributes.query_name} - staging voucher`);
	await salesforceApi.runSQLQuery(stagingVoucherSubsetsQueryId, stagingVoucherSubsetsQueryName);
	returnIds["stagingVoucherSubsetsQueryId"] = stagingVoucherSubsetsQueryId;

	const recurringCampaignQuery = `SELECT	SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time' AS [VALID_FROM_TIME]
			,	CAST(DATEADD(DAY,mpmt.RECURRING_OFFER_DELAY_DAYS + mpmt.RECURRING_OFFER_VALIDITY_DAYS, CAST(GETUTCDATE() AS DATE))AS DATETIME) + CAST('23:59' AS DATETIME) AS [VALID_TO_TIME]
			,	storevss.VOUCHER_SUBSET_ID AS INSTORE_VSS_ID
			,	onlinevss.VOUCHER_SUBSET_ID AS ONLINE_VSS_ID
			,	GETUTCDATE() AS LAST_UPDATE_AUDIT
			,	mpmt.PUSH_KEY
		FROM	[${environment.mobilePushMainTable}] mpmt	
		LEFT JOIN
		(
			SELECT	arvs1.VOUCHER_SUBSET_ID AS ONLINE_VOUCHER_SUBSET_ID
				,	arvs2.VOUCHER_SUBSET_ID AS INSTORE_VOUCHER_SUBSET_ID
			FROM	
			(
				SELECT	VOUCHER_SUBSET_ID
				FROM	[${environment.seedVoucherName}]
				WHERE	BARCODE_FLAG = 0
			) AS arvs1
			FULL JOIN 
			(
				SELECT	VOUCHER_SUBSET_ID
				FROM	[${environment.seedVoucherName}]
				WHERE	BARCODE_FLAG = 1
			) AS arvs2
			ON		1 = 1
		) vsubset
		ON			1 = 1
		LEFT JOIN	[${environment.voucherSubsetName}] onlinevss
		ON			vsubset.ONLINE_VOUCHER_SUBSET_ID = onlinevss.VOUCHER_SUBSET_ID
		LEFT JOIN	[${environment.voucherSubsetName}] storevss
		ON			vsubset.INSTORE_VOUCHER_SUBSET_ID = storevss.VOUCHER_SUBSET_ID
		WHERE		mpmt.PUSH_KEY = ${payloadAttributes.key}`;

	const recurringCampaignQueryName = `SEED - Add to recurring campaigns - ${dateString} - ${payloadAttributes.query_name}`;
	const recurringCampaignQueryId = await salesforceApi.createSQLQuery(environment.seedRecurringCampaignsId
													, recurringCampaignQuery
													, updateTypes.AddUpdate
													, environment.seedRecurringCampaignsName
													, recurringCampaignQueryName
													, `SEED - ${payloadAttributes.query_name} - add to recurring campaigns`);
	await salesforceApi.runSQLQuery(recurringCampaignQueryId, recurringCampaignQueryName);
	returnIds["recurringCampaignQueryId"] = recurringCampaignQueryId;

	const masterOfferQuery = `SELECT	gv.GLOBAL_VOUCHER_CODE 									AS VOUCHER_IN_STORE_CODE
			,	ISNULL(o.START_DATE_TIME, FORMAT(arc.[VALID_FROM_TIME] AT TIME ZONE 'GMT Standard Time' AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss')) AS START_DATE_TIME
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
		FROM	[${environment.mobilePushMainTable}] AS mpt
		LEFT JOIN [${environment.masterOfferName}]  AS o 
		ON      mpt.OFFER_ID = o.OFFER_ID
		JOIN 	[${environment.seedRecurringCampaignsName}] arc
		ON 		arc.PUSH_KEY = mpt.PUSH_KEY
		LEFT JOIN [${environment.globalVoucherName}] gv
		ON		arc.INSTORE_VSS_ID = gv.VOUCHER_SUBSET_ID
		WHERE	mpt.PUSH_KEY = ${payloadAttributes.key}
		AND	(	o.STATUS IS NULL
			OR 	o.STATUS <> 'D')`;
	
	const masterOfferQueryName = `SEED - Master offer - ${dateString} - ${payloadAttributes.query_name}`;
	const masterOfferQueryId = await salesforceApi.createSQLQuery(environment.masterOfferKey
													, masterOfferQuery
													, updateTypes.AddUpdate
													, environment.masterOfferName
													, masterOfferQueryName
													, `SEED - ${payloadAttributes.query_name} - master offer`);
	await salesforceApi.runSQLQuery(masterOfferQueryId, masterOfferQueryName);
	returnIds["masterOfferQueryId"] = masterOfferQueryId;

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
				FROM [${environment.seedListTable}]				
			) AS UpdateContactDE
			WHERE  LOYALTY_CARD_NUMBER  IS NOT NULL
		) AS parties
		INNER JOIN [${environment.mobilePushMainTable}] AS mpt
		ON mpt.push_key = ${payloadAttributes.key}
		INNER JOIN [${environment.seedRecurringCampaignsName}] arc
		ON 		arc.PUSH_KEY = ${payloadAttributes.key}
		LEFT JOIN [${environment.memberOfferName}] AS mo
		ON  parties.LOYALTY_CARD_NUMBER = mo.LOYALTY_CARD_NUMBER
		AND mpt.OFFER_ID = mo.OFFER_ID
		LEFT JOIN [${environment.globalVoucherName}] gvi
		ON		gvi.VOUCHER_SUBSET_ID = arc.INSTORE_VSS_ID
		LEFT JOIN [${environment.globalVoucherName}] gvo 
		ON		gvo.VOUCHER_SUBSET_ID = arc.ONLINE_VSS_ID
		WHERE parties.CARD_RN = 1`;
	
	const memberQueryPart1Name = `SEED - Part 1 member offer - ${dateString} - ${payloadAttributes.query_name}`;
	const memberQueryPart1Id = await salesforceApi.createSQLQuery(environment.seedMemberStagingId
													, memberQueryPart1
													, updateTypes.Overwrite
													, environment.seedMemberStagingName
													, memberQueryPart1Name
													, `SEED -${payloadAttributes.query_name} - part 1 member offer`);
	await salesforceApi.runSQLQuery(memberQueryPart1Id, memberQueryPart1Name);
	returnIds["memberQueryPart1Id"] = memberQueryPart1Id;

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
	FROM	[${environment.seedMemberStagingName}] pt1
	LEFT JOIN 
	(
		SELECT  VOUCHER_CODE
		,       ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RN
		FROM    [${environment.seedRecurringCampaignsName}] arc
		JOIN	[${environment.uniqueVoucherName}] v 
		ON		arc.ONLINE_VSS_ID = v.VOUCHER_SUBSET_ID
		WHERE	arc.PUSH_KEY = ${payloadAttributes.key}
		AND		v.ASSIGNED_FLAG = 0
	) VP
	ON pt1.RN = VP.RN`;

	const memberQueryPart2Name = `SEED - Part 2 member offer - ${dateString} - ${payloadAttributes.query_name}`;
	const memberQueryPart2Id = await salesforceApi.createSQLQuery(environment.memberOfferKey
													, memberQueryPart2
													, updateTypes.AddUpdate
													, environment.memberOfferName
													, memberQueryPart2Name
													, `SEED - ${payloadAttributes.query_name} - part 2 member offer`);
	await salesforceApi.runSQLQuery(memberQueryPart2Id, memberQueryPart2Name);
	returnIds["memberQueryPart2Id"] = memberQueryPart2Id;
	
	const claimVoucherQuery = `SELECT	uv.VOUCHER_CODE
			,   uv.VOUCHER_POT_NAME
			,   uv.VOUCHER_SUBSET_ID
			,   uv.TEMP_COUPON_NUMBER
			,   uv.[VALID_FROM_DATE]
			,   uv.VALID_TO_DATE
			,   uv.VOUCHER_SET_ID
			,	1 AS ASSIGNED_FLAG
		FROM	[${environment.memberOfferName}] AS mo
		INNER JOIN [${environment.mobilePushMainTable}] AS mpt
		ON		mo.OFFER_ID = mpt.OFFER_ID
		INNER JOIN [${environment.uniqueVoucherName}] AS uv 
		ON		mo.VOUCHER_ON_LINE_CODE = uv.VOUCHER_CODE
		WHERE	uv.ASSIGNED_FLAG = 0
		AND	 	mpt.push_key = ${payloadAttributes.key}`;

	const claimVoucherQueryName = `SEED - Claim vouchers - ${dateString} - ${payloadAttributes.query_name}`;
	const claimVoucherQueryId = await salesforceApi.createSQLQuery(environment.uniqueVoucherId
													, claimVoucherQuery
													, updateTypes.AddUpdate
													, environment.uniqueVoucherName
													, claimVoucherQueryName
													, `SEED - ${payloadAttributes.query_name} - set assigned flag for claimed vouchers`);

	await salesforceApi.runSQLQuery(claimVoucherQueryId, claimVoucherQueryName);
	returnIds["claimVoucherQueryId"] = claimVoucherQueryId;
	
	return returnIds;
}