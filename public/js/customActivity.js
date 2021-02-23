define([
    'postmonger',
    'jquery'
], function(
    Postmonger
) {
    'use strict';

    var debug                       = true;
    var connection                  = new Postmonger.Session();
    var payload                     = {};
    var onlineSetupStepEnabled      = false;
    var instoreSetupStepEnabled     = false;
    var steps                       = [
        { "label": "Message Type", "key": "step0" },
        { "label": "Push Message Setup", "key": "step1", "active": false },
        { "label": "Offer Setup", "key": "step2", "active": false },
        { "label": "Summary", "key": "step3" }
    ];
    var currentStep = steps[0];
    var today = new Date();
    var currentTime = today.toGMTString();
    var todayDate = new Date().toISOString().slice(0,10);

    let fuelToken;

    if ( debug ) {
        console.log("Current Step is: " + currentStep);
    }

    let development = false;
    if (window.location.hostname == "localhost") {
        development = true;
    }    

    var input_field_dictionary= new Map(); 
    //"fieldname":["Step","CharacterLimit","Required", "Fixed Text"]
    input_field_dictionary.set("update_contacts", [0, 200, "always", "Data Extension Name"]);
    input_field_dictionary.set("widget_name", [0, 100, "always", "Widget Name"]);

    input_field_dictionary.set("message_target_send_datetime", [1, 100, "adhoc"]);
    input_field_dictionary.set("recurring_push_offer_time", [1, 100, "recurring"]);
    input_field_dictionary.set("recurring_push_offer_delay_days", [1, 100, "recurring"]);
    input_field_dictionary.set("message_seed_send_datetime", [1, 100, "always"]);
    input_field_dictionary.set("message_title", [1, 30, "always", "Title"]);
    input_field_dictionary.set("cell_code", [1, 16, "always", "Cell Code"]);
    input_field_dictionary.set("cell_name", [1, 100, "always", "Cell Name"]);
    input_field_dictionary.set("campaign_name", [1, 100, "always", "Campaign Name"]);
    input_field_dictionary.set("campaign_code", [1, 12, "always", "Campaign Code"]);
    input_field_dictionary.set("message_content", [1, 140, "always", "Message Content"]);
    input_field_dictionary.set("message_url", [1, 250, "Optional", "Deep Link URL (Leave Blank For Default)"]);

    input_field_dictionary.set("offer_short_content", [2, 30, "always", "Short Description"]);
    input_field_dictionary.set("offer_image_url", [2, 250, "always", "Image URL"]);
    input_field_dictionary.set("offer_more_info", [2, 1000, "always", "More Info/Terms & Conditions"]);
    input_field_dictionary.set("offer_click_through_url", [2, 250, "always", "Click Through URL"]);
    input_field_dictionary.set("offer_info_button_text", [2, 20, "informational", "Info Button Text"]);
    input_field_dictionary.set("offer_id", [2, 16, "always", "Offer ID"]);
    input_field_dictionary.set("offer_start_datetime", [2, 100, "adhoc"]);
    input_field_dictionary.set("offer_end_datetime", [2, 100, "adhoc"]);
    input_field_dictionary.set("recurring_offer_time", [2, 100, "recurring"]);
    input_field_dictionary.set("recurring_offer_delay_days", [2, 100, "recurring"]);
    input_field_dictionary.set("recurring_offer_validity_days", [2, 100, "recurring"]);
    input_field_dictionary.set("offer_cell_code", [2, 16, ["informational", "recurring"], "Cell Code"]);
    input_field_dictionary.set("offer_cell_name", [2, 100, ["informational", "recurring"], "Cell Name"]);
    input_field_dictionary.set("offer_campaign_name", [2, 100, ["informational", "recurring"], "Campaign Name"]);
    input_field_dictionary.set("offer_campaign_code", [2, 12, ["informational", "recurring"], "Campaign Code"]);

    
    //Running Character Counts on Input Fields
    $("[type='text']").on("input", function(){
        var field_name = this.id;
        var count_field_name = `${field_name}_count`;
        if (document.getElementById(count_field_name)){
            var text_length = document.getElementById(field_name).value.length;
            var max_length = input_field_dictionary.get(field_name)[1];
            var fixed_text = input_field_dictionary.get(field_name)[3];
            document.getElementById(count_field_name).innerText = `${fixed_text}: ${text_length} characters`;
            if (text_length > max_length){
              document.getElementById(count_field_name).style.color = 'red';
            }else {
                document.getElementById(count_field_name).style.color = 'black';
            }
        }
    });
    
    $("textarea").on("input", function(){
        var field_name = this.id;
        var count_field_name = `${field_name}_count`;
        if (document.getElementById(count_field_name)){
            var text_length = document.getElementById(field_name).value.length;
            var max_length = input_field_dictionary.get(field_name)[1];
            var fixed_text = input_field_dictionary.get(field_name)[3];
            document.getElementById(count_field_name).innerText = `${fixed_text}: ${text_length} characters`;
            if (text_length > max_length){
              document.getElementById(count_field_name).style.color = 'red';
            }else {
                document.getElementById(count_field_name).style.color = 'black';
            }
        }
    });

    $("#offer_promotion_filter").on("input", function(){
        var searchValue = document.getElementById("offer_promotion_filter").value.toLowerCase();
        $("#offer_promotion option").filter(function() {
            $(this).toggle($(this).text().toLowerCase().indexOf(searchValue) > -1)
        });
    });

    $("#voucher_group_filter").on("input", function(){
        var searchValue = document.getElementById("voucher_group_filter").value.toLowerCase();
        $("#recurring_voucher_group_id option").filter(function() {
            var voucherGroupName = $(this).text().toLowerCase();
            var voucherGroupfiltered = voucherGroupName.indexOf(searchValue) > -1;
            $(this).toggle(voucherGroupfiltered)
            //if (voucherGroupfiltered == 1){
            //    console.log(`${searchValue} found in ${voucherGroupName}`);
            //}
        });
    });

    if (development) {
        payload = {
            arguments: {
                execute: {
                    inArguments: []
                }
            },
            metaData: {
                isConfigured: false
            },
            name: ""
        }

        document.getElementById("dev-helper-buttons").removeAttribute("hidden");
        document.getElementById("dev-button-validate").onclick = onClickedNext;
        document.getElementById("dev-button-initial").onclick = function () { showStep({key:"step0"}); }
        document.getElementById("dev-button-push").onclick = function () { showStep({key:"step1"}); }
        document.getElementById("dev-button-offer").onclick = function () { showStep({key:"step2"}); }
        document.getElementById("dev-button-summary").onclick = function () { updateSummaryPage(buildActivityPayload()); showStep({key:"step3"}); }
        document.getElementById("dev-button-cache-save").onclick = function () { window.localStorage.setItem("activity-cache", JSON.stringify(CreateCachePayload())); }
        document.getElementById("dev-button-cache-load").onclick = function () { const data = window.localStorage.getItem("activity-cache"); initialize(JSON.parse(data)); }
    } else {
        document.getElementById("dev-helper-buttons").setAttribute("hidden", "");
    }

    $(window).ready(onRender);

    connection.on('initActivity', initialize);
    connection.on('requestedTokens', onReceivedTokens);

    connection.on('clickedNext', onClickedNext);
    connection.on('clickedBack', onClickedBack);
    connection.on('gotoStep', onGotoStep);

    async function onRender() {
        if (development) {
            onReceivedTokens({fuel2token: "testtoken"});
        } else {
            connection.trigger('requestTokens');
        }

        connection.trigger('requestEndpoints');
    }

    async function onReceivedTokens(tokens) {
        fuelToken = tokens.fuel2token;

        let lookupTasks = [
            lookupPromos(),
            lookupVoucherGroups(),
            lookupControlGroups(),
            lookupUpdateContacts()
        ];
        
        await Promise.all(lookupTasks);

        loadEvents();
        
        // JB will respond the first time 'ready' is called with 'initActivity'
        connection.trigger('ready');

        if (development) {
            showOrHideOfferFormsBasedOnType();
            showOrHideSeedsSelectionDate();
        }
    }

    function initialize (data) {
        
        if (data) {
            payload = data;
            var argumentsSummaryPayload = payload.arguments.execute.inArguments[0];
        }

        if ( debug ) {
            console.log("Payload is:");
            console.log(payload.arguments.execute.inArguments[0]);
            console.log("Summary payload is:");
            console.log(argumentsSummaryPayload.buildPayload);
            console.log("Promotion Meta Data is:");
            console.log(payload.metadata)
        }

        var hasInArguments = Boolean(
            payload['arguments'] &&
            payload['arguments'].execute &&
            payload['arguments'].execute.inArguments &&
            payload['arguments'].execute.inArguments.length > 0
        );

        var inArguments = hasInArguments ? payload['arguments'].execute.inArguments : {};

        if ( debug ) {
            console.log("In arguments object is:");
            console.log(inArguments);
            console.log("promotion type from arg is:");
            console.log(argumentsSummaryPayload.buildPayload);
        }

        if ( argumentsSummaryPayload.buildPayload ) {

            if ( debug ) {
                console.log("inside if statement i.e. promotion key is present")
                console.log(argumentsSummaryPayload.buildPayload);
            }

            const argPromotionType = argumentsSummaryPayload.buildPayload.find(element => element.key == "push_type").value;
            const argKey = argumentsSummaryPayload.buildPayload.find(element => element.key == "message_key_hidden")?.value;

            if (argKey) {
                $("#message_key_hidden").val(argKey);
                $("#main_setup_key").html(argKey);
                $("#control_action_save").html("Data has been sent");
                $("#control_action_save").prop('disabled', true);
                $("#control_action_seed").prop('disabled', false);
                $("#control_action_broadcast").prop('disabled', false);
            }

            const seedSent = argumentsSummaryPayload.buildPayload.find(element => element.key == "seed_sent")?.value;
            const isBroadcastString = argumentsSummaryPayload.buildPayload.find(element => element.key == "is_broadcast")?.value;
            const isCancelledString = argumentsSummaryPayload.buildPayload.find(element => element.key == "is_cancelled")?.value;

            const isBroadcast = (isBroadcastString == "true");
            const isCancelled = (isCancelledString == "true");

            if (isBroadcast && !isCancelled) {
                $("#control_action_cancel").prop('disabled', false);
            } else if (isCancelled) {
                // Disable all buttons if the campaign is already cancelled
                $("#control_action_save").prop('disabled', true);
                $("#control_action_update").prop('disabled', true);
                $("#control_action_seed").prop('disabled', true);
                $("#control_action_broadcast").prop('disabled', true);
                $("#control_action_cancel").prop('disabled', true);
                $("#control_action_cancel").html("Campaign has been cancelled");
            }

            // argument data present, pre pop and redirect to summary page
            prePopulateFields(argumentsSummaryPayload.buildPayload);

            // update summary page
            updateSummaryPage(argumentsSummaryPayload.buildPayload);

            // trigger steps
            triggerSteps(argPromotionType);
        }

        showOrHideOfferFormsBasedOnType();
        showOrHideSeedsSelectionDate();
    }

    function loadEvents() {

        // render relevant steps based on input
        $('.promotion_type').click(function() {

            var pushType = $("input[name='push_type']:checked").val();

            if ( debug ) {
                console.log(pushType);
            }

            if ( pushType.includes('message') ) {

                // hide control group field
                $("#control_group_box").show();
                $("#update_contact_box").show();

                $("#promotion_alert").hide();

                if (pushType === 'message_non_loyalty') {
                    $("#control_group_box").hide();
                    $("#update_contact_box").hide();
                }

                if ( debug ) {
                    console.log("trigger step 1");   
                }
                
                onlineSetupStepEnabled = true; // toggle status
                steps[1].active = true; // toggle active
                instoreSetupStepEnabled = false; // toggle status
                steps[2].active = false; // toggle active

                if ( debug ) {
                    console.log(onlineSetupStepEnabled);
                    console.log(instoreSetupStepEnabled);
                    console.log(steps);                    
                }

                connection.trigger('updateSteps', steps);

            } else if ( pushType === 'offer' ) {

                // hide control group field
                //$("#control_group_box").hide();
                $("#promotion_alert").show();
                $("#control_group_box").show();
                $("#update_contact_box").show();

                if ( debug ) {
                    console.log("trigger step 2");   
                }
                
                onlineSetupStepEnabled = false; // toggle status
                steps[1].active = false; // toggle active
                instoreSetupStepEnabled = true; // toggle status
                steps[2].active = true; // toggle active

                if ( debug ) {
                    console.log(onlineSetupStepEnabled);
                    console.log(instoreSetupStepEnabled);
                    console.log(steps);                    
                }

                connection.trigger('updateSteps', steps);
            }

        });

        // render relevant steps based on input
        $('#offer_channel').change(showOrHideOfferFormsBasedOnType);
        $('#recurring_type').change(showOrHideOfferFormsBasedOnType); 
        $('#seed_selection_type').change(showOrHideSeedsSelectionDate); 

        $('#offer_promotion').change(function() {
            // get data attributes from dd and prepop hidden fields
            const promoData = JSON.parse(decodeURI($("option:selected", this).attr("data-attribute-promotionobject")));
            
            $("#offer_promotion_type").val(promoData.promotiontype);
            $("#offer_online_promotion_type").val(promoData.onlinepromotiontype);
            $("#offer_online_code_1").val(promoData.global_code_1);
            $("#offer_instore_code_1").val(promoData.instore_code_1);
            $("#offer_unique_code_1").val(promoData.unique_code_1);
            $("#offer_mc_id_1").val(promoData.mc_id_1);
            $("#offer_mc_id_6").val(promoData.mc_id_6);
            $("#communication_key").val(promoData.communication_cell_id);
            $("#offer_redemptions").val(promoData.instore_code_1_redemptions);
            $("#offer_campaign_code").val(promoData.campaign_code);
            $("#offer_campaign_name").val(promoData.campaign_name);
            $("#offer_cell_code").val(promoData.cell_code);
            $("#offer_cell_name").val(promoData.cell_name);
        });

        // hide the tool tips on page load
        $('.slds-popover_tooltip').hide();

        // hide error messages
        $('.slds-form-element__help').hide();

        // select first input
        $("#push_type_offer").click();

        $("#control_action_save").click(function(){
           if (validateSummaryPage()) {
                $("#sent").val(true);
                saveToDataExtension(buildActivityPayload());
            }
        });

        $("#control_action_update").click(function(){
            if (validateSummaryPage()) {
            updateExistingRow(buildActivityPayload());
            }
        });

        $("#control_action_seed").click(async function(){
            if (validateSummaryPage()) {
            $("#control_action_seed").prop("disabled", true);
            await sendCampaignToSeeds(buildActivityPayload());
            $("#seed_sent").val(true);
            $("#control_action_seed").html("Resend to seeds");
            $("#control_action_seed").prop("disabled", false);
            }
        });

        $("#recurring_action_seed").click(async function(){
            if (validateSummaryPage()) {
            $("#recurring_action_seed").prop("disabled", true);
            await sendRecurringCampaignToSeeds(buildActivityPayload());
            $("#seed_sent").val(true);
            $("#recurring_action_seed").html("Resend recurring to seeds");
            $("#recurring_action_seed").prop("disabled", false);
            }
        });

        $("#control_action_broadcast").click(async function(){
            if (validateSummaryPage()) {
            await broadcastCampaign(buildActivityPayload());
            }
        });

        $("#recurring_action_broadcast").click(async function(){
            if (validateSummaryPage()) {
            $("#recurring_action_broadcast").prop("disabled", true);
            await createRecurringAutomation(buildActivityPayload());
            $("#is_broadcast").val(true);
            $("#recurring_action_broadcast").html("Recreate Automation");
            $("#recurring_action_broadcast").prop("disabled", false);
            }
        });

        $("#control_action_cancel").click(async function() {
            if (validateSummaryPage() && confirm("Please confirm you'd like to cancel the app offer/push.\n\nIf you would like to cancel a push campaign less than 2 hours before go-live, please directly contact mobilize.")) {
                await cancelAppCampaign();
                $("#control_action_cancel").prop("disabled", true);
                $("#is_cancelled").val(true);
            }
        });

        $("#current_time").html(currentTime);

        // set date inputs to todays date
        $("#message_target_send_datetime").val(todayDate);
        $("#message_seed_send_datetime").val(todayDate);
        $("#offer_start_datetime").val(todayDate);
        $("#offer_end_datetime").val(todayDate);

    }

    function updateApiStatus(endpointSelector, endpointStatus) {

        if (endpointStatus) {
            $(`#${endpointSelector} > div > div`).removeClass("slds-theme_info");
            $(`#${endpointSelector} > div > div > span:nth-child(2)`).removeClass("slds-icon-utility-info");
            $(`#${endpointSelector} > div > div`).addClass("slds-theme_success");
            $(`#${endpointSelector} > div > div > span:nth-child(2)`).addClass("slds-icon-utility-success");
            $(`#${endpointSelector} > div > div > span:nth-child(2) svg use`).attr("xlink:href", "/assets/icons/utility-sprite/svg/symbols.svg#success");
            $(`#${endpointSelector} > div > div > .slds-notify__content h2`).text($(`#${endpointSelector} > div > div > .slds-notify__content h2`).text().replace("Loading", "Loaded"));
        } else {
            $(`#${endpointSelector} > div > div`).removeClass("slds-theme_info");
            $(`#${endpointSelector} > div > div > span:nth-child(2)`).removeClass("slds-icon-utility-info");
            $(`#${endpointSelector} > div > div`).addClass("slds-theme_error");
            $(`#${endpointSelector} > div > div > span:nth-child(2)`).addClass("slds-icon-utility-error");
            $(`#${endpointSelector} > div > div > span:nth-child(2) svg use`).attr("xlink:href", "/assets/icons/utility-sprite/svg/symbols.svg#error");
            $(`#${endpointSelector} > div > div > .slds-notify__content h2`).text($(`#${endpointSelector} > div > div > .slds-notify__content h2`).text().replace("Loading", "Error Loading"));
        }
    }

    function showOrHideOfferFormsBasedOnType() {
        var recurringType = $("input[name='recurring_type']:checked").val();
        if (recurringType == 'recurring'){
            //Recurring Camapaigns
            $("#offer_cell_box").show();
            $("#offer_type").hide();
            $("#adhoc_timings_box").hide();
            $("#recurring_timings_box").show();
            $("#recurring_action_broadcast").show();
            $("#control_action_broadcast").hide();
            $("#recurring_action_seed").show();
            $("#control_action_seed").hide();
            $("#recurring_push_offer_time_box").show();
            $("#recurring_push_delay_days_box").show();
            $("#adhoc_push_target_send_datetime_box").hide();
            if ($("#offer_channel").val() == '3'){
                //Recurring Info Only
                $("#promotion_box").hide();
                $("#info_button_text_form").show();
            } else {
                //Recurring Vouchers
                $("#promotion_box").show();
                $("#voucher_group_element").show(); //Voucher groups or promotions
                $("#promotion_element").hide(); //Imagine a voucher world without promo widgets...
                $("#seed_selection_type").show(); //It would be so nice
                $("#info_button_text_form").hide();
            }     
        }
        else {
            //Adhoc Camapaigns
            $("#offer_type").show();
            $("#adhoc_timings_box").show();
            $("#recurring_timings_box").hide();
            $("#recurring_action_broadcast").hide();
            $("#control_action_broadcast").show();
            $("#recurring_action_seed").hide();
            $("#control_action_seed").show();
            $("#recurring_push_offer_time_box").hide();
            $("#recurring_push_delay_days_box").hide();
            $("#adhoc_push_target_send_datetime_box").show();
            if ( $("#offer_channel").val() == '3'){
                //Adhoc Info Only Offers
                $("#offer_cell_box").show();
                $("#promotion_box").hide();
                $("#info_button_text_form").show();
            } else {
                //Adhoc Voucher Offers
                $("#offer_cell_box").hide();
                $("#promotion_box").show();
                $("#voucher_group_element").hide(); //Voucher groups or promotions
                $("#promotion_element").show(); //Imagine a voucher world without promo widgets... 
                $("#seed_selection_type").hide(); //It would be so nice           
                $("#info_button_text_form").hide();
            }
        }
    }

    function showOrHideSeedsSelectionDate() {
        var seedSelectionToday = $("input[name='seed_selection_type']:checked").val();
        if (seedSelectionToday == 'today'){
            $("#seed_selection_date_element").hide();
        }else{
            $("#seed_selection_date_element").show();
        }
    }

    function prePopulateFields(argumentsSummaryPayload) {

        if ( debug) {
            console.log("payload sent to prepop function");
            console.log(argumentsSummaryPayload);
        }

        var q;

        for (q = 0; q < argumentsSummaryPayload.length; q++) {
            if (debug) {
                console.log("Prepop: " + argumentsSummaryPayload[q].key + ", with value: " + argumentsSummaryPayload[q].value + ", and type: " + argumentsSummaryPayload[q].type);
            }
            if (argumentsSummaryPayload[q].type == "checkbox") {

                if (argumentsSummaryPayload[q].value) {
                    $("#" + argumentsSummaryPayload[q].key).val(true);
                    $("#" + argumentsSummaryPayload[q].key).prop('checked', "checked");
                }

            } else if (argumentsSummaryPayload[q].type == "radio") {
                if (argumentsSummaryPayload[q].key == "push_type") {
                    if (argumentsSummaryPayload[q].value == "message") {
                        $("#push_type_message").prop('checked', true);
                        $("#push_type_message").click();
                    }
                    else if (argumentsSummaryPayload[q].value == "offer") {
                        $("#push_type_offer").prop('checked', true);
                        $("#push_type_offer").click();
                    }
                    else if (argumentsSummaryPayload[q].value == "message_non_loyalty") {
                        $("#push_type_message_non_loyalty").prop('checked', true);
                        $("#push_type_message_non_loyalty").click();
                    } 
                }
                else if (argumentsSummaryPayload[q].key == "recurring_type") {
                    if (argumentsSummaryPayload[q].value == "adhoc") {
                        $("#recurring_type_adhoc").prop('checked', true);
                        $("#recurring_type_adhoc").click();
                    }
                    else if (argumentsSummaryPayload[q].value == "recurring") {
                        $("#recurring_type_recurring").prop('checked', true);
                        $("#recurring_type_recurring").click();
                    }
                }
            }

            $("#step" + (argumentsSummaryPayload[q].step - 1) + " #" + argumentsSummaryPayload[q].key).val(argumentsSummaryPayload[q].value);

        }
    }

    function triggerSteps(argPromotionType) {

        // argument data present, pre pop and redirect to summary page
        var prepopPromotionType = argPromotionType;

        if ( debug ) {
            console.log("prepopPromotionType is");
            console.log(prepopPromotionType);
        }

        var prePop;

        if ( prepopPromotionType == 'message' || prepopPromotionType == 'message_non_loyalty' ) {
            steps[1].active = true;
            steps[3].active = true;
            connection.trigger('updateSteps', steps);
            setTimeout(function() {
                connection.trigger('nextStep');
            }, 10);
            setTimeout(function() {
                connection.trigger('nextStep');
            }, 20);
            setTimeout(function() {
                showStep(null, 3);
            }, 30);
        } else if ( prepopPromotionType == 'offer' ) {
            steps[2].active = true;
            steps[3].active = true;
            connection.trigger('updateSteps', steps);
            setTimeout(function() {
                connection.trigger('nextStep');
            }, 10);
            setTimeout(function() {
                connection.trigger('nextStep');
            }, 20);
            setTimeout(function() {
                showStep(null, 3);
            }, 30);
        } else {
            if ( debug ) {
                console.log('nothing to pre-pop setting step 0 and first radio checked');
            }
            $("#push_type_offer").prop("checked", true).trigger("click");
        }
        if ( debug ) {
            console.log(prePop);
        }

    }

    function validateStep(stepToValidate, currentPage) {

        if (!currentPage) {
            currentPage = stepToValidate
        }

        if (debug) {
            console.log(`Step ${stepToValidate} to be validated`);
        }

        if ( $("#step" + stepToValidate).find('.slds-has-error').length > 0 ) {
            return false;

        } else {
            var ErrorCount = 0;
            var recurringType = $("input[name='recurring_type']:checked").val();
            var seedSelectionToday = $("input[name='seed_selection_type']:checked").val();
            for (let [field_name, field_name_info] of  input_field_dictionary.entries()) {
                if (stepToValidate == field_name_info[0]){
                    console.log("The selector is " + field_name);
                    if ( !document.getElementById(field_name).value && 
                            (   field_name_info[2]=="always"
                            ||  (document.getElementById("offer_channel").value == '3' && field_name_info[2].includes("informational"))
                            ||  field_name_info[2].includes(recurringType)
                            )){
                        document.getElementById(`step${currentPage}alerttext`).innerText = `The field ${field_name} is missing.`;
                        console.log(`The field ${field_name} is missing.`);
                        ErrorCount++;
                    } else if ( document.getElementById(field_name).value.length > field_name_info[1] ) {
                        document.getElementById(`step${currentPage}alerttext`).innerText = `The character limit of ${field_name} is ${field_name_info[1]}.`;
                        console.log(`The character limit of ${field_name} is ${field_name_info[1]}.`);
                        ErrorCount++;
                    }
                }
            }
            if ( stepToValidate == 2 && document.getElementById("offer_channel").value != '3' && document.getElementById("offer_promotion").value == "no-code" && recurringType == "adhoc") {
                    document.getElementById(`step${currentPage}alerttext`).innerText = "Voucher offers must have an Offer Promotion"
                    console.log("Voucher offers must have an Offer Promotion");
                    ErrorCount++;  
            }
            if ( stepToValidate == 2 && document.getElementById("offer_channel").value != '3' && document.getElementById("recurring_voucher_group_id").value == "no-code" && recurringType == "recurring") {
                document.getElementById(`step${currentPage}alerttext`).innerText = "Voucher offers must have an Voucher Group"
                console.log("Voucher offers must have an Voucher Group");
                ErrorCount++;  
            }
            if ( stepToValidate == 2 && document.getElementById("offer_channel").value != '3' && !document.getElementById("seed_selection_date").value  && recurringType == "recurring" && seedSelectionToday == "specific_date") {
                document.getElementById(`step${currentPage}alerttext`).innerText = "Choose a date to treat seeds as if selected on that date"
                console.log("Choose a date to treat seeds as if selected on that date");
                ErrorCount++;  
            }
            if ( stepToValidate == 0 && document.getElementById("update_contacts").value == "none" && !$("#push_type_message_non_loyalty").is(":checked")) {
                document.getElementById(`step${currentPage}alerttext`).innerText = "An update contact data extension is required for offers and loyalty pushes";
                console.log("An update contact data extension is required for offers and loyalty pushes");
                ErrorCount++;
            }
            if ( stepToValidate == 1 && recurringType == "adhoc" && (document.getElementById("message_target_send_datetime").value < document.getElementById("message_seed_send_datetime").value)){
                document.getElementById(`step${currentPage}alerttext`).innerText = "The target send datetime must be after the seed send datetime.";
                console.log("The target send datetime must be after the seed send datetime.");
			     ErrorCount++;
            }
            
            if ( ErrorCount == 0 ) {
                return true;
            } else {
                return false;
            }
        }
    }    

    function validateSummaryPage() {
       
        var pushType = $("#step0 .slds-radio input[name='push_type']:checked").val();
        
        if (pushType.includes('message') && validateStep(0, 3) && validateStep(1, 3)) {
            return true;

        } else if (pushType == "offer" && validateStep(0, 3) && validateStep(2, 3)) {
                return true;

        } else {
            toggleStepError(3,"show");
            return false;
            
        }        
    }
    

    async function lookupControlGroups() {
        try {
            // access offer types and build select input
            let result = await $.ajax({
                url: "/dataextension/lookup/controlgroups",
                headers: {
                    Authorization: `Bearer ${fuelToken}`
                }
            });

            if (debug) {
                console.log('lookup control groups executed');
                console.log(result.items);
            }

            var i;
            for (i = 0; i < result.items.length; ++i) {
                if (debug) {
                    console.log(result.items[i]);
                }

                let de = result.items[i].values;
                $("#control_group").append("<option value=" + encodeURI(de.dataextensionname) + ">" + de.dataextensionname + "</option>");
            }
            updateApiStatus("controlgroup-api", true);
        } catch {
            updateApiStatus("controlgroup-api", false);
        }
    }

    async function lookupPromos() {
        try{
            // access offer types and build select input
            let result = await $.ajax({
                url: "/dataextension/lookup/promotions",
                headers: {
                    Authorization: `Bearer ${fuelToken}`
                }
            });

            console.log('lookup promotions executed');
            console.log(result.items); 
            var i;
            if ( result.items ) {
                for (i = 0; i < result.items.length; ++i) {
                    
                    console.log(result.items[i].keys); 
                    const deRow = result.items[i].values;
                    const promotionKey = result.items[i].keys.promotion_key;
                    const deRowData = encodeURI(JSON.stringify(deRow));

                    if (deRow.promotiontype != "nocode"){
                        $("#offer_promotion").append(`<option data-attribute-promotionobject=${deRowData} value=${promotionKey}>${deRow.cell_name}</option>`);
                    }
                }
            }

            updateApiStatus("promotions-api", true);
        } catch (error) {
            updateApiStatus("promotions-api", false);
        }
        
    }

    async function lookupVoucherGroups() {
        try{
            // access offer types and build select input
            let result = await $.ajax({
                url: "/dataextension/lookup/vouchergroups",
                headers: {
                    Authorization: `Bearer ${fuelToken}`
                }
            });

            console.log('lookup voucher groups executed');
            console.log(result.items); 
            var i;
            if ( result.items ) {
                for (i = 0; i < result.items.length; ++i) {
                    
                    console.log(result.items[i].keys); 
                    const deRow = result.items[i].values;
                    const voucherGroupId = result.items[i].keys.voucher_group_id;
                    $("#recurring_voucher_group_id").append(`<option value=${voucherGroupId}>${deRow.voucher_group_name} - ${voucherGroupId}</option>`);
               }
            }

            updateApiStatus("vouchergroup-api", true);
        } catch (error) {
            updateApiStatus("vouchergroup-api", false);
        }
        
    }

    async function lookupUpdateContacts() {
        try {
            // access offer types and build select input
            let result = await $.ajax({
                url: "/dataextension/lookup/updatecontacts",
                headers: {
                    Authorization: `Bearer ${fuelToken}`
                }
            });

            if (debug) {
                console.log('lookup update contacts executed');
                console.log(result.items);
            }

            var i;
            for (i = 0; i < result.items.length; ++i) {
                if (debug) {
                    console.log(result.items[i]);
                }

                let de = result.items[i].values;
                if (de.channel == 'App'){
                    $("#update_contacts").append("<option value=" + encodeURI(de.dataextensionname) + ">" + de.dataextensionname + "</option>");
                }
            }
            updateApiStatus("updatecontacts-api", true);
        } catch (error) {
            updateApiStatus("updatecontacts-api", false);
        }        
    }

    function toggleStepError(errorStep, errorStatus) {

        if ( debug ) {
            console.log("error step is " + errorStep + " and error status is " + errorStatus);
        }

        if ( errorStatus == "show" ) {
            $("#step" + errorStep + "alert").show();
        } else {
            $("#step" + errorStep + "alert").hide();
        }
    }

    function onClickedNext () {

        var pushType = $("#step0 .slds-radio input[name='push_type']:checked").val();

        if ( debug ) {
            console.log(pushType);
            console.log(currentStep.key);
            console.log("next clicked");           
        }

        if ( pushType.includes('message')) {

            if ( currentStep.key === 'step0') {

                if ( validateStep(0) ) {

                    if ( debug ) {
                        console.log("step 0 validated");
                    }

                    toggleStepError(0, "hide");
                    connection.trigger('nextStep');

                } else {

                    if ( debug ) {
                        console.log("step 0 not validated");           
                    }

                    connection.trigger('ready');
                    toggleStepError(0, "show");

                }

            } else if ( currentStep.key === 'step1' ) {

                if ( validateStep(1) ) {

                    if ( debug ) {
                        console.log("step 1 validated");           
                    }                    

                    toggleStepError(1, "hide");
                    updateSummaryPage(buildActivityPayload());
                    connection.trigger('nextStep');

                } else {

                    if ( debug ) {
                        console.log("step 1 not validated");           
                    }  

                    connection.trigger('ready');
                    toggleStepError(1, "show");

                }

            } else if ( currentStep.key === 'step3' ) {

                if ( debug ) {
                    console.log("Close and save in cache");
                }
                save();

            } else {

                connection.trigger('nextStep');

            }

        } else if ( pushType == 'offer' ) {

            if ( currentStep.key === 'step0') {

                if ( validateStep(0) ) {

                    if ( debug ) {
                        console.log("step 0 validated");           
                    }                    

                    toggleStepError(0, "hide");
                    connection.trigger('nextStep');

                } else {

                    if ( debug ) {
                        console.log("step 0 not validated");           
                    }  

                    connection.trigger('ready');
                    toggleStepError(0, "show");

                }

            } else if ( currentStep.key === 'step2' ) {

                if ( validateStep(2) ) {

                    if ( debug ) {
                        console.log("step 2 validated");           
                    }                    

                    toggleStepError(2, "hide");
                    updateSummaryPage(buildActivityPayload());
                    connection.trigger('nextStep');

                } else {

                    if ( debug ) {
                        console.log("step 2 not validated");           
                    }  

                    connection.trigger('ready');
                    toggleStepError(2, "show");

                }

            } else if ( currentStep.key === 'step3' ) {

                if ( debug ) {
                    console.log("Close and save in cache");
                }
                save();      

            } else {

                connection.trigger('nextStep');
            }

        }
    }

    function onClickedBack () {
        connection.trigger('prevStep');
    }

    function onGotoStep (step) {

        if ( debug ) {
            console.log(step);
        }
        
        showStep(step);
        connection.trigger('ready');

    }

    function showStep(step, stepIndex) {

        if ( debug ) {
            console.log(step);
            console.log(stepIndex);
        }

        if (stepIndex && !step) {
            step = steps[stepIndex];
        }

        currentStep = step;

        if ( debug ) {
            console.log(currentStep);
        }

        $('.step').hide();

        switch(currentStep.key) {
            case 'step0':
                if ( debug ) {
                    console.log("step0 case hit");
                }
                $('#step0').show();
                connection.trigger('updateButton', {
                    button: 'next',
                    //enabled: Boolean(getMessage())
                });
                connection.trigger('updateButton', {
                    button: 'back',
                    visible: false
                });
                break;
            case 'step1':

                if ( debug ) {
                    console.log("step 1 case clicked");
                }

                $('#step1').show();
                connection.trigger('updateButton', {
                    button: 'back',
                    visible: true
                });
                if (onlineSetupStepEnabled) {
                    connection.trigger('updateButton', {
                        button: 'next',
                        text: 'next',
                        visible: true
                    });
                } else {
                    connection.trigger('updateButton', {
                        button: 'next',
                        text: 'next',
                        visible: true
                    });
                }
                break;
            case 'step2':

                if ( debug ) {
                    console.log("step 2 case clicked");
                }

                $('#step2').show();
                connection.trigger('updateButton', {
                     button: 'back',
                     visible: true
                });
                if (instoreSetupStepEnabled) {
                    connection.trigger('updateButton', {
                        button: 'next',
                        text: 'next',
                        visible: true
                    });
                } else {
                    connection.trigger('updateButton', {
                        button: 'next',
                        text: 'next',
                        visible: true
                    });
                }
                break;
            case 'step3':

                if ( debug ) {
                    console.log("step 3 case clicked");
                }

                $('#step3').show();
                connection.trigger('updateButton', {
                    button: 'next',
                    text: 'done'
                    //enabled: Boolean(getMessage())
                });
                connection.trigger('updateButton', {
                    button: 'back',
                    visible: true
                });
                break;
        }
    }

    /*
     * Function add data to data extension
     */

    function saveToDataExtension(payloadToSave) {

        if ( debug ) {
            console.log("Data Object to be saved is: ");
            console.log(payloadToSave);
        }

        try {
            $.ajax({ 
                url: '/dataextension/add',
                headers: {
                    Authorization: `Bearer ${fuelToken}`
                },
                type: 'POST',
                data: JSON.stringify(payloadToSave),
                contentType: 'application/json',                     
                success: function(data) {
                    console.log('success');
                    toggleStepError(3, "hide");
                    console.log(data);
                    $("#message_key_hidden").val(data);
                    $("#main_setup_key").html(data);
                    $("#control_action_save").html("Data has been sent");
                    $("#control_action_save").prop('disabled', true);
                    $("#control_action_update").prop('disabled', false);
                    $("#control_action_seed").prop('disabled', false);
                    $("#recurring_action_seed").prop('disabled', false);
                    $("#control_action_broadcast").prop('disabled', false);
                }
                , error: function(jqXHR, textStatus, err){
                        console.log(err);
                        document.getElementById("step3alerttext").innerText = jqXHR.responseText;
                        toggleStepError(3, "show");
                        
                }
            }); 
        } catch(e) {
            console.log("Error saving data");
            console.log(e);
            toggleStepError(3, "show");
        }

    }

    /*
     * Function add data to data extension
     */

    function updateExistingRow(payloadToSave) {

        if ( debug ) {
            console.log("Data Object to be saved is: ");
            console.log(payloadToSave);  
        }

        try {
            $.ajax({ 
                url: '/dataextension/update',
                headers: {
                    Authorization: `Bearer ${fuelToken}`
                },
                type: 'POST',
                data: JSON.stringify(payloadToSave),
                contentType: 'application/json',                     
                success: function(data) {
                    console.log('success');
                    console.log(data);
                    toggleStepError(3, "hide");
                    $("#control_action_save").html("Data has been updated");
                    $("#control_action_update").prop('disabled', false);
                    $("#control_action_seed").prop('disabled', false);
                    $("#recurring_action_seed").prop('disabled', false);
                    $("#control_action_broadcast").prop('disabled', false);
                }
                , error: function(jqXHR, textStatus, err){
                        console.log(err);
                        document.getElementById("step3alerttext").innerText = jqXHR.responseText;
                        toggleStepError(3, "show");
                }
            }); 
        } catch(e) {
            console.log("Error saving data");
            console.log(e);
            toggleStepError(3, "show");
            
            
        }

    }

    async function sendCampaignToSeeds(payloadToSave) {

        if ( debug ) {
            console.log("Data Object to be saved is: ");
            console.log(payloadToSave);
        }

        try {
            const data = await $.ajax({ 
                url: '/send/seed',
                headers: {
                    Authorization: `Bearer ${fuelToken}`
                },
                type: 'POST',
                data: JSON.stringify(payloadToSave),
                contentType: 'application/json'
            });

            console.log('success');
            console.log(data);
            toggleStepError(3, "hide");

        } catch(e) {
            console.log("Error saving data");
            console.log(e);
            document.getElementById("step3alerttext").innerText = e.responseText;
            toggleStepError(3, "show");
        }
    }

    async function broadcastCampaign(payloadToSave) {

        if ( debug ) {
            console.log("Data Object to be saved is: ");
            console.log(payloadToSave);
        }

        try {            
            $("#control_action_broadcast").prop('disabled', true);
            const data = await $.ajax({ 
                url: '/send/broadcast',
                headers: {
                    Authorization: `Bearer ${fuelToken}`
                },
                type: 'POST',
                data: JSON.stringify(payloadToSave),
                contentType: 'application/json'
            });

            $("#is_broadcast").val(true);
            $("#control_action_broadcast").html("Scheduled");
            $("#control_action_cancel").prop('disabled', false);
            console.log('success');
            console.log(data);
            toggleStepError(3, "hide");
            $("#query_key_hidden").val(data);

        } catch(e) {
            console.log("Error saving data");
            console.log(e);
            document.getElementById("step3alerttext").innerText = e.responseText;
            toggleStepError(3, "show");
            $("#control_action_broadcast").prop('disabled', false);
        }
    }

    async function sendRecurringCampaignToSeeds(payloadToSave) {

        if ( debug ) {
            console.log("Data Object to be saved is: ");
            console.log(payloadToSave);
        }

        try {            
            const data = await $.ajax({ 
                url: '/send/recurringseed',
                headers: {
                    Authorization: `Bearer ${fuelToken}`
                },
                type: 'POST',
                data: JSON.stringify(payloadToSave),
                contentType: 'application/json'
            });
;
            console.log('success');
            console.log(data);
            toggleStepError(3, "hide");
            $("#query_key_hidden").val(data);

        } catch(e) {
            console.log("Error saving data");
            console.log(e);
            document.getElementById("step3alerttext").innerText = e.responseText;
            toggleStepError(3, "show");
        }
    } 

    async function createRecurringAutomation(payloadToSave) {

        if ( debug ) {
            console.log("Data Object to be saved is: ");
            console.log(payloadToSave);
        }

        try {            
            //$("#control_action_broadcast").prop('disabled', true);
            const data = await $.ajax({ 
                url: '/send/createautomation',
                headers: {
                    Authorization: `Bearer ${fuelToken}`
                },
                type: 'POST',
                data: JSON.stringify(payloadToSave),
                contentType: 'application/json'
            });

            //$("#is_broadcast").val(true);
            //$("#control_action_broadcast").html("Scheduled");
            //$("#control_action_cancel").prop('disabled', false);
            console.log('success');
            console.log(data);
            toggleStepError(3, "hide");
            $("#query_key_hidden").val(data);

        } catch(e) {
            console.log("Error saving data");
            console.log(e);
            document.getElementById("step3alerttext").innerText = e.responseText;
            toggleStepError(3, "show");
            //$("#control_action_broadcast").prop('disabled', false);
        }
    }

    async function cancelAppCampaign() {
        const messageKey = $("#message_key_hidden").val();

        try {
            const result = await $.ajax({
                url: `/cancel/${messageKey}`,
                type: "POST",
                headers: {
                    Authorization: `Bearer ${fuelToken}`
                }
            });
            console.log(result);
            toggleStepError(3, "hide");
        } catch (error) {
            console.log("Error cancelling campaign.");
            console.log(error);
            document.getElementById("step3alerttext").innerText = error.responseText;
            toggleStepError(3, "show");
        }
    }

    function buildActivityPayload() {

        var step1FormInputs = $("#step0").find(":input");
        var step2FormInputs = $("#step1").find(":input");
        var step3FormInputs = $("#step2").find(":input");

        var i;
        var payloadNode = [];

        for ( i = 0; i < step1FormInputs.length; i++ ) {
            if ( step1FormInputs[i].id) {
                if ( step1FormInputs[i].type == "checkbox") {
                    if ( step1FormInputs[i].checked ) {
                        payloadNode.push({
                            step: 1,
                            key: step1FormInputs[i].id, 
                            value:  step1FormInputs[i].checked,
                            type: "checkbox"
                        });
                    }
                } else if ( step1FormInputs[i].type == "radio" ) {
                    if ( step1FormInputs[i].checked ) {
                        payloadNode.push({
                            step: 1,
                            key: step1FormInputs[i].name, 
                            value:  step1FormInputs[i].value,
                            type: "radio"
                        });
                    }
                } else {
                    if ( step1FormInputs[i].value ) {
                        payloadNode.push({
                            step: 1,
                            key: step1FormInputs[i].id, 
                            value:  step1FormInputs[i].value.replace(/\n|\r/g, ""),
                            type: "input"
                        });  
                    }
                }
            }
        }

        for ( i = 0; i < step2FormInputs.length; i++ ) {
            if ( step2FormInputs[i].id) {
                if ( step2FormInputs[i].type == "checkbox") {
                    if ( step2FormInputs[i].checked ) {
                        payloadNode.push({
                            step: 2,
                            key: step2FormInputs[i].id, 
                            value:  step2FormInputs[i].checked,
                            type: "checkbox"
                        });
                    }
                } else if ( step2FormInputs[i].type == "radio" ) {
                    if ( step2FormInputs[i].checked ) {
                        payloadNode.push({
                            step: 2,
                            key: step2FormInputs[i].name, 
                            value:  step2FormInputs[i].value,
                            type: "radio"
                        });
                    }
                } else {
                    if ( step2FormInputs[i].value ) {
                        payloadNode.push({
                            step: 2,
                            key: step2FormInputs[i].id, 
                            value:  step2FormInputs[i].value.replace(/\n|\r/g, ""),
                            type: "input"
                        });                       
                    }
                }
            }
        }

        for ( i = 0; i < step3FormInputs.length; i++ ) {
            if ( step3FormInputs[i].id) {
                if ( step3FormInputs[i].type == "checkbox") {
                    if ( step3FormInputs[i].checked ) {
                        payloadNode.push({
                            step: 3,
                            key: step3FormInputs[i].id, 
                            value:  step3FormInputs[i].checked,
                            type: "checkbox"
                        });
                    }
                } else if ( step3FormInputs[i].type == "radio" ) {
                    if ( step3FormInputs[i].checked ) {
                        payloadNode.push({
                            step: 3,
                            key: step3FormInputs[i].name, 
                            value:  step3FormInputs[i].value,
                            type: "radio"
                        });
                    }
                } else {
                    if ( step3FormInputs[i].value ) {
                        payloadNode.push({
                            step: 3,
                            key: step3FormInputs[i].id, 
                            value:  step3FormInputs[i].value.replace(/\n|\r/g, ""),
                            type: "input"
                        });                       
                    }
                }
            }
        }

        if ( debug ) {
            console.log(payloadNode);
        }

        return payloadNode;

    }

    function updateSummaryPage(summaryPayload) {

        $("#summary-main-setup, #summary-message-setup, #summary-offer-setup").empty();

        if ( debug ) {
            console.log("Build Payload for summary update it")
            console.log(summaryPayload);
        }
 
        var z = 0;

        for ( z = 0; z < summaryPayload.length; z++ ) {

            if ( summaryPayload[z].value != "no-code" ) {

                if ( summaryPayload[z].step == 1 ) {

                    if ( summaryPayload[z].key == "push_type" ) {
                        var summaryPromotionType = summaryPayload[z].value;
                        if ( summaryPromotionType.includes("message")) {
                            $("#summary-offer-setup").append('<p>No offer setup.</p>');
                        } else if ( summaryPromotionType == "offer" ) {
                            $("#summary-message-setup").append('<p>No message setup.</p>');
                        }
                    } else if ( summaryPayload[z].key == "control_group") {

                        $("#control_group_data_extension").text(summaryPayload[z].value);

                    } else if ( summaryPayload[z].key == "update_contacts" ) {

                        $("#update_contact_data_extension").text(summaryPayload[z].value);

                    }

                    $("#summary-main-setup").append('<dt class="slds-item_label slds-text-color_weak" title="'+summaryPayload[z].key+'"><b>'+cleanUpKeyText(summaryPayload[z].key)+'</b></dt>');
                    $("#summary-main-setup").append('<dd class="slds-item_detail" title="Description for '+summaryPayload[z].value+'">'+cleanUpValueText(summaryPayload[z].value)+'</dd>');

                } else if ( summaryPayload[z].step == 2 ) {

                    if ( summaryPromotionType.includes("message")) {

                        $("#summary-message-setup").append('<dt class="slds-item_label slds-text-color_weak" title="'+summaryPayload[z].key+'"><b>'+cleanUpKeyText(summaryPayload[z].key)+'</b></dt>');
                        $("#summary-message-setup").append('<dd class="slds-item_detail" title="Description for '+summaryPayload[z].value+'">'+summaryPayload[z].value+'</dd>');

                    }              

                } else if ( summaryPayload[z].step == 3 ) {

                    if ( summaryPromotionType == "offer" ) {

                        $("#summary-offer-setup").append('<dt class="slds-item_label slds-text-color_weak" title="'+summaryPayload[z].key+'"><b>'+cleanUpKeyText(summaryPayload[z].key)+'</b></dt>');
                        $("#summary-offer-setup").append('<dd class="slds-item_detail" title="Description for '+summaryPayload[z].value+'">'+summaryPayload[z].value+'</dd>');
                    
                    }     
                }
            }
        } 
        
    }

    function cleanUpKeyText(keyString) {
        return keyString.split("_").join(" ");
    }

    function cleanUpValueText(valueString) {
        return decodeURI(valueString);
    }

    function save() {
        const cachePayload = CreateCachePayload();

        // trigger payload save
        connection.trigger('updateActivity', cachePayload);
    }
    
    function CreateCachePayload() {

        const buildPayload = buildActivityPayload();
        const argPromotionKey = buildPayload.find(element => element.key == "message_key_hidden")?.value;

        console.log("arg key");
        console.log(argPromotionKey);
        // 'payload' is initialized on 'initActivity' above.
        // Journey Builder sends an initial payload with defaults
        // set by this activity's config.json file.  Any property
        // may be overridden as desired.
        payload.name = $("#widget_name").val();
        payload['arguments'].execute.inArguments = [{ buildPayload }];
        // set isConfigured to true
        if (argPromotionKey) {
            // this is only true is the app returned a key
            // sent to de and configured
            payload['metaData'].isConfigured = true;
        }
        else {
            // not sent to de but configured
            payload['metaData'].isConfigured = false;
        }

        return payload;
    }
});
