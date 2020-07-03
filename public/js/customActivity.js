define([
    'postmonger',
    'jquery'
], function(
    Postmonger
) {
    'use strict';

    var debug                       = true;
    var apiWaitTime                 = 500;
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

    if ( debug ) {
        console.log("Current Step is: " + currentStep);
    }

    if (window.location.hostname == "localhost") {
        document.getElementById("dev-helper-buttons").removeAttribute("hidden");
        document.getElementById("dev-button-validate").onclick = onClickedNext;
        document.getElementById("dev-button-initial").onclick = function () { showStep({key:"step0"}); }
        document.getElementById("dev-button-push").onclick = function () { showStep({key:"step1"}); }
        document.getElementById("dev-button-offer").onclick = function () { showStep({key:"step2"}); }
        document.getElementById("dev-button-summary").onclick = function () { updateSummaryPage(buildActivityPayload()); showStep({key:"step3"}); }
    } else {
        document.getElementById("dev-helper-buttons").setAttribute("hidden");
    }

    $(window).ready(onRender);

    connection.on('initActivity', initialize);
    connection.on('requestedTokens', onGetTokens);
    connection.on('requestedEndpoints', onGetEndpoints);

    connection.on('clickedNext', onClickedNext);
    connection.on('clickedBack', onClickedBack);
    connection.on('gotoStep', onGotoStep);

    async function onRender() {
        
        connection.trigger('requestTokens');
        connection.trigger('requestEndpoints');
        
        let lookupTasks = [
            lookupPromos(),
            lookupControlGroups(),
            lookupUpdateContacts()
        ];
        
        await Promise.all(lookupTasks);

        loadEvents();
        
        // JB will respond the first time 'ready' is called with 'initActivity'
        connection.trigger('ready');
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

            var r;
            var argPromotionType;
            var argKey;

            for ( r = 0; r < argumentsSummaryPayload.buildPayload.length; r++ ) {
                if ( argumentsSummaryPayload.buildPayload[r].key == "push_type" ) {
                    argPromotionType = argumentsSummaryPayload.buildPayload[r].value; 
                } else if ( argumentsSummaryPayload.buildPayload[r].key == "message_key_hidden" && argumentsSummaryPayload.buildPayload[r].value ) {
                    argKey = argumentsSummaryPayload.buildPayload[r].value;
                    $("#message_key_hidden").val(argKey);
                    $("#main_setup_key").html(argKey);
                    $("#control_action_save").html("Data has been sent");
                    $("#control_action_save").prop('disabled', true);
                    $("#control_action_seed").prop('disabled', false);
                    $("#control_action_create").prop('disabled', false);                 
                } else if ( argumentsSummaryPayload.buildPayload[r].key == "seed_sent") {
                    console.log("seed sent value is");
                    console.log(argumentsSummaryPayload.buildPayload[r].value);
                    //$("#control_action_seed").html("Automation Created");
                    //$("#control_action_seed").prop('disabled', true); 

                } else if ( argumentsSummaryPayload.buildPayload[r].key == "automation_sent") {
                    console.log("automation sent value is");
                    console.log(argumentsSummaryPayload.buildPayload[r].value);
                    //$("#control_action_create").html("Automation Created");
                    //$("#control_action_create").prop('disabled', true); 

                }
            }

            // argument data present, pre pop and redirect to summary page
            prePopulateFields(argumentsSummaryPayload.buildPayload);

            // update summary page
            updateSummaryPage(argumentsSummaryPayload.buildPayload);

            // trigger steps
            triggerSteps(argPromotionType);
        }

        showOrHideOfferFormsBasedOnType();
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

        $('#offer_promotion').change(function() {
            // get data attributes from dd and prepop hidden fields
            $("#offer_promotion_type").val($("option:selected", this).attr("data-attribute-promotion-type"));
            $("#offer_online_promotion_type").val($("option:selected", this).attr("data-attribute-online-promotion-type"));
            $("#offer_online_code_1").val($("option:selected", this).attr("data-attribute-online-code"));
            $("#offer_instore_code_1").val($("option:selected", this).attr("data-attribute-instore-code"));
            $("#offer_unique_code_1").val($("option:selected", this).attr("data-attribute-voucher-pot"));
            $("#offer_mc_id_1").val($("option:selected", this).attr("data-attribute-mc1"));
            $("#offer_mc_id_6").val($("option:selected", this).attr("data-attribute-mc6"));
            $("#communication_key").val($("option:selected", this).attr("data-attribute-cell"));
            $("#offer_redemptions").val($("option:selected", this).attr("data-attribute-redemptions"));
        });

        // hide the tool tips on page load
        $('.slds-popover_tooltip').hide();

        // hide error messages
        $('.slds-form-element__help').hide();

        // select first input
        $("#push_type_offer").click();

        // handler for Optima button
        $("#control_action_save").click(function(){
            $("#sent").val(true);
            saveToDataExtension(buildActivityPayload());
        });

        // handler for Optima button
        $("#control_action_update").click(function(){
            updateExistingRow(buildActivityPayload());
        });

        // handler for Optima button
        $("#control_action_seed").click(function(){
            $("#seed_sent").val(true);
            createAutomationSeed(buildActivityPayload());
        });

        // handler for Optima button
        $("#control_action_create").click(function(){
            $("#automation_sent").val(true);
            createAutomation(buildActivityPayload());
        });

        $("#current_time").html(currentTime);

        // set date inputs to todays date
        $("#message_target_send_datetime").val(todayDate);
        $("#message_seed_send_datetime").val(todayDate);
        $("#offer_start_datetime").val(todayDate);
        $("#offer_end_datetime").val(todayDate);

    }

    function updateApiStatus(endpointSelector, endpointStatus) {

        if ( endpointStatus ) {
            setTimeout(function() {
                $("#" + endpointSelector + " > div > div").removeClass("slds-theme_info");
                $("#" + endpointSelector + " > div > div > span:nth-child(2)").removeClass("slds-icon-utility-info");
                $("#" + endpointSelector + " > div > div").addClass("slds-theme_success");
                $("#" + endpointSelector + " > div > div > span:nth-child(2)").addClass("slds-icon-utility-success");
                $("#" + endpointSelector + " > div > div > span:nth-child(2) svg use").attr("xlink:href","/assets/icons/utility-sprite/svg/symbols.svg#success");
                $("#" + endpointSelector + " > div > div > .slds-notify__content h2").text($("#" + endpointSelector + " > div > div > .slds-notify__content h2").text().replace("Loading", "Loaded"));
            }, apiWaitTime);
        
        } else {
            setTimeout(function() {
                $("#" + endpointSelector + " > div > div").removeClass("slds-theme_info");
                $("#" + endpointSelector + " > div > div > span:nth-child(2)").removeClass("slds-icon-utility-info");
                $("#" + endpointSelector + " > div > div").addClass("slds-theme_error");
                $("#" + endpointSelector + " > div > div > span:nth-child(2)").addClass("slds-icon-utility-error");
                $("#" + endpointSelector + " > div > div > span:nth-child(2) svg use").attr("xlink:href","/assets/icons/utility-sprite/svg/symbols.svg#error");
                $("#" + endpointSelector + " > div > div > .slds-notify__content h2").text($("#" + endpointSelector + " > div > div > .slds-notify__content h2").text().replace("Loading", "Error Loading"));
            }, apiWaitTime);
        }

        apiWaitTime = apiWaitTime + 200;

    }

    function showOrHideOfferFormsBasedOnType() {

        if ( $("#offer_channel").val() == '3' || $("#offer_channel").val() == 3) {
            // informational, show cell code and de-couple from promotion widget
            $("#offer_cell_box").show();
            // hide promotion dropdown
            $("#promotion_element").hide();
            $("#show_validity_form").hide();

            if ($("#offer_validity").val() == "true") {
                $("#offer_validity").val("false").change();
            }
        
            $("#click_through_url_form").show();
            $("#info_button_text_form").show();

        } else {

            $("#offer_cell_box").hide();
            // show offer promotion
            $("#promotion_element").show();
            
            if ($("#offer_validity").val() == "false" && $("#show_validity_form").is(":hidden")) {
                $("#offer_validity").val("true").change();
            }
            $("#show_validity_form").show();

            $("#click_through_url_form").hide();
            $("#info_button_text_form").hide();
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

    function validateStep(stepToValidate) {

        if (debug) {
            console.log("Step that will be validated");
            console.log(stepToValidate);
        }

        if ( $("#step" + stepToValidate).find('.slds-has-error').length > 0 ) {

            return false;

        } else if ( stepToValidate == 0 ) {

            var step0Selectors = ["#update_contacts", "#widget_name"];
            var step0ErrorCount = 0;

            for ( var n = 0; n < step0Selectors.length; n++ ) {

                console.log("The selector is " + step0Selectors[n]);

                if ( !$(step0Selectors[n]).val() ) {

                    step0ErrorCount++;
                }
            }
            if ( $("#update_contacts").val() == "none" && !$("#push_type_message_non_loyalty").is(":checked")) {
                step0ErrorCount++;
            }

            if ( step0ErrorCount == 0 ) {

                return true;

            } else {

                return false;

            }

        } else if ( stepToValidate == 1 ) {


            var step1Selectors = ["#message_target_send_datetime", "#message_seed_send_datetime", "#message_title", "#cell_code", "#cell_name", "#campaign_name", "#campaign_code"];
            var step1ErrorCount = 0;

            for ( var l = 0; l < step1Selectors.length; l++ ) {

                console.log("The selector is " + step1Selectors[l]);

                if ( !$(step1Selectors[l]).val() ) {

                    step1ErrorCount++;
                }
            }

            if ( step1ErrorCount == 0 ) {

                return true;

            } else {

                return false;

            }

        } else if ( stepToValidate == 2 ) {

            var step2Selectors = ["#offer_short_content", "#offer_start_datetime", "#offer_end_datetime", "#offer_type", "#offer_image_url"];
            var step2ErrorCount = 0;

            for ( var m = 0; m < step2Selectors.length; m++ ) {

                console.log("The selector is " + step2Selectors[m]);

                if ( !$(step2Selectors[m]).val() ) {

                    step2ErrorCount++;
                }
            }



            var selectedChannel = $("#offer_channel").val();

            console.log("Channel value is");
            console.log(selectedChannel);

            if ( selectedChannel == 3 || selectedChannel == '3') {

                let step2CommSelectors = ["#offer_cell_code", "#offer_cell_name", "#offer_campaign_name", "#offer_campaign_code"]
                for ( let b = 0; b < step2CommSelectors.length; b++ ) {
                    console.log("The selector is " + step2CommSelectors[m]);

                    if ( !$(step2CommSelectors[b]).val() ) {
                        step2ErrorCount++;
                    }
                }

            } else {

                // check promotion isn't no-code
                if ( $("#offer_promotion").val() == 'no-code') {

                    step2ErrorCount++;

                }

            }

            console.log("The offer start date string is:");
            console.log($("#offer_start_datetime").val());
            console.log("The offer end date string is:");
            console.log($("#offer_end_datetime").val());

            if ( step2ErrorCount == 0 ) {

                return true;

            } else {

                return false;

            }            

        } else {

            return true;

        }
        
    }

    async function lookupControlGroups() {
        try {
            // access offer types and build select input
            let result = await $.ajax({
                url: "/dataextension/lookup/controlgroups"
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
                url: "/dataextension/lookup/promotions"
            });

            if ( debug ) {
                console.log('lookup promotions executed');
                console.log(result.items);               
            }

            var i;
            if ( result.items ) {
                for (i = 0; i < result.items.length; ++i) {
                    if ( debug ) {
                        console.log(result.items[i].keys);
                    }

                    let deRow = result.items[i].values;
                    if (deRow.promotiontype != "nocode"){
                        $("#offer_promotion").append(`<option data-attribute-redemptions=${deRow.instore_code_1_redemptions} data-attribute-control=${deRow.communication_cell_id_control} data-attribute-cell=${deRow.communication_cell_id} data-attribute-cell-name=${deRow.cell_name} data-attribute-mc6=${deRow.mc_id_6} data-attribute-mc1=${deRow.mc_id_1} data-attribute-instore-code=${deRow.instore_code_1} data-attribute-online-code=${deRow.global_code_1} data-attribute-online-promotion-type=${deRow.onlinepromotiontype} data-attribute-promotion-type=${deRow.promotiontype} data-attribute-voucher-pot=${deRow.unique_code_1} value=${result.items[i].keys.promotion_key}>${deRow.campaign_name}</option>`);
                    }
                }
            }

            updateApiStatus("promotions-api", true);
        } catch {
            updateApiStatus("promotions-api", false);
        }
        
    }

    async function lookupUpdateContacts() {
        try {
            // access offer types and build select input
            let result = await $.ajax({
                url: "/dataextension/lookup/updatecontacts"
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
                $("#update_contacts").append("<option value=" + encodeURI(de.dataextensionname) + ">" + de.dataextensionname + "</option>");
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

    function onGetTokens (tokens) {
        // Response: tokens == { token: <legacy token>, fuel2token: <fuel api token> }
        // console.log(tokens);
    }

    function onGetEndpoints (endpoints) {
        // Response: endpoints == { restHost: <url> } i.e. "rest.s1.qa1.exacttarget.com"
        // console.log(endpoints);
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
                type: 'POST',
                data: JSON.stringify(payloadToSave),
                contentType: 'application/json',                     
                success: function(data) {
                    console.log('success');
                    console.log(data);
                    $("#message_key_hidden").val(data);
                    $("#main_setup_key").html(data);
                    $("#control_action_save").html("Data has been sent");
                    $("#control_action_save").prop('disabled', true);
                    $("#control_action_update").prop('disabled', false);
                    $("#control_action_seed").prop('disabled', false);
                    $("#control_action_create").prop('disabled', false);
                }
                , error: function(jqXHR, textStatus, err){
                    if ( debug ) {
                        console.log(err);
                    }
                }
            }); 
        } catch(e) {
            console.log("Error saving data");
            console.log(e);
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
                type: 'POST',
                data: JSON.stringify(payloadToSave),
                contentType: 'application/json',                     
                success: function(data) {
                    console.log('success');
                    console.log(data);
                    $("#control_action_save").html("Data has been updated");
                    $("#control_action_update").prop('disabled', false);
                    $("#control_action_seed").prop('disabled', false);
                    $("#control_action_create").prop('disabled', false);
                }
                , error: function(jqXHR, textStatus, err){
                    if ( debug ) {
                        console.log(err);
                    }
                }
            }); 
        } catch(e) {
            console.log("Error saving data");
            console.log(e);
        }

    }

    function createAutomationSeed(payloadToSave) {

        if ( debug ) {
            console.log("Data Object to be saved is: ");
            console.log(payloadToSave);
        }

        try {
            $.ajax({ 
                url: '/automation/create/query/seed',
                type: 'POST',
                data: JSON.stringify(payloadToSave),
                contentType: 'application/json',                     
                success: function(data) {
                    console.log('success');
                    console.log(data);
                    $("#control_action_seed").html("Automation Created");
                    //$("#control_action_seed").prop('disabled', true);
                }
                , error: function(jqXHR, textStatus, err){
                    if ( debug ) {
                        console.log(err);
                    }
                }
            }); 
        } catch(e) {
            console.log("Error saving data");
            console.log(e);
        }

    }


    function createAutomation(payloadToSave) {

        if ( debug ) {
            console.log("Data Object to be saved is: ");
            console.log(payloadToSave);
        }

        try {
            $.ajax({ 
                url: '/automation/create/query',
                type: 'POST',
                data: JSON.stringify(payloadToSave),
                contentType: 'application/json',                     
                success: function(data) {
                    console.log('success');
                    console.log(data);
                    $("#query_key_hidden").val(data);
                    $("#main_setup_query_id").html(data);
                    $("#control_action_create").html("Scheduled for broadcast");
                    $("#control_action_create").prop('disabled', true);
                }
                , error: function(jqXHR, textStatus, err){
                    if ( debug ) {
                        console.log(err);
                    }
                }
            }); 
        } catch(e) {
            console.log("Error saving data");
            console.log(e);
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

        var buildPayload = buildActivityPayload();

        // replace with res from save to DE function

        if (debug) {
            console.log("Build Payload is:");
            console.log(JSON.stringify(buildPayload));
        }

        var argPromotionKey;

        for ( var w = 0; w < buildPayload.length; w++ ) {
            console.log("inside build payload loop");
            console.log(buildPayload[w]);
            if ( buildPayload[w].key == "message_key_hidden") {
                argPromotionKey = buildPayload[w].value;
            }
        }

        console.log("arg key");
        console.log(argPromotionKey); 

        // 'payload' is initialized on 'initActivity' above.
        // Journey Builder sends an initial payload with defaults
        // set by this activity's config.json file.  Any property
        // may be overridden as desired.
        payload.name = $("#widget_name").val();

        payload['arguments'].execute.inArguments = [{buildPayload}];

        // set isConfigured to true
        if ( argPromotionKey ) {
            // this is only true is the app returned a key
            // sent to de and configured
            payload['metaData'].isConfigured = true;
        } else {
            // not sent to de but configured
            payload['metaData'].isConfigured = false;
        }

        if ( debug ) {
            console.log("Payload including in args")
            console.log(payload.arguments.execute.inArguments);
        }

        // trigger payload save
        connection.trigger('updateActivity', payload);
    }

});