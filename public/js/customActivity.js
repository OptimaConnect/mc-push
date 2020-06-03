define([
    'postmonger',
    'jquery',
    'emojione', 
    'emojionearea', 
    'textcomplete'
], function(
    Postmonger
) {
    'use strict';

    var debug                       = true;
    var apiWaitTime                 = 500;
    var stepToValidate;
    var connection                  = new Postmonger.Session();
    var payload                     = {};
    var payloadNode                 = {};
    var onlineSetupStepEnabled      = false;
    var instoreSetupStepEnabled     = false;
    var steps                       = [
        { "label": "Message Type", "key": "step0" },
        { "label": "PUSH Message Setup", "key": "step1", "active": false },
        { "label": "PUSH Offer Setup", "key": "step2", "active": false },
        { "label": "Summary", "key": "step3" }
    ];
    var currentStep = steps[0].key;
    var stepValidation = false;
    var payloadToSave;
    var summaryPayload;
    var today = new Date();
    var currentTime = today.toGMTString();
    var todayDate = new Date().toISOString().slice(0,10);

    if ( debug ) {
        console.log("Current Step is: " + currentStep);
    }

    $(window).ready(onRender);

    connection.on('initActivity', initialize);
    connection.on('requestedTokens', onGetTokens);
    connection.on('requestedEndpoints', onGetEndpoints);

    connection.on('clickedNext', onClickedNext);
    connection.on('clickedBack', onClickedBack);
    connection.on('gotoStep', onGotoStep);

    function onRender() {
        var debug = true;
        // JB will respond the first time 'ready' is called with 'initActivity'
        connection.trigger('ready');

        connection.trigger('requestTokens');
        connection.trigger('requestEndpoints');

        lookupPromos();
        lookupControlGroups();
        lookupUpdateContacts();
        loadEvents();
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
            triggerSteps(argumentsSummaryPayload.buildPayload, argPromotionType);


        }      
    }

    function countChar1(val) {
        var len = val.getText().length;
        if (len >= 300) {
              val.value = val.content.substring(0, 300);
              $('#chars1').text(0);
        } else {
             $('#chars1').text(300 - len);
        }
    }

    function countChar2(val) {
        var len = val.getText().length;
        if (len >= 100) {
              val.value = val.content.substring(0, 10);
              $('#chars2').text(0);
        } else {
             $('#chars2').text(100 - len);
        }
    }

    function loadEvents() {

        $("#textarea-id-01").emojioneArea({
            pickerPosition: "bottom",
            filtersPosition: "bottom",
            tones: false,
            autocomplete: false,
            inline: false,
            events: {
                keyup: function (editor, event) {
                   console.log('event:keyup');
                   countChar1(this);
                }
            }
        });

        $("#textarea-id-02").emojioneArea({
            pickerPosition: "bottom",
            filtersPosition: "bottom",
            tones: false,
            autocomplete: false,
            inline: false,
            events: {
                keyup: function (editor, event) {
                   console.log('event:keyup');
                   countChar2(this);
                }
            }
        });

        $("#message_content").emojioneArea();


        // render relevant steps based on input
        $('.promotion_type').click(function() {

            var pushType = $("input[name='push_type']:checked").val();

            if ( debug ) {
                console.log(pushType);
            }

            if ( pushType === 'message' ) {

                // hide control group field
                $("#control_group_box").show();

                $("#promotion_alert").hide();

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
        $('#offer_channel').change(function() {

            if ( $("#offer_channel").val() == '3' || $("#offer_channel").val() == 3) {
                // informational, show cell code and de-couple from promotion widget
                $("#offer_cell_box").show();

                // hide promotion dropdown
                $("#promotion_element").hide();

            } else {

                $("#offer_cell_box").hide();
                // show offer promotion
                $("#promotion_element").show();

            }

        });

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
        $("#radio-1").click();

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
        $("#automation_run_date").val(todayDate);
        $("#message_target_send_date").val(todayDate);
        $("#offer_start_date").val(todayDate);
        $("#offer_end_date").val(todayDate);

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

    function prePopulateFields(argumentsSummaryPayload) {

        if ( debug) {
            console.log("payload sent to prepop function");
            console.log(argumentsSummaryPayload);
        }

        setTimeout(function(){ 

            var q;

            for ( q = 0; q < argumentsSummaryPayload.length; q++ ) {
                if ( debug ) {
                    console.log("Prepop: " + argumentsSummaryPayload[q].key + ", with value: " + argumentsSummaryPayload[q].value + ", and type: " + argumentsSummaryPayload[q].type);
                }
                if ( argumentsSummaryPayload[q].type == "checkbox") {

                    if ( argumentsSummaryPayload[q].value ) {
                        $("#" + argumentsSummaryPayload[q].key).val(true);
                        $("#" + argumentsSummaryPayload[q].key).prop('checked', "checked");
                    }
                    
                } else if ( argumentsSummaryPayload[q].type == "radio") {
                    if ( argumentsSummaryPayload[q].key == "push_type") {
                        if ( argumentsSummaryPayload[q].value == "message") {
                            $("#radio-1").prop('checked', true);
                            $("#radio-1").click();
                        } else if ( argumentsSummaryPayload[q].value == "offer") {
                            $("#radio-2").prop('checked', true);
                            $("#radio-2").click();
                        }
                    }
                }

                $("#step" + (argumentsSummaryPayload[q].step - 1) + " #" + argumentsSummaryPayload[q].key).val(argumentsSummaryPayload[q].value);

            } 
        }, 2000);
    }

    function triggerSteps(argumentsSummaryPayload, argPromotionType) {

        // argument data present, pre pop and redirect to summary page
        var prepopPromotionType = argPromotionType;

        if ( debug ) {
            console.log("prepopPromotionType is");
            console.log(prepopPromotionType);
        }

        var prePop;

        if ( prepopPromotionType == 'message' ) {
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
            $("#radio-1").prop("checked", true).trigger("click");
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

            var step0Selectors = ["#update_contacts", "#automation_run_date", "#widget_name"];
            var step0ErrorCount = 0;

            for ( var n = 0; n < step0Selectors.length; n++ ) {

                console.log("The selector is " + step0Selectors[n]);

                if ( !$(step0Selectors[n]).val() ) {

                    step0ErrorCount++;
                }
            }
            if ( $("#update_contacts").val() == "no-code") {
                step0ErrorCount++;
            }

            console.log("The automation date string is:");
            console.log($("#automation_run_date").val());

            if ( !validateTheDateFormat($("#automation_run_date").val()) ) {
                
                step0ErrorCount++;
            }


            if ( step0ErrorCount == 0 ) {

                return true;

            } else {

                return false;

            }

        } else if ( stepToValidate == 1 ) {


            var step1Selectors = ["#message_target_send_date", "#message_title", "#cell_code", "#cell_name", "#campaign_name", "#campaign_id", "#campaign_code", "#message_url"];
            var step1ErrorCount = 0;

            for ( var l = 0; l < step1Selectors.length; l++ ) {

                console.log("The selector is " + step1Selectors[l]);

                if ( !$(step1Selectors[l]).val() ) {

                    step1ErrorCount++;
                }
            }

            console.log("The message send date string is:");
            console.log($("#message_target_send_date").val());

            if ( !validateTheDateFormat($("#message_target_send_date").val()) ) {
                
                step1ErrorCount++;

            }

            if ( step1ErrorCount == 0 ) {

                return true;

            } else {

                return false;

            }

        } else if ( stepToValidate == 2 ) {

            var step2Selectors = ["#offer_short_content", "#offer_start_date", "#offer_end_date", "#offer_type", "#offer_image_url"];
            var step2ErrorCount = 0;

            var step2CommSelectors = ["#offer_cell_code", "#offer_cell_name", "#offer_campaign_name", "#offer_campaign_id", "#offer_campaign_code"]

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

                for ( var b = 0; b < step2CommSelectors.length; b++ ) {
                    console.log("The selector is " + step2Selectors[m]);

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
            console.log($("#offer_start_date").val());
            console.log("The offer end date string is:");
            console.log($("#offer_end_date").val());

            if ( !validateTheDateFormat($("#offer_start_date").val()) || !validateTheDateFormat($("#offer_end_date").val()) ) {
                
                step2ErrorCount++;
            }

            if ( step2ErrorCount == 0 ) {

                return true;

            } else {

                return false;

            }            

        } else {

            return true;

        }
        
    }

    function isCharInteger(string) {

        console.log("The string passed for INT checking is" + string);
        if (Number.isInteger(parseInt(string))) {
            return true;
        } else {
             return false
        }
    }

    function validateTheDateFormat(dateStringFromForm) {
        console.log("The string passed for Date checking is" + dateStringFromForm);
        var dateStringAsArray = dateStringFromForm.split("");

        console.log("Date array");
        console.log(dateStringAsArray)
        console.log("Date array length");
        console.log(dateStringAsArray.length);

        // is char 4 a - and char 7 a - and is char 9 true or false
        if ( dateStringAsArray.length == 10 && dateStringAsArray[4] == "-" && dateStringAsArray[7] == "-" ) {
            if (isCharInteger(dateStringAsArray[0]) && 
                isCharInteger(dateStringAsArray[1]) && 
                isCharInteger(dateStringAsArray[2]) && 
                isCharInteger(dateStringAsArray[3]) && 
                isCharInteger(dateStringAsArray[5]) && 
                isCharInteger(dateStringAsArray[6]) && 
                isCharInteger(dateStringAsArray[8]) && 
                isCharInteger(dateStringAsArray[9]) 
                ) {
                return true;
            }
        } else {
            return false;
        }
    }


    function isEmpty (value) {
        return ((value == null) || 
                (value.hasOwnProperty('length') && 
                value.length === 0) || 
                (value.constructor === Object && 
                Object.keys(value).length === 0) 
            );
    }

    function isWholeNumber(num) {
        return num === Math.round(num);
    }

    function isTwoValuesUsed(voucherPotValue, globalOnlineCodeValue) {
        return voucherPotValue != '' && globalOnlineCodeValue != '';
    }

    function isValidInstoreCode(selectedCode) {
        return selectedCode !== 'Please select a code' && selectedCode != '';
    }

    function lookupControlGroups() {

        // access offer types and build select input
        $.ajax({
            url: "/dataextension/lookup/controlgroups",
            error: function() {
                updateApiStatus("controlgroup-api", false);
            },  
            success: function(result){

                if ( debug ) {
                    console.log('lookup control groups executed');
                    console.log(result.items);               
                }

                var i;
                for (i = 0; i < result.items.length; ++i) {
                    if ( debug ) {
                        console.log(result.items[i]);
                    }
                    // do something with substr[i]
                    $("#control_group").append("<option value=" + encodeURI(result.items[i].values.dataextensionname) + ">" + result.items[i].values.dataextensionname + "</option>");
                }
                updateApiStatus("controlgroup-api", true);
            }
        });
    }

    function lookupPromos() {

        // access offer types and build select input
        $.ajax({

            url: "/dataextension/lookup/promotions",
            error: function() {
                updateApiStatus("promotions-api", false);
            }, 
            success: function(result){

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
                        // do something with `substr[i]
                        $("#offer_promotion").append("<option data-attribute-redemptions=" + result.items[i].values.instore_code_1_redemptions + " data-attribute-control=" + result.items[i].values.communication_cell_id_control + " data-attribute-cell=" + result.items[i].values.communication_cell_id + " data-attribute-mc6=" + result.items[i].values.mc_id_6 + " data-attribute-mc1=" + result.items[i].values.mc_id_1 + " data-attribute-instore-code=" + result.items[i].values.instore_code_1 + " data-attribute-online-code=" + result.items[i].values.global_code_1 + " data-attribute-online-promotion-type=" + result.items[i].values.onlinepromotiontype + " data-attribute-promotion-type=" + result.items[i].values.promotiontype + " data-attribute-voucher-pot=" + result.items[i].values.unique_code_1 + " value=" + result.items[i].keys.promotion_key + ">" + result.items[i].values.campaign_name + "</option>");
                    }                   
                }

                updateApiStatus("promotions-api", true);
            }

        });
    }

    function lookupUpdateContacts() {

        // access offer types and build select input
        $.ajax({
            url: "/dataextension/lookup/updatecontacts",
            error: function() {
                updateApiStatus("updatecontacts-api", false);
            },  
            success: function(result){

                if ( debug ) {
                    console.log('lookup update contacts executed');
                    console.log(result.items);               
                }

                var i;
                for (i = 0; i < result.items.length; ++i) {
                    if ( debug ) {
                        console.log(result.items[i]);
                    }
                    // do something with substr[i]
                    $("#update_contacts").append("<option value=" + encodeURI(result.items[i].values.dataextensionname) + ">" + result.items[i].values.dataextensionname + "</option>");
                }
                updateApiStatus("updatecontacts-api", true);
            }
        });
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

    function onGetSchema (payload) {
        // Response: payload == { schema: [ ... ] };
        // console.log('requestedSchema payload = ' + JSON.stringify(payload, null, 2));
    }

    function onGetCulture (culture) {
        // Response: culture == 'en-US'; culture == 'de-DE'; culture == 'fr'; etc.
        // console.log('requestedCulture culture = ' + JSON.stringify(culture, null, 2));
    }

    function onClickedNext () {

        var pushType = $("#step0 .slds-radio input[name='push_type']:checked").val();

        if ( debug ) {
            console.log(pushType);
            console.log(currentStep.key);
            console.log("next clicked");           
        }

        if ( pushType == 'message' ) {

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
                    $("#control_action_create").html("Automation Created");
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


    function addPromotionKeyToArgs(saveResponse) {
        if ( debug ){
            console.log("add promokey to args executed");
            console.log(saveResponse);
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
                            value:  step1FormInputs[i].value,
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
                            value:  step2FormInputs[i].value,
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
                            value:  step3FormInputs[i].value,
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
                        if ( summaryPromotionType == "message") {
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

                    if ( summaryPromotionType == "message" ) {

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