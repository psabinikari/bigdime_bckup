;try {
/* module-key = 'com.atlassian.auiplugin:aui-experimental-tooltips', location = 'js/aui-experimental-tooltip.js' */
(function(A){A.fn.tooltip=function(B){if(typeof B=="string"){this.tipsy(B);return C}var E=A.extend({},A.fn.tooltip.defaults,B),C=this.tipsy(E);if(E.hideOnClick&&(E.trigger=="hover"||!E.trigger&&this.tipsy.defaults.trigger=="hover")){var D=function(){A(this).tipsy("hide")};if(E.live){A(this.context).on("click.tipsy",this.selector,D)}else{this.bind("click.tipsy",D)}}return C};A.fn.tooltip.defaults={opacity:1,offset:1,delayIn:500,hoverable:true,hideOnClick:true}}(AJS.$));
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
