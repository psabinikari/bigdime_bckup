;try {
/* module-key = 'com.gliffy.integration.confluence:gliffy-confluence-basics', location = 'gliffy/JqueryExtensions.js' */
(function(){var a=require("jquery");a.isString=a.isString||function(b){return a.type(b)==="string"};a.isNumber=a.isNumber||function(b){return a.type(b)==="number"};a.format=a.format||function(c){var b=Array.prototype.slice.call(arguments,1);return c.replace(/{(\d+)}/g,function(d,e){return(typeof b[e]!="undefined")?b[e]:"undefined"})};a.htmlDecode=a.htmlDecode||function(b){var c=document.createElement("div");c.innerHTML=b;return c.childNodes.length===0?"":c.childNodes[0].nodeValue}})();
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'com.gliffy.integration.confluence:gliffy-confluence-basics', location = 'gliffy/Url.js' */
define("gliffy/url",{buildPath:function(a){return a.reduce(function(b,c){if(c==null){return b}else{if(b){return b+"/"+encodeURIComponent(c)}}return encodeURIComponent(c)},"")},buildQuery:function(a){return _.reduce(a,function(b,d,c){if(d==null){return b}else{if(b){return b+"&"+encodeURIComponent(c)+"="+encodeURIComponent(d)}}return encodeURIComponent(c)+"="+encodeURIComponent(d)},"")},buildPathAndQuery:function(a,b){return[this.buildPath(a),this.buildQuery(b)].join("?")},parse:function(b){var f=/^(?:([a-z]*):)?(?:\/\/)?(?:([^:@]*)(?::([^@]*))?@)?([0-9a-z-._]+)?(?::([0-9]*))?(\/[^?#]*)?(?:\?([^#]*))?(?:#(.*))?$/i;var g=b.match(f)||[];var e={url:b,scheme:g[1],user:g[2],pass:g[3],host:g[4],port:g[5]&&+g[5],path:g[6],query:g[7],hash:g[8]};e.queryParams={};var c=e.query.split("&");for(var a=0;a<c.length;a++){var d=c[a].split("=");e.queryParams[decodeURIComponent(d[0])]=decodeURIComponent(d[1])}return e}});
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'com.gliffy.integration.confluence:gliffy-confluence-basics', location = 'gliffy/controls/alert/Alert.soy' */
// This file was automatically generated from Alert.soy.
// Please don't edit this file by hand.

if (typeof gliffySoy == 'undefined') { var gliffySoy = {}; }
if (typeof gliffySoy.controls == 'undefined') { gliffySoy.controls = {}; }
if (typeof gliffySoy.controls.alert == 'undefined') { gliffySoy.controls.alert = {}; }


gliffySoy.controls.alert.CompleteAlert = function(opt_data, opt_sb) {
  var output = opt_sb || new soy.StringBuilder();
  output.append('<div id="gliffy-alert-save-as-complete" class="alert alert-success gliffy-alert"><a href="#" class="close" data-dismiss="alert">&times;</a><strong>', soy.$$escapeHtml(opt_data.diagramName), '</strong>&nbsp;', soy.$$escapeHtml("successfully saved to page"), '&nbsp;<a class="gliffy-alert-link" href="', soy.$$escapeHtml(opt_data.linkUrl), '" target="_blank"><img class="page-icon"/><strong>&nbsp;', soy.$$escapeHtml(opt_data.pageName), '</strong></a><br/>', soy.$$escapeHtml("in space"), '&nbsp;<strong>', soy.$$escapeHtml(opt_data.spaceName), '</strong></div>');
  return opt_sb ? '' : output.toString();
};


gliffySoy.controls.alert.ErrorAlert = function(opt_data, opt_sb) {
  var output = opt_sb || new soy.StringBuilder();
  output.append('<div id="gliffy-alert-save-as-error" class="alert alert-error gliffy-alert"><a href="#" class="close" data-dismiss="alert">&times;</a>', soy.$$escapeHtml(opt_data.errorDetail), '</div>');
  return opt_sb ? '' : output.toString();
};

} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'com.gliffy.integration.confluence:gliffy-confluence-basics', location = 'gliffy/ObjectValidatorFactory.js' */
define("gliffy/objectValidatorFactory",["jquery"],function(b){var a=function(d){var c=function(e,g,f){throw new Error(b.format("{0} {1} value ({2}) failed predicate: {3}",d.description,e,b.isFunction(g)?"[function]":g,f))};if(!b.isString(d.description)){d.description="ObjectValidator construction parameters";c("description",d.description,"Expected value to be a string.")}if(typeof d.keyToPredicateMap==="undefined"){d.description="ObjectValidator construction parameters";c("keyToPredicateMap",d.keyToPredicateMap,"Expected value to be defined.")}b.each(d.keyToPredicateMap,function(f,e){if(!b.isFunction(e.func)){c("keyToPredicateMap "+f+".func",e.func,"Expected value to be a function.")}if(!b.isString(e.failureMessage)){c("keyToPredicateMap "+f+".failureMessage",e.failureMessage,"Expected value to be a string.")}});this._={keyToPredicateMap:d.keyToPredicateMap,fail:c}};a.prototype.validate=function(d){var c=this;if(d==null){throw new Error("ObjectValidator.validate() requires non-null input.")}b.each(c._.keyToPredicateMap,function(f,e){if(!e.func(d[f])){c._.fail(f,d[f],e.description)}})};return{create:function(c){return new a(c)},predicates:{isString:{func:b.isString,failureMessage:"Expected value to be a string."},isNumber:{func:b.isNumber,failureMessage:"Expected value to be a number."},isFunction:{func:b.isFunction,failureMessage:"Expected value to be a function."},isArray:{func:b.isArray,failureMessage:"Expected value to be an array."},isTruthy:{func:function(c){return c==true},failureMessage:"Expected value to be truthy."},isFalsey:{func:function(c){return c==false},failureMessage:"Expected value to be falsey."},isDefinedAndNotNull:{func:function(c){return c!=null},failureMessage:"Expected value to be defined and not null."},containsAnyKey:{func:function(c){return Object.keys(c).length>0},failureMessage:"Expected value to contain at least one key."}}}});
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
