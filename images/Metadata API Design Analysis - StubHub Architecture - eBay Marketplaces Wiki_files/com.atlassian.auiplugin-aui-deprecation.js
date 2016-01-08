;try {
/* module-key = 'com.atlassian.auiplugin:aui-deprecation', location = 'js/aui/internal/console.js' */
window.AJS=window.AJS||{};(function(){if(typeof window.console==="undefined"){window.console={messages:[],log:function(B){this.messages.push(B)},show:function(){alert(this.messages.join("\n"));this.messages=[]}}}else{window.console.show=function(){}}function A(B){return function(){if(typeof console!=="undefined"&&console[B]){Function.prototype.apply.call(console[B],console,arguments)}}}AJS.log=A("log");AJS.warn=A("warn");AJS.error=A("error")})();
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'com.atlassian.auiplugin:aui-deprecation', location = 'js/aui/internal/deprecation.js' */
window.AJS=window.AJS||{};window.AJS.deprecate={};(function(G,F){var N=Object.prototype.hasOwnProperty;var B=Object.prototype.toString;function J(O){O+="";if(!O){return""}return O.charAt(0).toUpperCase()+O.substring(1)}var H=[];function M(O,P){if(typeof O==="function"){return O}var Q=false;P=P||{};return function(R){var T=F.__getDeprecatedLocation(R?R:1)||"";if(!Q||H.indexOf(T)===-1){Q=true;var S="DEPRECATED - "+J(O)+" has been deprecated"+(P.sinceVersion?" since "+P.sinceVersion:"")+" and will be removed in "+(P.removeInVersion||"a future release")+".";if(P.alternativeName){S+=" Use "+P.alternativeName+" instead. "}if(P.extraInfo){S+=" "+P.extraInfo}if(T===""){F.__logger(S+" \n No stack trace of the deprecated usage is available in your current browser.")}else{F.__logger(S+" \n "+T)}H.push(T)}}}F.__logger=function(){return AJS.warn.apply(undefined,arguments)};F.__getDeprecatedLocation=function(P){var Q=new Error();var O=Q.stack||Q.stacktrace;var R=(O&&O.replace(/^Error\n/,""))||"";if(R){R=R.split("\n");return R[P+2]}return R};function D(R,P,Q){var O=M(P||R.name||"this function",Q);return function(){O();return R.apply(this,arguments)}}function L(Q,O,P){var R=D(Q,O,P);R.prototype=Q.prototype;G.extend(R,Q);return R}var E=false;try{if(Object.defineProperty){Object.defineProperty({},"blam",{get:function(){},set:function(){}});E=true}}catch(I){}function K(R,T,Q){if(E){var P=R[T];Q=Q||{};var S=Q.displayName||T;var O=M(S,Q);Object.defineProperty(R,T,{get:function(){O();return P},set:function(U){P=U;O();return U}})}else{}}function A(P,R,O){if(typeof P[R]==="function"){O=O||{};var Q=O.displayName||R;P[R]=D(P[R],Q,O)}else{K(P,R,O)}}function C(R,P,Q){Q=Q||{};for(var O in R){if(N.call(R,O)){Q.displayName=P+O;Q.alternativeName=Q.alternativeNamePrefix&&(Q.alternativeNamePrefix+O);A(R,O,G.extend({},Q))}}}F.fn=D;F.construct=L;F.prop=A;F.obj=C;F.propertyDeprecationSupported=E;F.getMessageLogger=M})(jQuery||Zepto,window.AJS.deprecate);
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
