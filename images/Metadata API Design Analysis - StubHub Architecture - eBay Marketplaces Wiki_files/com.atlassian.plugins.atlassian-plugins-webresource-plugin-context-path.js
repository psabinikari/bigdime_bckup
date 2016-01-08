;try {
/* module-key = 'com.atlassian.plugins.atlassian-plugins-webresource-plugin:context-path', location = 'js/data/context-path.js' */
(function(){var a=null;(window.AJS||(window.AJS={}));AJS.contextPath=function(){if(a===null){a=WRM.data.claim("com.atlassian.plugins.atlassian-plugins-webresource-plugin:context-path.context-path")}return a};AJS.contextPath._reset=function(){a=null}}());
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
