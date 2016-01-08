;try {
/* module-key = 'com.atlassian.plugins.atlassian-plugins-webresource-plugin:data', location = 'js/data/data.js' */
(function(){if(!window.WRM){window.WRM={}}function a(c){if(!c){return c}return JSON.parse(c)}function b(d,c){return Object.prototype.hasOwnProperty.call(d,c)}WRM.data={claim:function(c){if(WRM._unparsedData&&b(WRM._unparsedData,c)){var g=WRM._unparsedData[c];try{var d=a(g);delete WRM._unparsedData[c];return d}catch(f){console&&console.log&&console.log("JSON Error parsing data with key "+c+": "+f)}}}}}());
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
