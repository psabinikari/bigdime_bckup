;try {
/* module-key = 'confluence.aui.staging:storage-manager', location = 'js/storage-manager.js' */
AJS.storageManager=function(h,i){var g=AJS.Meta.get("remote-user"),d=h+"."+(g?g+".":"")+i,e=!1,j=/\d+#/,f=function(a){if(!e)return null;var c,b=localStorage.getItem(d+"."+a);if(c=j.exec(b))if(b=b.replace(c[0],""),+new Date>c[0].replace("#",""))return localStorage.removeItem(d+"."+a),null;return b};try{e="localStorage"in window&&null!==window.localStorage}catch(k){AJS.log("Browser does not support localStorage, Confluence.storageManager will not work.")}return{getItem:f,doesContain:function(a){return!!f(a)},
getItemAsBoolean:function(a){a=f(a);return"false"==a?!1:"true"==a?!0:!!a},setItem:function(a,c,b){e&&(b=(b=1E3*(b||0))?+new Date+b+"#":"",localStorage.setItem(d+"."+a,b+c))},setItemQuietly:function(a,c,b){try{this.setItem(a,c,b)}catch(d){AJS.debug("Unable to write to localStorage. key: ",a,", value: ",c,", cause: ",d)}},removeItem:function(a){e&&localStorage.removeItem(d+"."+a)}}};
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
