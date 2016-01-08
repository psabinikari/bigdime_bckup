;try {
/* module-key = 'com.atlassian.auiplugin:jquery-compatibility', location = 'js-vendor/jquery/jquery-compatibility.js' */
jQuery.noConflict();
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
