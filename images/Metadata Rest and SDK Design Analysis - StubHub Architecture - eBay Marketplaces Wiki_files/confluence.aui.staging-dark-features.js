;try {
/* module-key = 'confluence.aui.staging:dark-features', location = 'js/dark-features.js' */
(function(e){var c=AJS.Meta.get("enabled-dark-features"),d=c?c.split(","):[],b={};e.each(d,function(){b[this]=!0});AJS.DarkFeatures={isEnabled:function(a){return!!b[a]},enable:function(a){a&&!b[a]&&(b[a]=!0)},disable:function(a){a&&b[a]&&delete b[a]},getAllEnabled:function(){return d}}})(AJS.$);
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
