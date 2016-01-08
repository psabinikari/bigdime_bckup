;try {
/* module-key = 'confluence.aui.staging:meta', location = 'js/meta.js' */
(function(b){AJS.asBooleanOrString=function(a){var b=a?a.toLowerCase():"";return"true"==b?!0:"false"==b?!1:a};var c={};AJS.Meta=b.extend({},AJS.Meta,{set:function(a,b){c[a]=b},get:function(a){if("undefined"!=typeof c[a])return c[a];a=b("meta[name='ajs-"+a+"']");if(a.length)return a=a.attr("content"),AJS.asBooleanOrString(a)},getBoolean:function(a){return!0===this.get(a)},getNumber:function(a){return+this.get(a)},getAllAsMap:function(){var a={};b("meta[name^=ajs-]").each(function(){a[this.name.substring(4)]=
this.content});return b.extend(a,c)}});AJS.Meta.Links={canonical:function(){return b('head link[rel="canonical"]').attr("href")},shortlink:function(){return b('head link[rel="shortlink"]').attr("href")}}})(AJS.$);
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
