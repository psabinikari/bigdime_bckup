;try {
/* module-key = 'com.atlassian.plugins.atlassian-plugins-webresource-rest:web-resource-manager', location = 'js/wrm/jquery.js' */
WRM.define("wrm/jquery",function(){return AJS.$});
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'com.atlassian.plugins.atlassian-plugins-webresource-rest:web-resource-manager', location = 'js/wrm/underscore.js' */
WRM.define("wrm/_",function(){var a={filter:function(f,d){var b=[];for(var c=0;c<f.length;++c){var e=f[c];if(d(e)){b.push(e)}}return b},map:function(d,c){var e=[];for(var b=0;b<d.length;++b){e.push(c(d[b]))}return e},each:function(d,c){if(d.length===+d.length){for(var b=0;b<d.length;++b){c(d[b])}}else{for(var e in d){if(d.hasOwnProperty(e)){c(d[e],e)}}}},bind:function(c,b){return function(){c.apply(b,Array.prototype.slice.call(arguments))}},isArray:function(b){return Object.prototype.toString.call(b)=="[object Array]"},isUndefined:function(b){return b===void 0},isNull:function(b){return b===null}};return a});
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'com.atlassian.plugins.atlassian-plugins-webresource-rest:web-resource-manager', location = 'js/wrm/builder.js' */
WRM.define("wrm/builder",function(){var a=function(d){var b="(?:$|\\?.*)";var c="^"+d;this.patterns={js:{superBatch:new RegExp(c+"/superbatch/js/batch.js"+b),contextBatch:new RegExp(c+"/contextbatch/js/(.*?)/batch.js"+b),resourceBatch:new RegExp(c+"/batch/(.*?)/(.*).js"+b),noBatch:new RegExp(c+"/resources/(.*?)/(.*).js"+b)},css:{superBatch:new RegExp(c+"/superbatch/css/batch.css"+b),contextBatch:new RegExp(c+"/contextbatch/css/(.*?)/batch.css"+b),resourceBatch:new RegExp(c+"/batch/(.*?)/(.*).css"+b),noBatch:new RegExp(c+"/resources/(.*?)/(.*).css"+b)}}};a.prototype={initialize:function(c){var g,b=c.getElementsByTagName("script"),e=c.getElementsByTagName("link"),f={superBatch:false,modules:[],contexts:[]};for(g=0;g<b.length;++g){var d=b[g];if(d.getAttribute("src")){this._inspectMatches(d.getAttribute("src"),this.patterns.js,f)}}for(g=0;g<e.length;++g){var h=e[g];if(h.getAttribute("href")){this._inspectMatches(h.getAttribute("href"),this.patterns.css,f)}}return f},getModulesAndContextsFromScriptUrl:function(c){var b={superBatch:false,modules:[],contexts:[]};this._inspectMatches(c,this.patterns.js,b);return b},_inspectMatches:function(d,f,c){var b;b=d.match(f.superBatch);if(b){c.superBatch=true;return}b=d.match(f.contextBatch);if(b){var g=b[1].split(",");for(var e=0;e<g.length;++e){this._addToLoadedList(c.contexts,g[e])}return}b=d.match(f.resourceBatch);if(b){this._addToLoadedList(c.modules,b[1]);return}b=d.match(f.noBatch);if(b){this._addToLoadedList(c.modules,b[1]);return}},_addToLoadedList:function(d,c){for(var b=0;b<d.length;++b){if(d[b]===c){return}}d.push(c)}};return a});
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'com.atlassian.plugins.atlassian-plugins-webresource-rest:web-resource-manager', location = 'js/wrm/ie-only-filter.js' */
WRM.define("wrm/ie-only-filter",["wrm/jquery","wrm/_"],function(c,a){function b(d){var e=c.browser.msie;return a.filter(d,function(f){return f.ieOnly?e:true})}return b});
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'com.atlassian.plugins.atlassian-plugins-webresource-rest:web-resource-manager', location = 'js/wrm/conditional-comment-filter.js' */
WRM.define("wrm/conditional-comment-filter",["wrm/jquery","wrm/_"],function(c,a){function b(e){var f=c.browser.msie;var d=c.browser.version;return a.filter(e,function(h){var g=h.conditionalComment;if(!g){return true}if(!f||d>9){return false}g=g.toLowerCase();switch(g){case"ie":return true;case"lt ie 9":return d<9;case"lte ie 9":return d<=9;case"ie 9":return d==9;case"gt ie 9":return d>9;case"gte ie 9":return d>=9;case"lt ie 8":return d<8;case"lte ie 8":return d<=8;case"ie 8":return d==8;case"gt ie 8":return d>8;case"gte ie 8":return d>=8;default:AJS.log("Cannot parse conditional comment "+g);return false}})}return b});
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'com.atlassian.plugins.atlassian-plugins-webresource-rest:web-resource-manager', location = 'js/wrm/resource-base-url-pattern.js' */
WRM.define("wrm/resource-base-url-pattern",function(){return WRM.data.claim("com.atlassian.plugins.atlassian-plugins-webresource-rest:web-resource-manager.resource-base-url-pattern")});
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'com.atlassian.plugins.atlassian-plugins-webresource-rest:web-resource-manager', location = 'js/wrm/require-handler.js' */
WRM.define("wrm/require-handler",["wrm/jquery","wrm/_","wrm/builder","wrm/conditional-comment-filter","wrm/ie-only-filter","wrm/resource-base-url-pattern"],function(e,h,i,g,a,c){var f=/^wr!(.*)$/,d=/^wrc!(.*)$/;var b=function(){this._requireCache={}};b.prototype={require:function(k,j){if(!h.isArray(k)){k=[k]}if(!this._requireCache.hasOwnProperty(k)){this._requireCache[k]=this._resolveAsync(k)}var l=this._requireCache[k];if(j){l.done(function(){j.apply(this,arguments)})}return l},_resolveAsync:function(l){var j=e.Deferred();var k=this;this._getScriptsForResources(l).done(function(p){if(p.unparsedData){window.WRM._unparsedData||(window.WRM._unparsedData={});h.each(p.unparsedData,function(u,t){window.WRM._unparsedData[t]=u})}var q=[];var s=[];var m=k._filter(p.resources);for(var o=0;o<m.length;++o){var r=m[o];var n=r.url;if(r.resourceType==="JAVASCRIPT"){if(!k._isJSInInitLoad(n)){q.push("js!"+n+"!order")}}else{if(r.resourceType==="CSS"){if(!k._isCSSInInitLoad(n)){if(r.media&&"all"!==r.media){s.push(r)}else{q.push("css!"+n)}}}else{AJS.log("Unknown resource type required: "+n)}}}AJS.log("Downloading resources:\n"+q.join("\n"));WRM.curl(q,function(){h.each(s,function(u){k._loadCssImmediate(u)});var t=h.map(l,function(){return window});j.resolveWith(k,t)},function(){j.rejectWith(k,arguments)})});return j.promise()},_loadCssImmediate:function(k){AJS.log("WARN: asynchronously loading a CSS resource containing a media query: "+k.url);var j='<link rel="stylesheet" type="text/css" href="'+k.url+'" media="'+k.media+'" />';e(j).appendTo("head")},_getScriptsForResources:function(k){if(!this._builder){this._builder=new i(c);this._initLoadResources=this._builder.initialize(document)}var j=[];var l=[];h.each(k,function(n){var m;if(m=n.match(f)){j.push(m[1])}else{if(m=n.match(d)){l.push(m[1])}}});return e.ajax({url:AJS.contextPath()+"/rest/webResources/1.0/resources",type:"POST",contentType:"application/json",dataType:"json",data:JSON.stringify({r:j,c:l,xc:this._initLoadResources.contexts,xr:this._initLoadResources.modules})})},_isJSInInitLoad:function(j){return e("script[src='"+j+"']").length>0},_isCSSInInitLoad:function(j){return e("link[href='"+j+"']").length>0},_filter:function(k){if(!this._filters){this._filters=[g,a]}var j=k;h.each(this._filters,function(l){j=l(j)});return j}};return b});
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'com.atlassian.plugins.atlassian-plugins-webresource-rest:web-resource-manager', location = 'js/wrm/index.js' */
WRM.curl(["wrm/require-handler"],function(b){var a;WRM.require=function(d,c){if(!a){a=new b()}return a.require(d,c)}});
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
