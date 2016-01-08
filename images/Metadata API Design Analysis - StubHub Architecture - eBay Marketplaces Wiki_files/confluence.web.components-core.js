;try {
/* module-key = 'confluence.web.components:core', location = '/includes/js/components/confluence.js' */
(function(c){Confluence={getContextPath:function(){return AJS.Meta.get("context-path")},getBaseUrl:function(){return AJS.$("#confluence-base-url").attr("content")||""},getBuildNumber:function(){return AJS.Meta.get("build-number")},getContentId:function(){var a=AJS.Meta.get("content-id");+a||(a=AJS.Meta.get("page-id"));+a||(a="0");return a},getXsrfToken:function(){return AJS.Meta.get("atl-token")},runBinderComponents:function(){for(var a in Confluence.Binder)if(Confluence.Binder.hasOwnProperty(a))try{Confluence.Binder[a]()}catch(b){AJS.logError("Exception in initialising of component '"+
a+"': "+b.message)}},placeFocus:function(){Confluence.Binder.placeFocus()},unescapeEntities:function(a){var b={amp:"&",lt:"<",gt:">","#39":"'",quot:'"'};return null==a?a:(""+a).replace(/&[#\d\w]+;/g,function(a){var c=a.substring(1,a.length-1);return b[c]||a})},sessionCheck:function(a,b){var d=AJS.Meta.get("remote-user");if(d){var e=this.getContextPath()+AJS.REST.getBaseUrl()+"session/check/"+d,f=this;return c.getJSON(e).done(function(){AJS.debug("SESSION CHECK - OK - "+d)}).fail(function(){AJS.log("SESSION CHECK - FAIL - "+
d);f.sessionFail(a,b)})}},sessionFail:function(a,b){if(!a){var d=c(".aui-dialog:visible");if(d.length)a=d.find(".dialog-panel-body").empty(),d.find(".aui-button").attr("disabled","disabled");else throw Error("sessionFail method really expected a visible dialog about now :/");}b||(b=AJS.format("Your session has expired. Please {0}reload{1} the page to try again.",'<a class="reload" href="">',"</a>"),a.on("click","a.reload",function(){window.location.reload()}));AJS.messages.warning(a,{body:b,closeable:!1,shadowed:!0})},Binder:{placeFocus:function(){var a,
b=-1;AJS.$("input[data-focus]").each(function(){var d=AJS.$(this),c=d.attr("data-focus");c>b&&(b=c,a=d)});a&&a.focus()}}}})(AJS.$);AJS.toInit(function(){Confluence.runBinderComponents()});AJS.Confluence=Confluence;Confluence.hintManager=function(c){if(!AJS.$.isArray(c))throw Error("Hints passed in must be an array of strings");var a=Math.floor(Math.random()*c.length);return{getNextHint:function(){var b=c[a];a=(a+1)%c.length;return b}}};
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'confluence.web.components:core', location = '/includes/js/components/confluence-defaults.js' */
(function(){Confluence.Defaults={maxResults:50}})(AJS.$);
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'confluence.web.components:core', location = '/includes/js/components/confluence-highlighter.js' */
(function(){Confluence.Highlighter=function(b,i){var d,e;if(b&&b.length&&b[0]){for(var g=[],f=0,j=b.length;f<j;f++){var h=b[f];h&&g.push(h.replace(/[\.\*\+\?\|\(\)\[\]{}\\]/g,"\\$"))}d=RegExp("("+g.join("|")+")","gi");e=AJS.template(i||"<strong>{highlight}</strong>").fill({highlight:"$1"}).toString()}return{highlight:function(a,c){if(!a)return a;c||(a=AJS.template.escape(a));return!d?a:a.replace(d,e)},safeHighlight:function(a){if(!a)return a;if(d&&e)var c=e.split("$1"),b=c[0],c=c[1],a=a.replace(d,
"@@@hl@@@$1@@@endhl@@@");return _.escape(a).replace(/@@@hl@@@/gi,b).replace(/@@@endhl@@@/gi,c)}}}})();
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'confluence.web.components:core', location = '/includes/js/components/position.js' */
AJS.Position=function(d){return{spaceAboveBelow:function(c,f){var b=f.position().top,a,g=f.outerHeight(!0),e;"IFRAME"==c.nodeName?(a=c.contentWindow||c.contentDocument,e=d(c).height(),a=d(a.document||a),b-=a.scrollTop()):(a=d(c),e=a.height(),b-=a.position().top);return{above:b,below:e-b-g}}}}(AJS.$);
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'confluence.web.components:core', location = '/includes/js/components/cache-manager.js' */
(function(){AJS.Confluence.cacheManager=function(a){if(!(this instanceof AJS.Confluence.cacheManager))return new AJS.Confluence.cacheManager(a);this.cache={};this.cacheStack=[];this.cacheSize=a||30};AJS.Confluence.cacheManager.prototype.get=function(a){var d=this.cache[a];if(d){var b=this.cacheStack[this.cacheStack.length-1][0]+1;d.weight=b;this.cacheStack.push([b,a]);return d.value}};AJS.Confluence.cacheManager.prototype.put=function(a,d){var b=this.cacheStack.length?this.cacheStack[this.cacheStack.length-
1][0]+1:0;this.cache[a]={weight:b,value:d};this.cacheStack.push([b,a]);for(var c;this.cacheStack.length>this.cacheSize;)c=this.cacheStack.shift(),b=c[0],c=c[1],b===this.cache[c].weight&&delete this.cache[c]};AJS.Confluence.cacheManager.prototype.clear=function(){this.cache={};this.cacheStack=[]}})();
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'confluence.web.components:core', location = '/includes/js/components/localstorage-cache-manager.js' */
(function(g){AJS.Confluence.localStorageCacheManager=function(b,h){this.cache={};this.cacheStack=[];this.cacheSize=h||30;if(window.localStorage&&!("string"!==typeof b&&0<b.length)){var a="Confluence."+b,c=a+".cache",d=a+".cacheStack",a=localStorage.getItem(c),e=localStorage.getItem(d),f=this;if(a&&e)try{this.cache=JSON.parse(a),this.cacheStack=JSON.parse(e)}catch(i){this.cache={},this.cacheStack={}}g(window).unload(function(){localStorage[c]=JSON.stringify(f.cache);localStorage[d]=JSON.stringify(f.cacheStack)})}};
AJS.Confluence.localStorageCacheManager.prototype=new AJS.Confluence.cacheManager})(AJS.$);
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'confluence.web.components:core', location = '/includes/js/components/confluence-storage-manager.js' */
Confluence.storageManager=function(a){return AJS.storageManager("confluence",a)};
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'confluence.web.components:core', location = '/includes/js/components/input-driven-dropdown.js' */
(function(f){function e(c,d){this._effect="appear";this._timer=null;this.id=c;this.options=d;this.busy=this.inactive=!1;this.cacheManager=AJS.Confluence.cacheManager()}var g=function(c,d){var b=c.options,a=c.dd;a&&(a.hide(),a.$.remove());b.ajsDropDownOptions=b.ajsDropDownOptions||{};b.ajsDropDownOptions&&!b.ajsDropDownOptions.alignment&&(b.ajsDropDownOptions.alignment="left");b.ajsDropDownOptions.selectionHandler=b.ajsDropDownOptions.selectionHandler||function(a,c){if(a.type!="click"){var b=f("a",
c).first(),d=b.attr("href");if(a.type==="keydown"&&(a.metaKey||a.ctrlKey)&&a.which===13)window.open(d,"_blank");else{b.click();document.location=d}a.preventDefault()}};a=c.dd=AJS.dropDown(d.matrix,b.ajsDropDownOptions)[0];b.ajsDropDownOptions&&b.ajsDropDownOptions.className&&a.$.addClass(b.ajsDropDownOptions.className);b.dropdownPlacement?b.dropdownPlacement(a.$):(AJS.log("No dropdownPlacement function specified. Appending dropdown to the body."),f("body").append(a.$));var e=new Confluence.Highlighter(d.queryTokens||
[d.query]);f("li a:not(.dropdown-prevent-highlight) span",a.$).each(function(){var a=f(this);a.html(e.safeHighlight(a.text()))});var h=a.$.closest(".aui-dd-parent").width();h||(a.$.width("25em"),h=a.$.width());f("a span:not(.icon)",a.$).each(function(){var a=f(this),b=AJS("var","&#8230;"),c=b.width(),d=false;a.wrapInner(f("<em>"));f("em",a).each(function(){var a=f(this);a.show();if(this.offsetLeft+this.offsetWidth+c>h-20){for(var b=this.childNodes,e=false,j=b.length-1;j>=0;j--){var k=b[j],l=1,g=k.nodeType==
3?"nodeValue":"innerHTML",i=k[g];do if(l<=i.length)k[g]=i.substr(0,i.length-l++);else break;while(this.offsetLeft+this.offsetWidth+c>h-20);if(l<=i.length){e=true;break}}e?d=true:a.hide()}});if(d){a.append(b);this.elpss=b}});f("a span:not(.icon)",a.$).each(function(){var a=f(this),b;try{b=AJS.dropDown.getAdditionalPropertyValue(a,"spaceName")}catch(c){AJS.logError("Problem getting space name: "+c.message)}var d=a.text();b&&(d=d+(" ("+AJS("i").html(b).text()+")"));a.attr("title",d)});b.dropdownPostprocess&&
b.dropdownPostprocess(a.$);a.show(c._effect);"function"==typeof b.onShow&&b.onShow.call(a,a.$);return a};e.prototype.clearCache=function(){this.cacheManager.clear()};e.prototype.change=function(c,d,b){var a=this;if(c!=a._value){a._value=c;a.busy=!1;clearTimeout(a._timer);var f=RegExp("\\S{"+(a.options.minLengthForGetData||2)+",}");if(d||f.test(c))if(d=a.cacheManager.get(c))g(a,d);else{a.busy=!0;var e=function(){a.options.getDataAndRunCallback.call(a,c,function(){a.show.apply(this,arguments);b&&b(c)})};
0===a.options.dropDownDelay?e():a._timer=setTimeout(function(){e()},a.options.dropDownDelay||200)}else a.dd&&a.dd.hide(),b&&b(c)}};e.prototype.hide=function(){this.dd&&this.dd.hide()};e.prototype.remove=function(){var c=this.dd;c&&(this.hide(),c.$.remove());this.inactive=!0;this.options.onDeath&&this.options.onDeath()};e.prototype.show=function(c,d,b){this.inactive?AJS.log("Quick search abandoned before server response received, ignoring. "+this):(c={matrix:c,query:d,queryTokens:b},this.cacheManager.put(d,
c),g(this,c),this.busy=!1)};AJS.inputDrivenDropdown=function(c){return new e("inputdriven-dropdown",c)}})(jQuery);
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'confluence.web.components:core', location = '/includes/js/components/quicksearchdropdown.js' */
(function(f){f.fn.quicksearch=function(c,l,a){l&&(a.onShow=l);a.makeParams=a.makeParams||function(b){return{query:b}};var m=function(b){if(b.statusMessage)b=[[{html:b.statusMessage,className:"error"}]];else if(a.makeRestMatrixFromData){b=a.makeRestMatrixFromData(b);b=AJS.REST.convertFromRest(b);a.addDropdownData&&(b=a.addDropdownData(b))}else b=b.contentNameMatches;return b},i,j;"function"==typeof c?(j=c(),i=function(b){var a=c();if(a!=j){j=a;b.clearCache()}return a}):i=function(){return c};a.getDataAndRunCallback=
a.getDataAndRunCallback||function(b,k){var g=this,h=i(g,b);if(a.includeUrlPrefix==void 0||a.includeUrlPrefix)h=(AJS.Meta.get("context-path")||"")+h;var c=f.data(g,"lastRequest");c&&c.abort();c=f.ajax({type:"GET",dataType:"json",url:h,xhrFields:{withCredentials:true},data:a.makeParams(b),success:function(a,c){f(window).trigger("quicksearch.ajax-success",{url:h,json:a,resultStatus:c});if(document.activeElement==d[0]){var e=m(a);a.results&&a.results.length>0?k.call(g,e,b,a.queryTokens):k.call(g,e)}},
global:false,timeout:5E3,error:function(a,c,e){f(window).trigger("quicksearch.ajax-error",{url:h,xmlHttpRequest:a,resultStatus:c,errorThrown:e});if(document.activeElement==d[0]&&c=="timeout"){a=m({statusMessage:"Timeout",query:b});k.call(g,a,null)}},complete:function(){f.removeData(g,"lastRequest");d.trigger("quick-search-loading-stop")}});f.data(g,"lastRequest",c);d.trigger("quick-search-loading-start")};var e=AJS.inputDrivenDropdown(a),d=f(this);d.keyup(function(a){a.which==13||a.which==9||!d.hasClass("placeholded")&&
e.change(this.value,false,function(a){d.attr("data-last-search",a)})});d.quickSearchControl=e;d.bind("clearCache.autocomplete",function(){e.clearCache()});d.bind("hide.autocomplete",function(){e.hide()});return d}})(jQuery);
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'confluence.web.components:core', location = '/includes/js/components/binders/placeholder.js' */
AJS.Confluence.Binder.placeholder=function(d){var d=d||document.body,e=AJS.$,f=["autocomplete-multiuser","select2-input"];"placeholder"in document.createElement("input")||e('textarea[placeholder][data-placeholder-bound!="true"],input[placeholder][data-placeholder-bound!="true"],input.default-text[data-placeholder-bound!="true"]',d).each(function(){for(var a=e(this).attr("data-placeholder-bound","true"),b=0,c=f.length;b<c;++b)if(a.hasClass(f[b]))return;a.bind("reset.placeholder",function(a,b){b.element.closest("form").bind("submit",
function(){b.element.hasClass("placeholded")&&b.element.val("")})});b=function(){if(!e.trim(a.val()).length){var b=a.attr("placeholder")||a.attr("data-default-text");a.val(b).addClass("placeholded").trigger("reset.placeholder",{element:a,defaultText:b});a.trigger("reset.default-text")}};c=function(){a.hasClass("placeholded")&&(a.val(""),a.removeClass("placeholded"))};b();a.blur(b).focus(c).mousedown(c)})};Confluence.Binder.inputDefaultText=Confluence.Binder.placeholder;
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'confluence.web.components:core', location = '/includes/js/components/binders/inserter.js' */
AJS.Confluence.Binder.insertOnEvent=function(){var a=AJS.$;a('.insert-on-event[data-inserter-bound!="true"]').each(function(){var b=a(this).attr("data-inserter-bound","true"),f=a(b.attr("data-target"))[0],c=b.attr("data-event"),d=b.attr("data-insert-position"),g=b.attr("data-insert-unique-key");f&&(c&&d)&&a(self).bind(c,function(c,e){if(f==e.target){if(g){var i=e.content[g],h={};a(".key-holder",d).each(function(){h[a(this).attr("data-key")]=!0});if(i in h)return}a(d).append(AJS.template(b.text()).fill(e.content))}})})};
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'confluence.web.components:core', location = '/includes/js/components/binders/autocomplete-user.js' */
Confluence.Binder.autocompleteUserOrGroup=function(e){var e=e||document.body,f=AJS.$,k=function(d){if(!d||!d.result)throw Error("Invalid JSON format");f.each(d.result,function(a,b){b.key=b.username||b.name;"group"===b.type&&(b.title=b.name,b.link=[],b.thumbnailLink={href:AJS.contextPath()+"/images/icons/avatar_group_48.png"})});var g=[];g.push(d.result);return g};f.each(["user","group","user-or-group"],function(d,g){f("input.autocomplete-"+g+'[data-autocomplete-user-or-group-bound!="true"]',e).each(function(){var a=
f(this).attr("data-autocomplete-user-or-group-bound","true").attr("autocomplete","off"),b=a.attr("data-max")||10,d=a.attr("data-alignment")||"left",e=a.attr("data-dropdown-target"),c=null,i=a.attr("data-target"),j=i&&f(i);e?c=f(e):(c=f("<div></div>"),a.after(c));a.attr("data-resize-to-input")&&(c.width(a.outerWidth()),c.addClass("resize-to-input"));c.addClass("aui-dd-parent autocomplete");a.quicksearch(AJS.REST.getBaseUrl()+"search/"+g+".json",function(){a.trigger("open.autocomplete-user-or-group");
g==="user"&&a.trigger("open.autocomplete-user")},{makeParams:function(a){return{"max-results":b,query:a}},dropdownPlacement:function(a){c.append(a)},makeRestMatrixFromData:k,addDropdownData:function(h){if(!h.length){var b=a.attr("data-none-message");b&&h.push([{name:b,className:"no-results",href:"#"}])}return h},ajsDropDownOptions:{alignment:d,displayHandler:function(a){return a.restObj&&a.restObj.username?a.name+" ("+AJS.escapeHtml(a.restObj.username)+")":a.name},selectionHandler:function(b,d){if(d.find(".search-for").length){a.trigger("selected.autocomplete-user-or-group",
{searchFor:a.val()});g==="user"&&a.trigger("selected.autocomplete-user",{searchFor:a.val()})}else{if(!d.find(".no-results").length){var c=f("span:eq(0)",d).data("properties");if(j){var e=c.restObj.title;c.restObj.username&&(e=e+(" ("+c.restObj.username+")"));a.val(e);j.val(c.restObj.key)}else a.val(c.restObj.key);a.trigger("selected.autocomplete-user-or-group",{content:c.restObj});g==="user"&&a.trigger("selected.autocomplete-user",{content:c.restObj})}this.hide();b.preventDefault()}}}})})});Confluence.Binder.autocompleteUser=
function(){}};
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'confluence.web.components:core', location = '/includes/js/components/binders/autocomplete-content.js' */
(function(c){var n=function(a){if(!a||!a.group)throw Error("Invalid JSON format");for(var d=[],c=0,e=a.group.length;c<e;c++)d.push(a.group[c].result);return d},e=function(a,d,e,f){var b=c(a);if(!b.attr("data-autocomplete-content-bound")){"string"==typeof d&&(d=[d]);b.attr("data-autocomplete-content-bound","true").attr("autocomplete","off");var o=d.join(","),i=b.attr("data-max")||10,a=b.attr("data-alignment")||"left",h=b.attr("data-spacekey"),j=b.attr("data-none-message"),k=b.attr("data-search-link-message"),
p=b.attr("data-template")||e,l=b.attr("data-target"),m=b.attr("data-dropdown-target"),g=null;m?g=c(m):(g=c("<div></div>"),b.after(g));g.addClass("aui-dd-parent autocomplete");b.focus(function(){h=b.attr("data-spacekey")});b.quicksearch(AJS.REST.getBaseUrl()+"search/name.json",null,{onShow:function(){b.trigger("open.autocomplete-content",{contentTypes:d})},makeParams:function(a){a={"max-results":i,pageSize:i,type:o,query:a};if(h)a.spaceKey=h;return a},dropdownPlacement:function(a){g.append(a)},makeRestMatrixFromData:n,
addDropdownData:function(a){a.length||j&&a.push([{name:j,className:"no-results",href:"#"}]);if(k){var d=AJS.escapeHtml(b.val()),d=AJS.format(k,d);a.push([{className:"search-for",name:d,href:"#"}])}return a},ajsDropDownOptions:{alignment:a,displayHandler:f,selectionHandler:function(a,f){AJS.debug("autocomplete-content: ajsDropDownOptions.selectionHandler");if(f.find(".search-for").length)b.trigger("selected.autocomplete-content",{contentTypes:d,searchFor:b.val()});else{if(f.find(".no-results").length)AJS.debug("no results selected");
else{var g=f.data("properties");b.val(AJS.template(p).fillHtml(g.restObj));if(l){var h=AJS.template(e).fillHtml(g.restObj);c(l).val(h)}b.trigger("selected.autocomplete-content",{contentTypes:d,content:g.restObj,selection:f})}this.hide();a.preventDefault()}}}})}};Confluence.Binder.autocompleteSpace=function(a){a=a||document.body;c("input.autocomplete-space",a).each(function(){e(this,["spacedesc","personalspacedesc"],"{name}",function(a){return a.name})})};Confluence.Binder.autocompleteAttachment=function(a){a=
a||document.body;c("input.autocomplete-attachment",a).each(function(){e(this,"attachment","{fileName}",function(a){var c=a.restObj&&a.restObj.fileName||a.name;a.restObj&&(a.restObj.space&&a.restObj.space.title)&&(c+=" ("+a.restObj.space.title+")");return c})})};var f=function(a){return a.restObj&&a.restObj.title&&AJS.escapeHtml(a.restObj.title)||a.name};Confluence.Binder.autocompletePage=function(a){a=a||document.body;c("input.autocomplete-page",a).each(function(){e(this,"page","{title}",f)})};Confluence.Binder.autocompleteBlogpost=
function(a){a=a||document.body;c("input.autocomplete-blogpost",a).each(function(){e(this,"blogpost","{title}",f)})};Confluence.Binder.autocompleteConfluenceContent=function(a){a=a||document.body;c("input.autocomplete-confluence-content",a).each(function(){e(this,["page","blogpost"],"{title}",f)})};Confluence.Binder.autocompleteSearch=function(a){a=a||document.body;c("input.autocomplete-search",a).each(function(){e(this,[],"{title}",f)})}})(AJS.$);
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'confluence.web.components:core', location = '/includes/js/components/attachment-uploader.js' */
(function(d){Confluence.AttachmentUploader=function(e,g){var a,f,c,b;a=d.extend({getForm:function(){return d("form",e.baseElement)},getUploadingMessageElement:function(){return d(".upload-in-progress",e.baseElement)},pack:function(){},displayErrors:function(a){b.displayMessages(a);this.pack()},clearErrors:function(){b.clearMessages();this.pack()},setUploadInProgress:function(a,c){var b=this.getUploadingMessageElement();a&&b.html(c||this.getDefaultUploadingMessage());AJS.setVisible(b,a);AJS.setVisible(this.getForm(),
!a)},onUploadSuccess:function(){},getMessageHandler:function(){b||(b=AJS.MessageHandler({baseElement:d(".warning",e.baseElement)}));return b},getDefaultErrorMessage:function(){return "Could not upload the file to Confluence. The server may be unavailable."},getDefaultUploadingMessage:function(){return "File uploading\u2026"},getContentId:function(){return AJS.Meta.get("attachment-source-content-id")}},g&&g(e));f=a.getMessageHandler();c=a.getForm();AJS.Meta.getBoolean("can-attach-files")?(c.ajaxForm({dataType:"json",
timeout:36E5,data:{contentId:a.getContentId(),responseFormat:"html"},resetForm:!0,beforeSubmit:function(){a.setUploadInProgress(!0);f.clearMessages()},success:function(b){a.setUploadInProgress(!1);if(!f.handleResponseErrors(b,a.getDefaultErrorMessage()))a.onUploadSuccess(b.attachmentsAdded||[])},error:function(b){a.setUploadInProgress(!1);f.displayMessages(a.getDefaultErrorMessage());AJS.logError("Response from server was: "+b.responseText)}}),c.find("input:file").change(function(){c.submit()})):
c.remove();return a}})(AJS.$);
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'confluence.web.components:core', location = '/includes/js/components/binders/hover-user.js' */
Confluence.Binder.userHover=function(){var d=[],f=Confluence.getContextPath(),b=AJS.$,e=function(a){var c=d[a],g={username:c,target:this};b(self).trigger("hover-user.open",g);b(".ajs-menu-bar",this).ajsMenu();b(".follow, .unfollow",this).each(function(){var a=b(this).click(function(d){if(!a.hasClass("waiting")){var e=a.hasClass("unfollow")?"/unfollowuser.action":"/followuser.action";a.addClass("waiting");AJS.safe.post(f+e+"?username="+encodeURIComponent(c)+"&mode=blank",{},function(){a.removeClass("waiting");
a.parent().toggleClass("follow-item").toggleClass("unfollow-item");b(self).trigger("hover-user.follow",g)});return AJS.stopEvent(d)}})})},h=AJS.DarkFeatures.isEnabled("show.business.group.in.user.hover");return function(){b("span.user-hover-trigger, a.confluence-userlink, img.confluence-userlink, a.userLogoLink").filter("[data-user-hover-bound!='true']").each(function(){var a=b(this),c=a.attr("data-username");a.attr("title","").attr("data-user-hover-bound","true");b("img",a).attr("title","");a=b.inArray(c,
d);-1==a&&(d.push(c),a=b.inArray(c,d));b(this).addClass("userlink-"+a)});b.each(d,function(a){var c=f+"/users/userinfopopup.action?username="+encodeURIComponent(d[a]);h&&(c+="&profileGroups=business");AJS.contentHover(b(".userlink-"+a),a,c,e)})}}();
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'confluence.web.components:core', location = '/includes/js/components/binders/favourites.js' */
(function(a){var i=[],g=function(b,d){b.attr("data-favourites-bound")||(b.delegate(".icon-add-fav, .icon-remove-fav","click",function(e){var f=a(e.target),c,e=b.attr("data-entity-type");c=d&&d.getEntityId&&"function"==typeof d.getEntityId?d.getEntityId(f):b.attr("data-entity-id");if(i[c])AJS.log("Already busy toggling favourite for "+e+" '"+c+"'. Ignoring request.");else{i[c]=!0;var h=f.hasClass("icon-remove-fav"),g=f.parent().find(".icon-wait"),j,k;"page"==e&&(j=Confluence.getContextPath()+"/json/"+
(h?"removefavourite.action":"addfavourite.action"),k={entityId:c});"space"==e&&(j=Confluence.getContextPath()+"/json/"+(h?"removespacefromfavourites.action":"addspacetofavourites.action"),k={key:c});f.addClass("hidden");g.removeClass("hidden");AJS.safe.ajax({url:j,type:"POST",data:k}).done(function(a){AJS.debug(a)}).fail(function(a,b){AJS.debug("Error Toggling Favourite: "+b)}).always(function(){g.addClass("hidden");f.parent().find(h?".icon-add-fav":".icon-remove-fav").removeClass("hidden");delete i[c];
AJS.trigger(h?"remove-fav-complete":"add-fav-complete")});return!1}}),b.attr("data-favourites-bound",!0))};AJS.Confluence.Binder.favourites=function(){a(".entity-favourites").each(function(){a(this).attr("data-favourites-bound")||g(a(this),{})})};a.fn.favourites=function(b){a(this).each(function(){var d=a(this);g(d,b)})}})(AJS.$);
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'confluence.web.components:core', location = '/includes/js/components/binders/watches.js' */
(function(a){var i=[],h=function(c,e){c.attr("data-watching-bound")||(c.delegate(".icon-start-watching, .icon-stop-watching, .link-start-watching, .link-stop-watching","click",function(g){var b=a(g.target),d,g=c.attr("data-entity-type");d=e&&e.getEntityId&&"function"==typeof e.getEntityId?e.getEntityId(b):c.attr("data-entity-id");if(i[d])AJS.log("Already busy toggling favourite for "+g+" '"+d+"'. Ignoring request.");else{i[d]=!0;var f=b.hasClass("icon-stop-watching")||b.hasClass("link-stop-watching"),
h=b.hasClass("icon-stop-watching"),j=b.parent().find(".icon-wait"),k,l;"page"==g&&(k=Confluence.getContextPath()+"/users/"+(f?"removepagenotificationajax.action":"addpagenotificationajax.action"),l={pageId:d});"space"==g&&(k=Confluence.getContextPath()+"/users/"+(f?"removespacenotificationajax.action":"addspacenotificationajax.action"),l={spaceKey:d});b.addClass("hidden");j.removeClass("hidden");AJS.safe.ajax({url:k,type:"POST",data:l,success:function(a){AJS.log(a);j.addClass("hidden");h?b.parent().find(f?
".icon-start-watching":".icon-stop-watching").removeClass("hidden"):b.parent().find(f?".link-start-watching":".link-stop-watching").removeClass("hidden");delete i[d]},error:function(a,c){j.addClass("hidden");h?b.parent().find(f?".icon-start-watching":".icon-stop-watching").removeClass("hidden"):b.parent().find(f?".link-start-watching":".link-stop-watching").removeClass("hidden");AJS.log("Error Toggling Watching: "+c);delete i[d]}});return!1}}),c.attr("data-watching-bound",!0))};AJS.Confluence.Binder.watching=
function(){a(".entity-watching").each(function(){a(this).attr("data-watching-bound")||h(a(this),{})})};a.fn.watching=function(c){a(this).each(function(){var e=a(this);h(e,c)})}})(AJS.$);
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'confluence.web.components:core', location = '/includes/soy/dialog.soy' */
// This file was automatically generated from dialog.soy.
// Please don't edit this file by hand.

if (typeof Confluence == 'undefined') { var Confluence = {}; }
if (typeof Confluence.Templates == 'undefined') { Confluence.Templates = {}; }
if (typeof Confluence.Templates.Dialog == 'undefined') { Confluence.Templates.Dialog = {}; }


Confluence.Templates.Dialog.helpLink = function(opt_data, opt_sb) {
  var output = opt_sb || new soy.StringBuilder();
  Confluence.Templates.Dialog.customHelpLink({href: opt_data.href, text: soy.$$escapeHtml("Help")}, output);
  return opt_sb ? '' : output.toString();
};


Confluence.Templates.Dialog.customHelpLink = function(opt_data, opt_sb) {
  var output = opt_sb || new soy.StringBuilder();
  output.append('<div class="dialog-help-link"><a href="', soy.$$escapeHtml(opt_data.href), '" target="_blank">', soy.$$escapeHtml(opt_data.text), '</a></div>');
  return opt_sb ? '' : output.toString();
};

} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
