;try {
/* module-key = 'com.gliffy.integration.confluence:gliffy-confluence-editor-launcher', location = 'gliffy/controls/imageselection/ImageSelectionControl.soy' */
// This file was automatically generated from ImageSelectionControl.soy.
// Please don't edit this file by hand.

if (typeof gliffySoy == 'undefined') { var gliffySoy = {}; }
if (typeof gliffySoy.widget == 'undefined') { gliffySoy.widget = {}; }
if (typeof gliffySoy.widget.image == 'undefined') { gliffySoy.widget.image = {}; }


gliffySoy.widget.image.gliffyImageSelectionWidget = function(opt_data, opt_sb) {
  var output = opt_sb || new soy.StringBuilder();
  output.append('<div class="gliffy-image-selection"><div class="gliffy-image-selection-inner-panel" title="', soy.$$escapeHtml(opt_data.labelText), '"><div class="gliffy-image-selection-spinner"/><div><img class="gliffy-image-selection-image" src="', soy.$$escapeHtml(opt_data.imageUrl), '"/><div class="gliffy-image-selection-icon gliffy-image-selection-zoom"><img/></div></div></div><div class="gliffy-image-selection-label">', soy.$$escapeHtml(opt_data.labelText), '</div></div>');
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
/* module-key = 'com.gliffy.integration.confluence:gliffy-confluence-editor-launcher', location = 'gliffy/controls/imageselection/ImageSelectionControlFactory.js' */
define("gliffy/controls/imageselection/imageSelectionControlFactory",["jquery","gliffy/objectValidatorFactory"],function(c,b){var a=function(h){var e=this;b.create({description:"ListTable constructor parameters",keyToPredicateMap:{parent:b.predicates.isDefinedAndNotNull,labelText:b.predicates.isString,imageUrl:b.predicates.isString}}).validate(h);var h=c.extend({isSelected:false},h);var f=gliffySoy.widget.image.gliffyImageSelectionWidget(h);var d=c(f).appendTo(h.parent);d.on("click",function(j){e.setIsSelected(true)});var i=d.find(".gliffy-image-selection-image");var g=d.find(".gliffy-image-selection-zoom");if(i.previewer){g.previewer({src:i.attr("src"),type:"image/png",zindex:10000})}else{g.on("click",function(k){var j=i.clone();j.removeClass("gliffy-image-selection-image");j.addClass("gliffy-image-selection-fancybox-image");c.fancybox({orig:i,content:j,showCloseButton:true,hideOnOverlayClick:true,hideOnContentClick:true})})}this._={domRoot:d,labelText:h.labelText,imageUrl:h.imageUrl,isSelected:!h.isSelected,selectionChangedListeners:[],};this.setIsSelected(h.isSelected)};a.prototype.getIsSelected=function(){return this._.isSelected};a.prototype.setIsSelected=function(e){if(this._.isSelected===e){return}this._.isSelected=e;if(e){this._.domRoot.addClass("gliffy-image-selected")}else{this._.domRoot.removeClass("gliffy-image-selected")}var d=this;this._.selectionChangedListeners.forEach(function(f){f({source:d,isSelected:e})})};a.prototype.getDomRoot=function(){return this._.domRoot};a.prototype.addSelectionChangedListener=function(d){this._.selectionChangedListeners.push(d)};return{create:function(d){return new a(d)}}});
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'com.gliffy.integration.confluence:gliffy-confluence-editor-launcher', location = 'gliffy/confluence/versionselection/VersionSelectionDialog.soy' */
// This file was automatically generated from VersionSelectionDialog.soy.
// Please don't edit this file by hand.

if (typeof gliffySoy == 'undefined') { var gliffySoy = {}; }
if (typeof gliffySoy.dialog == 'undefined') { gliffySoy.dialog = {}; }
if (typeof gliffySoy.dialog.version == 'undefined') { gliffySoy.dialog.version = {}; }


gliffySoy.dialog.version.versionSelectionDialogBody = function(opt_data, opt_sb) {
  var output = opt_sb || new soy.StringBuilder();
  output.append('<form action="#" method="post" class="aui top-label"><div class="gliffy-version-dialog-panel"><div class="gliffy-version-dialog-description">', soy.$$escapeHtml(opt_data.promptText), '</div><div class="gliffy-image-selection-widget-area"/></div></form>');
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
/* module-key = 'com.gliffy.integration.confluence:gliffy-confluence-editor-launcher', location = 'gliffy/confluence/versionselection/VersionSelectionDialogFactory.js' */
define("gliffy/confluence/versionselection/versionSelectionDialogFactory",["jquery","gliffy/controls/imageselection/imageSelectionControlFactory","gliffy/objectValidatorFactory"],function(c,d,b){var a=function(f){var e=this;var f=c.extend({onCancel:function(){}},f);b.create({description:"VersionSelectionDialog constructor parameters",keyToPredicateMap:{headerText:b.predicates.isString,promptText:b.predicates.isString,attachmentVersion:b.predicates.isNumber,diagramName:b.predicates.isString,ownerPageId:b.predicates.isDefinedAndNotNull,referencePageId:b.predicates.isDefinedAndNotNull,pinnedIsDefault:b.predicates.isDefinedAndNotNull,onComplete:b.predicates.isFunction}}).validate(f);this._={hasBeenShown:false,parameters:f,pinnedVersionIsSelected:f.pinnedIsDefault,buildDom:function(){var h=gliffySoy.dialog.version.versionSelectionDialogBody({promptText:f.promptText});var k=c(h);var j=k.find(".gliffy-image-selection-widget-area");var i=d.create({parent:j,labelText:AJS.format("Version {0}",f.attachmentVersion),imageUrl:AJS.format("{0}/download/attachments/{1}/{2}.png?api=v2&version={3}&modificationDate=0",e._.parameters.contextPath,e._.parameters.ownerPageId,encodeURIComponent(e._.parameters.diagramName),e._.parameters.attachmentVersion),isSelected:f.pinnedIsDefault});var g=d.create({parent:j,labelText:"Latest",imageUrl:AJS.format("{0}/download/attachments/{1}/{2}.png?api=v2&timestamp={3}",e._.parameters.contextPath,e._.parameters.ownerPageId,encodeURIComponent(e._.parameters.diagramName),new Date().getTime()),isSelected:!f.pinnedIsDefault});i.getDomRoot().addClass("left");g.getDomRoot().addClass("right");i.addSelectionChangedListener(function(l){if(l.isSelected){g.setIsSelected(false);e._.pinnedVersionIsSelected=true}});g.addSelectionChangedListener(function(l){if(l.isSelected){i.setIsSelected(false);e._.pinnedVersionIsSelected=false}});return k}}};a.prototype.show=function(){var e=this;if(e._.hasBeenShown){throw new Error("versionSelectionDialogFactory can be shown only once per instance lifetime.")}var f=new AJS.Dialog({width:600,height:380,closeOnOutsideClick:false});f.addHeader(e._.parameters.headerText);f.addPanel("",e._.buildDom(),"panel-body");f.addButton("Select",function(g){g.remove();e._.parameters.onComplete(e._.pinnedVersionIsSelected)});f.show();e._.hasBeenShown=true};return{create:function(e){return new a(e)}}});
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'com.gliffy.integration.confluence:gliffy-confluence-editor-launcher', location = 'gliffy/confluence/versionselection/VersionSelectionDialogController.js' */
define("gliffy/confluence/versionselection/versionSelectionDialogController",["jquery","gliffy/objectValidatorFactory","gliffy/confluence/versionselection/versionSelectionDialogFactory"],function(b,a,c){return{showUnpinMacroDiagramVersionDialog:function(d){c.create(b.extend({headerText:"Select Diagram Version to Display",promptText:AJS.format("Version {0} is currently pinned to this page. Which version would you like to display?",d.attachmentVersion),pinnedIsDefault:false},d)).show()},showSelectVersionToEditDialog:function(d){c.create(b.extend({headerText:"Select Diagram Version to Edit",promptText:AJS.format("Version {0} is currently pinned to this page. Which version you would like to edit?",d.attachmentVersion),pinnedIsDefault:true},d)).show()}}});
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'com.gliffy.integration.confluence:gliffy-confluence-editor-launcher', location = 'gliffy/confluence/versionselection/VersionSelectionIntegration.js' */
(function(a){a(function(){var b=function(f){var g=AJS.format("{0}/rest/api/content/{1}?expand=body.storage,version,space,ancestors",AJS.Confluence.getContextPath(),f.referencePageId);a.ajax({type:"GET",url:g,success:function(h){a.ajax({type:"GET",url:g,success:function(i){var m=i.body.storage.value;var n=a("<div/>").append(m);var k=n.find("ac\\:parameter[ac\\:name='']");k.remove();var l=n.find("p:has(ac\\:parameter[ac\\:name='version'])p:has(ac\\:parameter[ac\\:name='name'])").filter(function(){var q=a(this).find("ac\\:parameter[ac\\:name='version']").text()===f.attachmentVersion.toString();var p=a(this).find("ac\\:parameter[ac\\:name='name']").text()===f.diagramName;var o=a(this).find("ac\\:parameter[ac\\:name='pageid']");var r=((o.length===0)&&(f.referencePageId===f.ownerPageId))||(o.text()===f.ownerPageId.toString());return q&&p&&r});var j=l.find("ac\\:parameter[ac\\:name='version']");j.remove();h.body.storage.value=n.html();h.version.number=h.version.number+1;if(h.ancestors){if(h.ancestors.length===0){delete h.ancestors}else{if(h.ancestors.length>1){h.ancestors=[h.ancestors.pop()]}}}a.ajax({type:"PUT",contentType:"application/json; charset=utf-8",url:g,data:JSON.stringify(h),dataType:"text",processData:false,success:function(){window.location.reload()}})}})}})};var c=function(f){var h=AJS.format("{0}/rpc/json-rpc/confluenceservice-v2",AJS.Confluence.getContextPath());var i=h+"/getPage";var g=[f.referencePageId];a.ajax({type:"POST",url:i,contentType:"application/json",data:JSON.stringify(g),success:function(k){var n=k.content;var p=a("<div/>").append(n);var m=p.find("ac\\:parameter[ac\\:name='']");m.remove();var o=p.find("p:has(ac\\:parameter[ac\\:name='version'])p:has(ac\\:parameter[ac\\:name='name'])").filter(function(){var s=a(this).find("ac\\:parameter[ac\\:name='version']").text()===f.attachmentVersion.toString();var r=a(this).find("ac\\:parameter[ac\\:name='name']").text()===f.diagramName;var q=a(this).find("ac\\:parameter[ac\\:name='pageid']");var t=((q.length===0)&&(f.referencePageId===f.ownerPageId))||(q.text()===f.ownerPageId.toString());return s&&r&&t});var l=o.find("ac\\:parameter[ac\\:name='version']");l.remove();var j={jsonrpc:"2.0",method:"updatePage",params:[{id:k.id,space:k.space,title:k.title,parentId:k.parentId,version:k.version,content:p.html()},{versionComment:"Unpinned macro version",minorEdit:false}],id:f.referencePageId};a.ajax({type:"POST",contentType:"application/json",url:h,data:JSON.stringify(j),success:function(){window.location.reload()}})}})};var e=function(f){var g;if(f.ownerPageId===Confluence.Editor.getContentId()){g=AJS.format('.editor-inline-macro[data-macro-parameters="name={0}|version={1}"]',f.diagramName,f.attachmentVersion)}else{g=AJS.format('.editor-inline-macro[data-macro-parameters="name={0}|pageid={2}|version={1}"]',f.diagramName,f.attachmentVersion,f.ownerPageId)}var h=AJS.Rte.getEditor().dom.select(g);AJS.$.each(h,function(j,i){var k=GLIFFY._confluence.utils.getMacroParams(i);k.version=undefined;var l={contentId:Confluence.Editor.getContentId(),macro:{name:"gliffy",params:k,defaultParameterValue:"",body:""}};GLIFFY.confluence.insertMacro(l,i)})};var d=function(f){var k=window.localStorage.getItem("com.gliffy.confluence.diagram.edited");if(k){window.localStorage.removeItem("com.gliffy.confluence.diagram.edited");var j=JSON.parse(k);if(j){var m=require("gliffy/url").parse(j.url).queryParams;var l=0;if(m.originalAttachmentVersion){l=parseInt(m.originalAttachmentVersion)}else{if(m.attachmentVersion){l=parseInt(m.attachmentVersion)}}var i=a.find(AJS.format('[data-pageid="{0}"]',m.pageId));if(l>0&&(m.inline==="true"||i.length>0)){var h=AJS.version.split("."),g=!(parseInt(h[0])>=5&&parseInt(h[1])>=4),n={diagramName:m.name,attachmentId:m.attachmentId,attachmentVersion:l,ownerPageId:m.ceoid,referencePageId:m.pageId};require("gliffy/confluence/versionselection/versionSelectionDialogController").showUnpinMacroDiagramVersionDialog(a.extend({contextPath:AJS.Confluence.getContextPath(),onComplete:function(o){if(!o){if(f){e(n)}else{if(g){c(n)}else{b(n)}}}}},n))}}}};AJS.bind("gliffy.confluence.inlineEditorClosed",function(){d(true)});a(function(){d(false)})})})(require("jquery"));
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'com.gliffy.integration.confluence:gliffy-confluence-editor-launcher', location = 'gliffy/confluence/editor/EditorLauncher.js' */
define("gliffy/confluence/editor/editorLauncher",["jquery","gliffy/objectValidatorFactory","gliffy/confluence/versionselection/versionSelectionDialogController"],function(d,c,a){var b=function(e,g,f,i,h){if(i&&i>0){d.ajax({url:e+"/rest/gliffy/1.0/diagrams/allVersionInformation",type:"GET",data:{name:g,pageId:f},success:function(j){h(j.numRevisions!==parseInt(i))},error:function(j){h(false)}})}else{h(false)}};return{launchEditorFromViewPage:function(e){var e=d.extend({contextPath:AJS.Confluence.getContextPath()},e);b(e.contextPath,e.diagramName,e.ownerPageId,e.attachmentVersion,function(f){if(f){a.showSelectVersionToEditDialog(d.extend({onComplete:function(g){var h;if(g){h=e.url}else{h=e.url.replace("attachmentVersion=","originalAttachmentVersion=")}window.open(h,"_self")}},e))}else{window.open(e.url,"_self")}})},launchEditorFromEditPage:function(e){var e=d.extend({contextPath:AJS.Confluence.getContextPath()},e);b(e.contextPath,e.diagramName,e.ownerPageId,e.attachmentVersion,function(f){if(f){a.showSelectVersionToEditDialog(d.extend({onComplete:function(g){if(!g){e.originalAttachmentVersion=e.attachmentVersion;e.attachmentVersion=0}AJS.trigger("gliffy.confluence.launchInlineEditor",[e])}},e))}else{AJS.trigger("gliffy.confluence.launchInlineEditor",[e])}})}}});
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
