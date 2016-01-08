;try {
/* module-key = 'com.atlassian.auiplugin:aui-navigation', location = 'js/aui-navigation.js' */
(function(A){AJS.navigation=A(AJS.$,window.skate,AJS._internal.widget)})(function(E,A,D){function C(F){this.$el=E(F).closest(".aui-nav");if(this.$el.length>1){return this.$el.map(function(H,I){return new C(I)})[0]}if(this.$el.data("aui-navigation")){return this.$el.data("aui-navigation")}this.$el.data("aui-navigation",this);this.$treeParent=this.$el.parent("li[aria-expanded]");this.$subtreeToggleIcon=this.$treeParent.children(".aui-nav-subtree-toggle").children("span.aui-icon");this.hideMoreItems();this.$el.children("li:has(.aui-nav-selected)").addClass("aui-nav-child-selected");var G=this.$el.children(".aui-nav-selected");G.parents(".aui-nav > [aria-expanded=false]").add(G.filter("[aria-expanded=false]")).each(function(){var H=B(E(this).children(".aui-nav"));H.expand()});this.$el.find("> li[aria-expanded] > .aui-nav-subtree-toggle").on("click",function(){var H=B(E(this).siblings(".aui-nav"));H.toggle()});return this}C.prototype.isNested=function(){return this.$treeParent.length===1};C.prototype.isCollapsed=function(){return this.$treeParent.attr("aria-expanded")==="false"};C.prototype.expand=function(){this.$treeParent.attr("aria-expanded","true");this.$subtreeToggleIcon.removeClass("aui-iconfont-collapsed").addClass("aui-iconfont-expanded");this.hideMoreItems();return this};C.prototype.collapse=function(){this.$treeParent.attr("aria-expanded","false");this.$subtreeToggleIcon.removeClass("aui-iconfont-expanded").addClass("aui-iconfont-collapsed");return this};C.prototype.toggle=function(){if(this.isCollapsed()){this.expand()}else{this.collapse()}return this};C.prototype.hideMoreItems=function(){if(this.$el.is(".aui-nav:not([aria-expanded=false]) [data-more]")){var I=this.$el.attr("data-more")||"More\u2026";var H=Math.abs(parseInt(this.$el.attr("data-more-limit")))||5;var J=this.$el.children("li");var G=J.length<=H+1;var K=J.filter(".aui-nav-selected").length;var F=J.filter(".aui-nav-more").length;if(G||K||F){return this}E("<li>",{"class":"aui-nav-more","aria-hidden":"true"}).append(E("<a>",{href:"#","class":"aui-nav-item",text:I,click:function(L){L.preventDefault();E(this).parent().remove()}})).insertAfter(J.eq(H-1))}return this};var B=D("navigation",C);A("aui-nav",{insert:function(F){B(F)}});return B});
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
;try {
/* module-key = 'com.atlassian.auiplugin:aui-navigation', location = 'soy/navigation.soy' */
// This file was automatically generated from navigation.soy.
// Please don't edit this file by hand.

if (typeof aui == 'undefined') { var aui = {}; }
if (typeof aui.navigation == 'undefined') { aui.navigation = {}; }


aui.navigation.item = function(opt_data, opt_sb) {
  var output = opt_sb || new soy.StringBuilder();
  output.append('<li class="', (opt_data.isSelected || opt_data.key && opt_data.selectedItemKey == opt_data.key) ? 'aui-nav-selected' : '');
  aui.renderExtraClasses(opt_data, output);
  output.append('"', (opt_data.isCollapsible && opt_data.children && opt_data.children.length > 0) ? 'aria-expanded="' + ((opt_data.isCollapsed) ? 'false' : 'true') + '"' : '');
  aui.renderExtraAttributes(opt_data, output);
  output.append('><a class="aui-nav-item" href="', soy.$$escapeHtml(opt_data.href), '">');
  if (opt_data.iconClass && ! opt_data.isChild) {
    aui.icons.icon({icon: opt_data.iconClass, size: 'small', useIconFont: true}, output);
  }
  output.append((opt_data.count == 0 || opt_data.count) ? '<span class="aui-badge">' + soy.$$escapeHtml(opt_data.count) + '</span>' : '', '<span class="aui-nav-item-label">', soy.$$escapeHtml(opt_data.label), '</span></a>');
  if (opt_data.children && opt_data.children.length > 0) {
    if (opt_data.isCollapsible) {
      output.append('<a href="#" class="aui-nav-subtree-toggle">');
      aui.icons.icon({icon: opt_data.isCollapsed ? 'collapsed' : 'expanded', size: 'small', useIconFont: true}, output);
      output.append('</a>');
    }
    aui.navigation.nav({items: opt_data.children, selectedItemKey: opt_data.selectedItemKey, isChild: true}, output);
  }
  output.append('</li>');
  return opt_sb ? '' : output.toString();
};


aui.navigation.nav = function(opt_data, opt_sb) {
  var output = opt_sb || new soy.StringBuilder();
  output.append('<ul class="aui-nav');
  aui.renderExtraClasses(opt_data, output);
  output.append('"');
  aui.renderExtraAttributes(opt_data, output);
  output.append('>');
  var itemList57 = opt_data.items;
  var itemListLen57 = itemList57.length;
  for (var itemIndex57 = 0; itemIndex57 < itemListLen57; itemIndex57++) {
    var itemData57 = itemList57[itemIndex57];
    aui.navigation.item(soy.$$augmentData(itemData57, {isChild: opt_data.isChild, selectedItemKey: opt_data.selectedItemKey}), output);
  }
  output.append('</ul>');
  return opt_sb ? '' : output.toString();
};

} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
