;try {
/* module-key = 'com.atlassian.auiplugin:aui-focus-manager', location = 'js/focus-manager.js' */
AJS.FocusManager=(function(G){(function B(){function J(L){return(G.css(L,"visibility")==="visible")}function I(P,Q){var S=P.nodeName.toLowerCase();if(S==="area"){var M=P.parentNode;var R=M.name;var T=G("img[usemap=#"+R+"]").get();if(!P.href||!R||M.nodeName.toLowerCase()!=="map"){return false}return T&&J(T)}var O=/input|select|textarea|button|object/.test(S);var L=S==="a";var N=(P.href||Q);return(O?!P.disabled:(L?N:Q))&&J(P)}function K(N){var L=G.attr(N,"tabindex"),M=isNaN(L);var O=(M||L>=0);return O&&I(N,!M)}G.extend(G.expr[":"],{"aui-focusable":function(L){return I(L,!isNaN(G.attr(L,"tabindex")))},"aui-tabbable":K})}());var D="_aui-focus-restore";function C(){this._focusTrapStack=[];G(document).on("focusout",{focusTrapStack:this._focusTrapStack},F)}C.defaultFocusSelector=":aui-tabbable";C.prototype.enter=function(I){I.data(D,G(document.activeElement));if(I.attr("data-aui-focus")!=="false"){var J=I.attr("data-aui-focus-selector")||C.defaultFocusSelector;var K=I.is(J)?I:I.find(J);K.first().focus()}if(H(I)){E(I,this._focusTrapStack)}};function E(I,J){J.push(I)}function A(I){I.pop()}function H(I){return I.is(".aui-dialog2")}C.prototype.exit=function(I){if(H(I)){A(this._focusTrapStack)}var J=document.activeElement;if(I[0]===J||I.has(J).length){G(J).blur()}var K=I.data(D);if(K&&K.length){I.removeData(D);K.focus()}};function F(K){var O=K.data.focusTrapStack;if(!K.relatedTarget){return }if(O.length===0){return }var R=O[O.length-1];var I=K.target;var N=K.relatedTarget;var M=R.find(":aui-tabbable");var L=AJS.$(M.first());var J=AJS.$(M.last());var Q=R.has(N).length===0;var P=Q&&N;if(P){if(L.is(I)){J.focus()}else{if(J.is(I)){L.focus()}}}}C.global=new C();return C}(AJS.$));
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
