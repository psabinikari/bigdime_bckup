;try {
/* module-key = 'com.atlassian.auiplugin:internal-aui-animation', location = 'js/aui/internal/animation.js' */
window.AJS=window.AJS||{};(function(A){A._internal=A._internal||{};A._internal.animation={};A._internal.animation.recomputeStyle=function(B){B=B.length?B[0]:B;window.getComputedStyle(B,null).getPropertyValue("left")}}(AJS));
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
