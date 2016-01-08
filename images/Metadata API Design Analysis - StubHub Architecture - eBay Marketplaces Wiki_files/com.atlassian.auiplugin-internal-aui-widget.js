;try {
/* module-key = 'com.atlassian.auiplugin:internal-aui-widget', location = 'js/aui/internal/widget.js' */
(function(A){AJS._internal=AJS._internal||{};AJS._internal.widget=A(AJS.$)})(function(B){return function A(D,C){var E="_aui-widget-"+D;return function(K,I){var F;var G;if(B.isPlainObject(K)){G=K}else{F=K;G=I}var H=F&&B(F);var J;if(!H||!H.data(E)){J=new C(H,G||{});H=J.$el;H.data(E,J)}else{J=H.data(E)}return J}}});
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
