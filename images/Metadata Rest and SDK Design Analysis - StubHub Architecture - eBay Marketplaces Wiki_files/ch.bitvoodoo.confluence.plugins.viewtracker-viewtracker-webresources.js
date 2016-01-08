;try {
/* module-key = 'ch.bitvoodoo.confluence.plugins.viewtracker:viewtracker-webresources', location = 'ch/bitvoodoo/confluence/plugins/viewtracker/scripts/viewtracker.js' */
AJS.toInit(function(a){a(".bv_viewracker_span").click(function(){$viewtracker=a(this).parent(".bv_viewtracker");$this=a(this);if($this.hasClass("bv_viewtracker_closed")){$this.removeClass("bv_viewtracker_closed").addClass("bv_viewtracker_expanded");$viewtracker.next(".bv_viewtracker_visits").slideDown()}else{$this.addClass("bv_viewtracker_closed").removeClass("bv_viewtracker_expanded");$viewtracker.next(".bv_viewtracker_visits").slideUp()}})});
} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
