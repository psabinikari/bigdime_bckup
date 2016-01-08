;try {
/* module-key = 'com.atlassian.confluence.restplugin:confluence-rest', location = 'js/confluence-rest.js' */
AJS.REST = (function($) {
    var baseUrl = "/rest/prototype/1/";

    AJS.safeHTML = function (html) {
        return html.replace(/[<>&"']/g, function (symbol) {
            return "&#" + symbol.charCodeAt() + ";";
        });
    };

    /**
     * Converts a single object in REST format into an object in the format expected by AJS.dropDown.
     */
    var getDropdownObjectForRestResult = function (result) {
        if (!result) {
            AJS.log("REST result is null");
            return null;
        }
        if (!result.link || !$.isArray(result.link)) {
            AJS.log("No link for result with title: " + result.title);
            return null;
        }
        if (!result.title) {
            AJS.log("No title for result with id: " + result.id);
            return null;
        }
        var obj = {
            href : encodeURI(AJS.REST.findLink(result.link)),
            name : AJS.safeHTML(result.title),
            spaceName: AJS.safeHTML(AJS.REST.findSpaceName(result)),
            restObj : result
        };
        if (result.thumbnailLink) {
            var versionQuery = result.version ? "?version="+result.version + "&modificationDate=" : "";
            obj.icon = result.thumbnailLink.href + versionQuery;
        } else {
            obj.className = result.iconClass || "content-type-" + result.type + (result.type == "space" ? "desc" : "");
        }
        return obj;
    };

    return {

        getBaseUrl: function() {
            return baseUrl;
        },

        /**
         * Takes a relative path,
         *
         *   e.g. 'session/history.json?max-results=20'
         *
         * and prefixes it with the context and REST paths to form a complete '/'-based URL,
         *
         * e.g.  '/confluence/rest/prototype/1/session/history.json?max-results=20'
         *
         * @param path end of URL to be prefixed
         */
        makeUrl: function (path) {
            return Confluence.getContextPath() + baseUrl + path;
        },

        /**
         * Iterates through the links array to find the first matching link of the given type and rel.
         * @param links typically the link field on a REST JSON object
         * @param type type of link. Defaults to "text/html" if not defined.
         * @param rel relationship of the link. Defaults to "alternate".
         */
        findLink: function(links, type, rel) {
            if (!type) type = "text/html";
            if (!rel) rel = "alternate";
            if (AJS.$.isArray(links)) {
                for (var i=0,ii=links.length; i<ii; i++) {
                    var link = links[i];
                    if (link.type == type && link.rel == rel) {
                        return link.href;
                    }
                }
            }
            return "#";
        },

        findSpaceName: function(restObj){
            if (restObj.space){
                return restObj.space.name;
            }
            return "";
        },

        /**
         * Converts a matrix in REST format into a matrix in the format expected by AJS.dropDown.
         *
         * @param restMatrix matrix of objects in Confluence REST format
         * @return matrix of objects in Confluence drop-down format
         */
        convertFromRest: function (restMatrix) {
            var matrix = [], catArray, obj;
            for (var i = 0, len = restMatrix.length; i < len; i++) {
                catArray = [];
                for (var j = 0, len2 = restMatrix[i].length; j < len2; j++) {
                    obj = getDropdownObjectForRestResult(restMatrix[i][j]);
                    obj && catArray.push(obj);
                }
                catArray.length && matrix.push(catArray);
            }
            return matrix;
        },

        /**
         * Given an ContentEntityObject's REST data construct the alias, destination, href and wiki-markup.
         *
         * @param data - the content data in REST format
         */
        wikiLink : function (data) {
            var alias = data.title,
                destination = data.wikiLink && data.wikiLink.slice(1, -1); // remove the [ and ]

            // CONF-18940 strip off the space key and page title if linking to an attachment on the current page
            if (destination && data.type == "attachment" && data.space && data.space.key == AJS.Meta.get('space-key') &&
                data.ownerId == AJS.params.attachmentSourceContentId) {
                    destination = destination.substring(destination.indexOf("^"));
            }

            var wikiMarkup = destination && (alias != destination ? (alias + "|") : "") + destination;
            AJS.log("AJS.Editor.Autocompleter.Manager.makeLinkDetails =>" + wikiMarkup);

            return {
                alias : alias,
                destination : destination,
                href : this.findLink(data.link),
                wikiMarkup : wikiMarkup
            };
        },

        /**
         * Converts an object in REST format into a matrix containing the REST data.
         *
         * @async - called from an AJAX callback method
         * @param restObj object in Confluence REST format
         */
        makeRestMatrixFromData: function (restObj, suggestionField) {
            var restMatrix = [];
            var resultArr = eval("restObj." + suggestionField);
            if (resultArr && resultArr.length)
                    restMatrix.push(resultArr);
            return restMatrix;
        },

        /**
         * Converts an object in REST format into a matrix containing the search REST data.
         *
         * @async - called from an AJAX callback method
         * @param restObj object in Confluence REST format
         * @param suggestionField - the name of the field in the resObj that stores the suggestion. If null, "group" is used.
         * The "group" is the field used for in the /search/name REST service. 
         */
        makeRestMatrixFromSearchData: function(restObj, suggestionField) {
            var restMatrix = [];
            suggestionField = suggestionField || "group";
            var resultArr = eval("restObj." + suggestionField);
            if (resultArr) {

                var set = {
                    content: [],
                    attachment: [],
                    userinfo: [],
                    spacedesc: [],
                    page: [],
                    blogpost: [],
                    comment: [],
                    personalspacedesc: [],
                    mail: []
                };
                for (var i = 0, ii = resultArr.length; i < ii; i++) {
                    var group = resultArr[i];
                    set[group.name] && set[group.name].push(group.result);
                }

                // This line determines the order that the search sections appear. Don't change this unless you have to.
                restMatrix = restMatrix.concat(set.content, set.attachment, set.userinfo, set.spacedesc, set.page, set.blogpost, set.comment, set.personalspacedesc, set.mail);
            }
            else {
                log("makeRestMatrixFromData", "WARNING: Unknown rest object", restObj);
            }

            return restMatrix;
        }
    };
})(AJS.$);

} catch (err) {
    if (console && console.log && console.error) {
        console.log("Error running batched script.");
        console.error(err);
    }
}

;
