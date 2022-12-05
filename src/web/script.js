/**
 * Created by buchholb on 3/31/15.
 */

var urlFullPath = window.location.pathname
var urlPrefix = urlFullPath.substring(0, urlFullPath.lastIndexOf('/')+1);
console.log(urlPrefix);

$(document).ready(function () {

    handleStatsDisplay();
    var ind = window.location.href.indexOf("?query=");
    if (ind > 0) {
        ind += 7;
        var ccInd = window.location.href.indexOf("&cmd=clear-cache");
        if (ccInd > 0) {
            $("#clear").prop("checked", true);
        }
        var sInd = window.location.href.indexOf("&send=");
        if (sInd > 0) {
            if (ccInd <= 0 || ccInd > sInd) {
                ccInd = sInd;
            }
        }
        var queryEscaped;
        if (ccInd > 0) {
                queryEscaped = window.location.href.substr(ind, ccInd - ind);
        } else {
                queryEscaped = window.location.href.substr(ind);
        }
        $("#query").val(decodeURIComponent(queryEscaped.replace(/\+/g, '%20')));
        processQuery(window.location.href.substr(ind - 7));
    }
    $("#runbtn").click(function () {
        var q = encodeURIComponent($("#query").val());
        var queryString = "?query=" + q;
        queryString += "&action=qlever_json_export";
        if ($("#clear").prop('checked')) {
            queryString += "&cmd=clear-cache";
        }
        queryString += "&send=100"
        var loc = window.location.href.substr(0, window.location.href.indexOf("?"));
        if (loc.length == 0) {
            loc = "index.html"
        }
        window.history.pushState("html:index.html", "QLever",
            loc + queryString);
        processQuery(queryString)
    });
    $("#csvbtn").click(function () {
        var q = encodeURIComponent($("#query").val());
        console.log(q);
        var queryString = "?query=" + q;
        if ($("#clear").prop('checked')) {
            console.log("With clear-cache");
            queryString += "&cmd=clear-cache";
        } else {
            console.log("Without clear-cache");
        }
        processCsvQuery(queryString)
    });
    $("#tsvbtn").click(function () {
        var q = encodeURIComponent($("#query").val());
        console.log(q);
        var queryString = "?query=" + q;
        if ($("#clear").prop('checked')) {
            console.log("With clear-cache");
            queryString += "&cmd=clear-cache";
        } else {
            console.log("Without clear-cache");
        }
        processTsvQuery(queryString)
    });
});


function htmlEscape(str) {
//    return str.replace(/&/g, "&amp;")
//        .replace(/</g, "&lt;")
//        .replace(/>/g, "&gt;");
    return $("<div/>").text(str).html();
}

function getShortStr(str, maxLength) {
    var pos;
    var cpy = str;
    if (cpy.charAt(0) == '<') {
        pos = cpy.lastIndexOf('/');
        var paraClose = cpy.lastIndexOf(')');
        if (paraClose > 0 && paraClose > pos) {
            var paraOpen = cpy.lastIndexOf('(', paraClose);
            if (paraOpen > 0 && paraOpen < pos) {
                pos = cpy.lastIndexOf('/', paraOpen);
            }
        }

        if (pos < 0) {
            pos += 1;
        }
        cpy = cpy.substring(pos + 1, cpy.length - 1);
        if (cpy.length > maxLength) {
            return cpy.substring(0, maxLength - 1) + "[...]"
        } else {
            return cpy;
        }
    }
    if (cpy.charAt(0) == '\"') {
        pos = cpy.lastIndexOf('\"');
        if (pos !== 0) {
            cpy = cpy.substring(0, pos + 1);
        }
        if (cpy.length > maxLength) {
            return cpy.substring(0, maxLength - 1) + "[...]\""
        } else {
            return cpy;
        }
    }
    return str;
}


function displayError(result) {
    var disp = "<div id=\"err\">";
    disp += "Query: " + "<br/>" + htmlEscape(result.query) + "<br/>" + "<br/>" + "<br/>";
    disp += "Error Msg: " + "<br/>" + htmlEscape(result.exception) + "<br/>" + "<br/>";
    disp += "</div>";
    displayStatus(disp)
}

function displayStatus(str) {
    $("#answer").html(str);
}


function processCsvQuery(query) {
    window.location.href = urlPrefix + query + "&action=csv_export";
    return false;
}

function processTsvQuery(query) {
    window.location.href = urlPrefix + query + "&action=tsv_export";
    return false;
}

function processQuery(query) {
    displayStatus("Waiting for response...");
    maxSend = 0;
    var sInd = window.location.href.indexOf("&send=");
    if (sInd > 0) {
        var nextAmp = window.location.href.indexOf("&", sInd + 1);
        if (nextAmp > 0) {
            maxSend = parseInt(window.location.href.substr(sInd + 6, nextAmp - (sInd + 6)))
        } else {
            maxSend = parseInt(window.location.href.substr(sInd + 6))
        }
    }
    var uri = urlPrefix + query;
    console.log("getJSON on URI: " +  uri);
    $.getJSON(uri, function (result) {
        if (result.status == "ERROR") {
            displayError(result);
            return;
        }
        var res = "<div id=\"res\">";
        // Time
        res += "<div id=\"time\">";
        var nofRows = result.res.length;
        res += "Number of rows (without LIMIT): " + result.resultsize + "<br/><br/>";
        res += "Time elapsed:<br>";
        res += "Total: " + result.time.total + "<br/>";
        res += "&nbsp;- Computation: " + result.time.computeResult + "<br/>";
        res += "&nbsp;- Resolving entity names + excerpts and creating JSON: "
            + (parseInt(result.time.total.replace(/ms/, ""))
            - parseInt(result.time.computeResult.replace(/ms/, ""))).toString()
            + "ms";
        res += "</div>";
        if (maxSend > 0 && maxSend <= nofRows && maxSend < parseInt(result.resultsize)) {
            res += "<div>Only transmitted " + maxSend.toString()
                + " rows out of the " + result.resultsize
                + " that were computed server-side. " +
                "<a href=\"" + window.location.href.substr(0, window.location.href.indexOf("&")) + "\">[show all]</a>";
        }
        res += "<table id=\"restab\">";
        for (var i = 0; i < result.res.length; i++) {
            res += "<tr>"
            for (var j = 0; j < result.res[i].length; ++j) {
                var value = result.res[i][j];
                if (!value) {
                  value = "";
                }
                res += "<td title=\"" + htmlEscape(value.replace(/\"/g, "&quot;")) + "\">"
                    + htmlEscape(getShortStr(value, 26))
                    + "</td>";
            }
            res += "</tr>";
        }
        res += "</table>";
        res += "<div>";
        $("#answer").html(res);
    }).fail(function(jqXHR, textStatus, errorThrown ) {
        console.log("textStatus: " + textStatus);
        console.log("errorThrown: " + errorThrown);
        var disp = "<div id=\"err\">";
        disp += "Connection problem in JQuery getJSON.";
        disp += "</div>";
        displayStatus(disp);
    });
}

function tsep(str) {
    var spl = str.toString().split('.');
    var intP = spl[0];
    var frac = spl.length > 1 ? '.' + spl[1] : '';
    var regex = /(\d+)(\d{3})/;
    while (regex.test(intP)) {
        intP =intP.replace(regex, '$1' + ',' + '$2');
    }
    return intP + frac;
}

function handleStatsDisplay() {
    $.getJSON(urlPrefix + "?cmd=stats", function (result) {
        $("#kbname").html("KB index: <b>" + result["name-index"] + "</b> ");
        $("#textname").html("Text index: <b>" + result["name-text-index"] + "</b> ");
        $("#ntriples").html("Number of triples: <b>" + tsep(result["num-triples-normal"]) + "</b> ");
        $("#nrecords").html("Number of text records: <b>" + tsep(result["num-text-records"]) + "</b> ");
        $("#nwo").html("Number of word occurrences: <b>" + tsep(result["num-word-occurrences"]) + "</b> ");
        $("#neo").html("Number of entity occurrences: <b>" + tsep(result["num-entity-occurrences"]) + "</b> ");

        if (result["num-permutations"] == "6") {
            $("#permstats").html("Registered <b>" + result["num-permutations"]
                + "</b> permutations of the KB index."
                + " &nbsp; #subjects: <b>"
                + tsep(result["num-subjects-normal"]) + "</b>"
                + " &nbsp; #predicates: <b>"
                + tsep(result["num-predicates-normal"]) + "</b>"
                + " &nbsp; #objects: <b>" + tsep(result["num-objects-normal"]) + "</b>");
        } else {
            $("#permstats").html("Registered <b>" + result["num-permutations"] + "</b> permutations of the KB index.")
        }
    });
}
