/**
 * Created by buchholb on 3/31/15.
 */
$(document).ready(function () {
    $("#runbtn").click(function () {
        processQuery()
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
    disp += "Query: " + "<br/>" + result.query + "<br/>" + "<br/>" + "<br/>";
    disp += "Error Msg: " + "<br/>" + result.exception + "<br/>" + "<br/>";
    disp += "</div>";
    displayStatus(disp)
}

function displayStatus(str) {
    $("#answer").html(str);
}


function processCsvQuery(query) {
    window.location.href = "/" + query + "&action=csv_export";
    return false;
}

function processTsvQuery(query) {
    window.location.href = "/" + query + "&action=tsv_export";
    return false;
}

function processQuery() {
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
    $.getJSON("/json", function (result) {
        if (result.status == "ERROR") {
            displayError(result);
            return;
        }
        var res = "<div id=\"res\">";
        // Time
        res += "<div id=\"time\">";
        res += "Number of rows: " + result.resultsize + "<br/><br/>";
        res += "Time elapsed:<br>";
        res += "Total: " + result.time.total + "<br/>";
        res += "&nbsp;- Computation: " + result.time.computeResult + "<br/>";
        res += "&nbsp;- Resolving entity names + excerpts and creating JSON: "
            + (parseInt(result.time.total.replace(/ms/, ""))
            - parseInt(result.time.computeResult.replace(/ms/, ""))).toString()
            + "ms";
        res += "</div>";
        if (maxSend > 0 && maxSend < parseInt(result.resultsize)) {
            res += "<div>Only transmitted " + maxSend.toString()
                + " rows out of the " + result.resultsize
                + " that were computed server-side. " +
                "<a href=\"" + window.location.href.substr(0, window.location.href.indexOf("&")) + "\">[show all]</a>";
        }
        res += "<table id=\"restab\">";
        for (var i = 0; i < result.res.length; i++) {
            res += "<tr>"
            for (var j = 0; j < result.res[i].length; ++j) {
                res += "<td title=\"" + htmlEscape(result.res[i][j]).replace(/\"/g, "&quot;") + "\">"
                    + htmlEscape(getShortStr(result.res[i][j], 26))
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