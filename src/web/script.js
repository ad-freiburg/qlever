/**
 * Created by buchholb on 3/31/15.
 */
$(document).ready(function () {
    var ind = window.location.href.indexOf("?query=");
    if (ind > 0) {
        ind += 7;
        var ccInd = window.location.href.indexOf("&cmd=clearcache");
        if (ccInd > 0) {
            $("#clear").prop("checked", true);
            $("#query").val(decodeURIComponent(
                window.location.href.substr(ind, ccInd - ind)));
        } else {
            $("#query").val(decodeURIComponent(
                window.location.href.substr(ind)));
        }
        processQuery(window.location.href.substr(ind - 7));
    }
    $("#runbtn").click(function () {
        var q = encodeURIComponent($("#query").val());
        console.log(q);
        var queryString = "?query=" + q;
        if ($("#clear").prop('checked')) {
            console.log("With clearcache");
            queryString += "&cmd=clearcache";
        } else {
            console.log("Without clearcache");
        }
        var loc = window.location.href.substr(0, window.location.href.indexOf("?"));
        if (loc.length == 0) {
            loc = "index.html"
        }
        window.history.pushState("object or string", "Title",
            loc + queryString);
        processQuery(queryString)
    });
    $("#csvbtn").click(function () {
        var q = encodeURIComponent($("#query").val());
        console.log(q);
        var queryString = "?query=" + q;
        if ($("#clear").prop('checked')) {
            console.log("With clearcache");
            queryString += "&cmd=clearcache";
        } else {
            console.log("Without clearcache");
        }
        processCsvQuery(queryString)
    });
    $("#tsvbtn").click(function () {
        var q = encodeURIComponent($("#query").val());
        console.log(q);
        var queryString = "?query=" + q;
        if ($("#clear").prop('checked')) {
            console.log("With clearcache");
            queryString += "&cmd=clearcache";
        } else {
            console.log("Without clearcache");
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
    disp += "Query: " + "<br/>" + result.query  + "<br/>" + "<br/>" + "<br/>";
    disp += "Error Msg: " + "<br/>" + result.exception + "<br/>" + "<br/>";
    $("#answer").html(disp);
}

function processCsvQuery(query) {
        window.location.href = "/" + query + "&action=csv_export";
        return false;
}

function processTsvQuery(query) {
    window.location.href = "/" + query + "&action=tsv_export";
    return false;
}

function processQuery(query) {
    $.getJSON("/" + query, function (result) {
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
        res += "&nbsp;- Creating JSON: "
            + (parseInt(result.time.total.replace(/ms/, ""))
            - parseInt(result.time.computeResult.replace(/ms/, ""))).toString()
            + "ms";
        res += "</div>";
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
    });
}
