/**
 * Created by buchholb on 3/31/15.
 */
$(document).ready(function () {
    $("#runbtn").click(function () {
        processQuery($("#query").val())
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
        if (pos < 0) { pos += 1; }
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

function processQuery(query) {
    console.log(encodeURIComponent(query));
    var queryString = "/?query=" + encodeURIComponent(query);
    if ($("#clear").prop('checked')) {
        console.log("With clearcache");
        queryString += "&cmd=clearcache";
    } else {
        console.log("Without clearcache");
    }

    $.getJSON(queryString, function (result) {
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
                    +"</td>";
            }
            res += "</tr>";
        }
        res += "</table>";
        res += "<div>";
        $("#answer").html(res);
    });
}
