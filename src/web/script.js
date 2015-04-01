/**
 * Created by buchholb on 3/31/15.
 */
$(document).ready(function () {
    $("#runbtn").click(function () {
        processQuery($("#query").val())
    });
});

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
                res += "<td>"
                + result.res[i][j].replace(/&/g, "&amp;")
                    .replace(/</g, "&lt;")
                    .replace(/>/g, "&gt;")
                + "</td>"
            }
            res += "</tr>";
        }
        res += "</table>";
        res += "<div>";
        $("#answer").html(res);
    });
}
