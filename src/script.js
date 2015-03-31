/**
 * Created by buchholb on 3/31/15.
 */
$(document).ready(function () {
    $("#query").keyup(function () {
        processQuery($("#query").val())
    });
});

function processQuery(query) {
    console.log(encodeURIComponent(query));
    $.getJSON("/?query=" + encodeURIComponent(query), function (result) {
        res = "<ul>";
        for (i = 0; i < result.res.length; i++) {
            res += "<li>" + result.res[i] + "</li>";
        }
        $("#answer").html(res);
    });
}
