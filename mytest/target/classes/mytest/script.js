/**
 * 
 */
$(document).ready(function () {
    $.getJSON(url,
    function (result) {
        var tr;
        for (var i = 0; i < result.length; i++) {
            tr = $('<tr/>');
            tr.append("<td>" + json[i] + "</td>");
            tr.append("<td>" + json[i] + "</td>");
            $('table').append(tr);
        }
    });
});