<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" 
        "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta http-equiv="expire" content="0">
<title>DocuSearch</title>
<script type="text/javascript" src="http://ajax.Microsoft.com/ajax/jQuery/jquery-1.3.2.js">
</script>
<script type="text/javascript">

var server='<%= java.net.InetAddress.getLocalHost().getHostName() %>';

function show(id) {
	var href = "/docusearch/svc/search/efile_providers/" + id;
    jQuery.ajax({
          type: "GET",
          url: href,
          dataType: "json",
          success: function(details){
			     var textToInsert = '';
			     textToInsert += '<li>' + details._id + '</li>';
			     textToInsert += '<li>' + details.business_name + '</li>';
			     textToInsert += '<li>' + details.street_address_1 + '</li>';
			     textToInsert += '<li>' + details.city + '</li>';
			     textToInsert += '<li>' + details.state + '</li>';
			     textToInsert += '<li>' + details.zip + '</li>';
			     textToInsert += '<li>' + details.contact_first_name + '</li>';
			     textToInsert += '<li>' + details.contact_last_name + '</li>';
			     textToInsert += '<li>' + details.phone + '</li>';
			     textToInsert += '<li><a href="http://' + server + ':5984/efile_providers/' + details._id + '">CouchDB</a></li>';
                 var list = $('<ul/>');
                 list.append(textToInsert);  
     	         $('#details').empty();
                 $('#details').append(list).show();  
          },
          error: function(XMLHttpRequest, textStatus, errorThrown){
              alert("Error getting details");
          }
      });
}

$(document).ready(function(){
   $(".button").click(function() {search()});
}); 
function search(){
	  var kw = $("input#keywords").val();
	  var href = "/docusearch/svc/search/efile_providers?keywords=" + kw;

      jQuery.ajax({
          type: "GET",
          url: href,
          dataType: "json",
          success: function(summary){
     	     $('#summary').empty();
             if (summary.docs.length == 0) {  
                 $('#summary').append('<h3>No data found</h3>').show();  
             }  else {  
			     var textToInsert = '';
                 $.each(summary.docs, function() {  
				     textToInsert  += '<tr><td>' + this.symbol + '</td><td><a href="#" onclick="show(' + this._id + ');">' + this.name + '</a></td></tr>';
                 });  

                 var table = $('<table />').attr('cellspacing', 0).attr('cellpadding', 4);  
                 table.append(textToInsert);  
                 $('#summary').append(table).show();  
             }  
          },
          error: function(XMLHttpRequest, textStatus, errorThrown){
              alert("Error quering");
          }
      });
  }

</script>
</head>
<body>
<form id="search" onsubmit="search();return false;">
<fieldset>  
<label for="keywords">Keywords: </label>
<input type="text" id="keywords" class="kw" name="keywords">
<label for="index">Index: </label>
<select name="index">
      <option selected value="efile_providers">Index</option>
</select>
<input type="button" id="search" name="search" class="button" value="Search">
</fieldset>  
</form>
<table>
<tr>
<td valign="top" width="50%">
<div id="summary"> 
</div>
</td>
<td valign="top" width="50%">
<div id="details"> 
</div>
</td>
</tr>
</table>
</body>
</html>
