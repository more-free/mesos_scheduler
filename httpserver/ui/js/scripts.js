// Empty JS for your own code to be here

function createTask() {
  var task = $('#newTask').val();
  $.ajax({
    type : 'POST',
    dataType : 'json',
    url : 'create',
    data : task,
    error : function (jqXHR, textStatus, errorThrown) {
        $('#taskStatus').html('submission failed. please check and retry.');
        $('#taskStatus').css('color', 'red');
    },
    success : function (data, textStatus, jqXHR) {
        var text = "task id = " + data.id;
        $('#taskStatus').html('submission succeeded ! ' + text);
        $('#taskStatus').css('color', 'green');

        // polling new task list here
    }
  });
}
