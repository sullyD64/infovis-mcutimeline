$(document).ready(function () {

    // $('#sidebar').addClass('active')
    // $('.overlay').addClass('active');

    $('#sidebarDismiss, .overlay').on('click', function () {
        $('#sidebar').removeClass('active');
        $('.overlay').removeClass('active');
    });
    $('#sidebarToggle').on('click', function () {
        $('#sidebar').addClass('active');
        $('.overlay').addClass('active');
        $('.collapse.in').toggleClass('in');
        $('a[aria-expanded=true]').attr('aria-expanded', 'false');
    });
});