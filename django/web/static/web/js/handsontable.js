let hot1;
$(function () {
    fixAdminLTELayout(function () {
        reRenderHandsontable();
        calcControlSidebarHeight();
    });

    // rerender handsontable when click to open/close sidebar
    $(".sidebar-toggle").click(function () {
        reRenderHandsontable();
    });

    // close sidebar control and rerender handsontable
    $(".control-sidebar .close").click(function () {
        closeRightSidebar(reRenderHandsontable());
    });

    //rerender handsontable when click to open/close horizontal multistep
    $('.multiStep-horizontal .close-multistep').unbind('click').click(function () {
        toggleMultistep(reRenderHandsontable());
    });

    // rerender handsontable when click to open/close console
    $('#toggle-console').unbind('click').click(function () {
        toggleConsole(reRenderHandsontable());
    });

    // DEMO
    // populate handsontable

    // handsontable
    const data1 = [
        ['', 'Tesla', 'Nissan', 'Toyota', 'Honda', 'Mazda', 'Ford'],
        ['2017', 10, 11, 12, 13, 15, 16],
        ['2018', 10, 11, 12, 13, 15, 16],
        ['2019', 10, 11, 12, 13, 15, 16],
        ['2020', 10, 11, 12, 13, 15, 16],
        ['2021', 10, 11, 12, 13, 15, 16],
        ['2017', 10, 11, 12, 13, 15, 16],
        ['2018', 10, 11, 12, 13, 15, 16],
        ['2019', 10, 11, 12, 13, 15, 16],
        ['2020', 10, 11, 12, 13, 15, 16],
        ['2021', 10, 11, 12, 13, 15, 16],
        ['2017', 10, 11, 12, 13, 15, 16],
        ['2018', 10, 11, 12, 13, 15, 16],
        ['2019', 10, 11, 12, 13, 15, 16],
        ['2020', 10, 11, 12, 13, 15, 16],
        ['2021', 10, 11, 12, 13, 15, 16],
        ['2017', 10, 11, 12, 13, 15, 16],
        ['2018', 10, 11, 12, 13, 15, 16],
        ['2019', 10, 11, 12, 13, 15, 16],
        ['2020', 10, 11, 12, 13, 15, 16],
        ['2021', 10, 11, 12, 13, 15, 16],
        ['2017', 10, 11, 12, 13, 15, 16],
        ['2018', 10, 11, 12, 13, 15, 16],
        ['2019', 10, 11, 12, 13, 15, 16],
        ['2020', 10, 11, 12, 13, 15, 16],
        ['2021', 10, 11, 12, 13, 15, 16],
        ['2017', 10, 11, 12, 13, 15, 16],
        ['2018', 10, 11, 12, 13, 15, 16],
        ['2019', 10, 11, 12, 13, 15, 16],
        ['2020', 10, 11, 12, 13, 15, 16],
        ['2021', 10, 11, 12, 13, 15, 16],
        ['2017', 10, 11, 12, 13, 15, 16],
        ['2018', 10, 11, 12, 13, 15, 16],
        ['2019', 10, 11, 12, 13, 15, 16],
        ['2020', 10, 11, 12, 13, 15, 16],
        ['2021', 10, 11, 12, 13, 15, 16],
        ['2017', 10, 11, 12, 13, 15, 16],
        ['2018', 10, 11, 12, 13, 15, 16],
        ['2019', 10, 11, 12, 13, 15, 16],
        ['2020', 10, 11, 12, 13, 15, 16],
        ['2021', 10, 11, 12, 13, 15, 16],
        ['2017', 10, 11, 12, 13, 15, 16],
        ['2018', 10, 11, 12, 13, 15, 16],
        ['2019', 10, 11, 12, 13, 15, 16],
        ['2020', 10, 11, 12, 13, 15, 16],
        ['2021', 10, 11, 12, 13, 15, 16],
        ['2017', 10, 11, 12, 13, 15, 16],
        ['2018', 10, 11, 12, 13, 15, 16],
        ['2019', 10, 11, 12, 13, 15, 16],
        ['2020', 10, 11, 12, 13, 15, 16],
        ['2021', 10, 11, 12, 13, 15, 16],
        ['2017', 10, 11, 12, 13, 15, 16],
        ['2018', 10, 11, 12, 13, 15, 16],
        ['2019', 10, 11, 12, 13, 15, 16],
        ['2020', 10, 11, 12, 13, 15, 16],
        ['2021', 10, 11, 12, 13, 15, 16],
        ['2017', 10, 11, 12, 13, 15, 16],
        ['2018', 10, 11, 12, 13, 15, 16],
        ['2019', 10, 11, 12, 13, 15, 16],
        ['2020', 10, 11, 12, 13, 15, 16],
        ['2021', 10, 11, 12, 13, 15, 16]
    ];
    const $container1 = $('#HOT');
    if ($container1) {
        $container1.handsontable({
            data: data1,
            readOnly: true,
            minSpareRows: 0,
            sortIndicator: true,
            columnSorting: true,
            stretchH: 'all',
            preventOverflow: 'horizontal',
            selectionMode: 'single',
            height: calcPageContentHeight(),
            licenseKey: 'non-commercial-and-evaluation'
        });

        hot1 = $("#HOT").handsontable('getInstance');
    }

    //open right sidebar when clicked on a Handsontable cell
    $("#HOT td").click(function () {
        $("body").addClass("control-sidebar-open");
        calcControlSidebarHeight();
        reRenderHandsontable();
    });

});

$(window).resize(function () {
    reRenderHandsontable();
});

function reRenderHandsontable() {
    $('.content-wrapper').css('overflow', 'hidden');
    setTimeout(function () {
        const newWidthTable =
            $('body').filter('.inner-control-sidebar.control-sidebar-open').length > 0
                ? $('.content').width() - $('.control-sidebar').width()
                : $('.content').width();

        if (hot1) {
            hot1.updateSettings({
                height: calcPageContentHeight(),
                width: newWidthTable,
            });
        }
        setTimeout(function () {
            $('.content-wrapper').css('overflow', 'auto');
        }, 2000);
    }, 300);
}