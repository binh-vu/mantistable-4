$(function () {
    fixAdminLTELayout();

    // if "multi-step" is in sidebar, add scrollbar (slim scroll)
    setTimeout(function () {
        const sideBarMenuHeight = $('.sidebar-menu.tree').height();

        $('.multiStepScrollbar').slimScroll({
            height: `calc(100vh - 100px - ${sideBarMenuHeight}px)`,
            size: '3px',
        });
    }, 500);

    // open/close console
    $('#toggle-console').click(function () {
        toggleConsole();
    });

    // open/close horizontal multistep
    $('.multiStep-horizontal .close-multistep').click(function () {
        toggleMultistep();
    });

    // close sidebar control
    $(".control-sidebar .close").click(function () {
        $("body").removeClass("control-sidebar-open");
    });

});

function fixAdminLTELayout(callback) {
    // fix adminLTE incompatibility between fixed layout and !expandOnHover sidebar
    // remove fixed from HTML
    // set height to content-wrapper
    // sidebar overflow is disabled -> use only if you have a short nav in sidebar

    if ($("body").hasClass('sde-fixed')) {
        // $("body").removeClass('sde-fixed');
        $(".content-wrapper").css("max-height", `calc(100vh - ${$('.main-header').height()}px`);
        $('body, .wrapper').addClass('overflow-hidden');
    }

    if (typeof callback === 'function') {
        callback();
    }
}

function closeRightSidebar(callback) {
    $("body").removeClass("control-sidebar-open");

    if (typeof callback === 'function') {
        callback();
    }
}

function calcPageContentHeight() {
    const bodyHeight = '100vh';
    const headerHeight = $('.main-header').height();
    const footerHeight = $('.sticky-footer').outerHeight() !== undefined ? $('.sticky-footer').outerHeight() : 0;
    const multistepHeight = $('.multiStep-horizontal').outerHeight()
    const contentPadding = $('.content').css('padding').replace("px", "") * 2;
    if (!isNaN(multistepHeight))
        return `calc(${bodyHeight} - ${headerHeight}px - ${footerHeight}px - ${contentPadding}px - ${multistepHeight}px)`;

    return `calc(${bodyHeight} - ${headerHeight}px - ${footerHeight}px - ${contentPadding}px)`;
}

function calcControlSidebarHeight() {
    setTimeout(function () {
        let newControlSidebarheigth;
        let newInfoBoxHeight;
        const bodyHeight = '100vh';
        const headerHeight = $('.main-header').height();
        const footerHeight = $('.sticky-footer').outerHeight();
        const multistepHeight = $('.multiStep-horizontal').outerHeight()
        const contentPadding = $('.content').css('padding').replace("px", "") * 2;

        if ($('body').hasClass('inner-control-sidebar')) {
            if (!isNaN(multistepHeight))
                newControlSidebarheigth = newInfoBoxHeight = `calc(${bodyHeight} - ${headerHeight}px - ${footerHeight}px - ${multistepHeight}px)`;
            else
                newControlSidebarheigth = newInfoBoxHeight = `calc(${bodyHeight} - ${headerHeight}px  - ${footerHeight}px`;
        } else {
            newControlSidebarheigth = `calc(${bodyHeight} - ${footerHeight}px`;
            newInfoBoxHeight = `calc(${bodyHeight} - ${headerHeight}px  - ${footerHeight}px`;
        }


        $(".control-sidebar, #infoBox").css('height', newControlSidebarheigth);
        $("#infoBox").slimScroll({
            height: newInfoBoxHeight
        })
    }, 300);

}

function toggleMultistep(callback) {
    $('.multiStep-horizontal').toggleClass('closed');

    setTimeout(function () {
        calcControlSidebarHeight();
    }, 300);

    if (typeof callback === 'function') {
        callback();
    }
}

function toggleConsole(callback) {
    $('.sticky-footer').toggleClass('close-console');
    $('.sticky-footer').toggleClass('open-console');
    $('.up-down-arrow').toggleClass('fa-caret-up');
    $('.up-down-arrow').toggleClass('fa-caret-down');
    $(".scrollable-area").slimScroll({
        height: '120px', //scrollable-area height-padding
    });
    calcControlSidebarHeight();

    if (typeof callback === 'function') {
        callback();
    }
}