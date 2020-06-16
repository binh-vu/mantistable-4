$(window).on('load resize', function () {
    fixAdminLTELayout();
});

$(function () {
    // if "multi-step" is in sidebar, add scrollbar (slim scroll)
    setTimeout(function () {
        const sideBarMenuHeight = $('.sidebar-menu.tree').height();

        $('.multiStepScrollbar').slimScroll({
            height: `calc(100vh - 100px - ${sideBarMenuHeight}px)`,
            size: '3px',
        });
    }, 500);


    // close sidebar control
    $(".control-sidebar .close").click(function () {
        $("body").removeClass("control-sidebar-open");
    });

    //Material Design Forms
    MDForms();

});

function fixAdminLTELayout(callback) {
    // fix adminLTE incompatibility between fixed layout and !expandOnHover sidebar
    // remove fixed from HTML
    // set height to content-wrapper
    // sidebar overflow is disabled -> use only if you have a short nav in sidebar

    if ($("body").hasClass('sde-fixed')) {
        // $("body").removeClass('sde-fixed');

        const headerHeight = $('.main-header').height();
        $(".content-wrapper").css("max-height", `calc(100vh - ${headerHeight}px`);

        setTimeout(function () {
            $(".content-wrapper").css("min-height", `calc(100vh - ${headerHeight}px`);
        }, 200);

        $('body, .wrapper').addClass('overflow-hidden');
    }

    if (typeof callback === 'function') {
        callback();
    }
}

function calcPageContentHeight() {
    const bodyHeight = '100vh';
    const headerHeight = $('.main-header').height();
    const contentPadding = $('.content').css('padding') && $('.content').css('padding').replace("px", "") * 2;
    const twoSideLayout = $('.twoSide-layout').length ? -contentPadding : 0;
    return `calc(${bodyHeight} - ${headerHeight + contentPadding + twoSideLayout}px)`;

}

function hex2rgb(hex) {
    return ['0x' + hex[1] + hex[2] | 0, '0x' + hex[3] + hex[4] | 0, '0x' + hex[5] + hex[6] | 0];
}

function makeUrl(url) {
    return $("<a />", {href: url, text: url, title: url, target: '_blank'});
}

function MDForms() {

    formOnLoad();

    $(".form-group.material").find("input, textarea, select").on('input', function () {
        $(this).parent().removeClass('error');
        $('.global.form-error-message').empty();

        if ($(this).parent().hasClass('trailing-icon')) {
            if ($(this).val() !== '') {
                $('.clear').show();
            } else {
                $('.clear').hide();
            }
        }
    });

    $(".form-group.material").find("input, textarea, select").focus(function () {
        floatLabel($(this));
        $(this).parent().find("label").addClass('focus');
    });

    $(".form-group.material").find("input, textarea, select").focusout(function () {
        $(this).parent().find("label").removeClass('label-float focus');

        if ($(this).val() !== null && $(this).val() !== "") {
            $(this).parent().find("label").addClass('label-float');
        }
    });

    $(".form-group.material.input-radio input").on('change', function () {
        const currentFormGroup = $(this).closest('.form-group');
        currentFormGroup.removeClass('error');
        const currentInputLabel = $(this).closest('label');
        $(currentFormGroup).find('label').removeClass('selected');

        if (this.checked) {
            $(currentInputLabel).addClass('selected');
        }
    });

    $(".form-group.material.input-checkbox input").on('change', function () {

        $(this).closest('.form-group').removeClass('error');

        const currentInputLabel = $(this).closest('label');

        if (this.checked) {
            $(currentInputLabel).addClass('selected');
        } else {
            $(currentInputLabel).removeClass('selected');
        }
    });

    $('.form-group.material.trailing-icon .clear').on('click', function () {

        $(this).hide().closest('.form-group.material').find('input').val("").trigger("focusout");
    });

    function floatLabel(currentInput) {
        $(currentInput).closest('.form-group.material').find("label").addClass('label-float');
    }

    function formOnLoad() {

        //if input/select/textarea contains a value, set float label
        $.each($(".form-group.material").find('input, select, textarea'), function () {
            if ($(this).val() !== undefined && $(this).val() !== "") {
                floatLabel($(this));
            }

            try {
                setTimeout(function () {
                    $(`input:-webkit-autofill`).each(function () {
                        floatLabel($(this));
                    })
                }, 200);
            } catch {
            }

        });


        // select radio/checkbox
        $.each($('input[type="radio"], input[type="checkbox"]'), function () {
            if (this.checked) {
                const currentInputLabel = $(this).closest('label');
                $(currentInputLabel).addClass('selected');
            }
        });

        // disable input radio/checkbox if parent is disabled
        disableAllRadioInput($(".form-group.material.disabled"));

    }

}

function setErrorMessage(item, message) {

    const formGroup = $(item).closest('.form-group.material');
    const errorMessageDiv = formGroup.find('.form-error-message');

    $(formGroup).addClass('error');
    errorMessageDiv.append(message);

}

function removeErrorMessage(formGroup) {
    // const formGroup = $(item).closest('.form-group.material');
    const errorMessageDiv = formGroup.find('.form-error-message');

    $(formGroup).removeClass('error');
    errorMessageDiv.empty();
}

function resetErrorsAndNotifications(form) {
    const formGroups = $(form).find('.form-group.material');
    removeErrorMessage(formGroups);
    $('.form-error-message.global').hide();
    $('.snackbar').hide();
}

function enableAllRadioInput(inputGroup) {
    const formGroup = $(inputGroup).closest('.form-group.material');
    formGroup.removeClass('disabled');
    formGroup.find('label').removeClass('disabled');
    formGroup.find('input').prop('disabled', false);
}

function disableAllRadioInput(formGroup) {
    if ($(formGroup).hasClass('input-radio') || $(formGroup).hasClass('input-checkbox')) {
        formGroup.find('label').addClass('disabled');
        formGroup.find('input').prop('disabled', true);
    }
}

function manageSnackbar(item) {

    $('.snackbar').show();

    setTimeout(function () {
        $('.snackbar').fadeOut('slow');
    }, 4000);
}

function setSlimScrollGsCol() {

    $(window).on('load resize', function () {

        if ($(document).width() >= 768) {

            $('.gs-col').slimScroll({
                height: calcPageContentHeight() - 10,
                size: '5px',
                color: '#979ca7',
            })

        } else {
            console.log("detected");
            $('.gs-col').slimScroll({destroy: true});
            $('.gs-col').attr('style', '');
        }
    });


}

function setTooltipScore() {

    $('[data-toggle="tooltip"]').each(function () {
        if ($(this).attr('data-scoreName')) {
            $(this).tooltip({
                html: true,
                title: getTooltipScore($(this).attr('data-scoreName')),
                template: '<div class="tooltip scoreDescription" role="tooltip"><div class="tooltip-arrow"></div><div class="tooltip-inner"></div></div>'

            });
        }
    });

    function getTooltipScore(scoreName) {
        let scoreDesc = "";
        switch (scoreName.toLowerCase()) {
            case 'precision':
                scoreDesc = "Precision = (# perfect annotations) / (# submitted annotations)"
                break;
            case 'recall':
                scoreDesc = "Recall = (# perfect annotations) / (# ground truth annotations)"
                break;
            case 'f1':
                scoreDesc = "F1 Score =(2 * Precision * Recall) / (Precision + Recall)"
                break;
            case 'ah':
            case 'ah score':
                scoreDesc = "AH-Score = (1 * (# perfect annotations) + 0.5 * (# okay annotations) - 1 * (# wrong annotations)) / (# target columns)"
                break;
            case 'ap':
            case 'ap score':
                scoreDesc = "AP-Score = (# perfect annotations) / (# total annotated classes)"
                break
        }

        return scoreDesc;
    }
}