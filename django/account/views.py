from django.shortcuts import render
from django.contrib.auth import authenticate, login, logout
from django.views import View
from django.utils.http import urlsafe_base64_encode
from django.http import JsonResponse

import mantistable.settings
from account.tokengen import account_activation_token
from account.forms import RegistrationForm, ImageUploadForm, LoginForm
import json

class RegisterView(View):
    template_name = 'account/register.html'

    def get(self, request):
        return render(request, self.template_name, {
            "registration_form": RegistrationForm(),
            "image_form": ImageUploadForm(),
            "smtp": json.dumps(self.__has_smtp(mantistable.settings.EMAIL_HOST_USER)),
        })

    def post(self, request):
        registration_form = RegistrationForm(data=request.POST)
        image_form = ImageUploadForm(request.POST, request.FILES)

        if registration_form.is_valid() and image_form.is_valid():
            # Create user profile
            user = registration_form.save(commit=False)
            avatar = image_form.cleaned_data["avatar"]
            self.__create_user_profile(user, avatar)

            # Send email activation
            if self.__has_smtp(mantistable.settings.EMAIL_HOST_USER):
                mail_subject = mantistable.settings.ACCOUNT_SETTINGS["registration"]["mail_subject"]
                to_email = registration_form.cleaned_data.get('email')
                self.__send_email(to_email, mail_subject, user, self.__get_host())
            else:
                user.is_active = True
                user.save()

            redirect = reverse(mantistable.settings.ACCOUNT_SETTINGS["registration"]["redirect"])
            return JsonResponse({
                "redirect": redirect
            })

        # Invalid login
        errors = {}
        if not registration_form.is_valid():
            errors.update(registration_form.errors)

        if not image_form.is_valid():
            errors.update(image_form.errors)

        return JsonResponse(errors, status=400)

    def __create_user_profile(self, user, avatar):
        user.is_active = False
        user.save()

        profile = UserProfile(user=user)
        if avatar is not None:
            profile.avatar = avatar

        profile.save()

    def __has_smtp(self, email_host):
        return email_host != '' and "@" in email_host

    def __send_email(to_email, mail_subject, user, host):
        message = render_to_string('app/email_activation.html', {
            'user': user,
            'domain': host,
            'uid': urlsafe_base64_encode(force_bytes(user.pk)),
            'token': account_activation_token.make_token(user),
        })
        email = EmailMessage(mail_subject, message, to=[to_email])
        email.send()

    def __get_host():
        if mantistable.settings.PORT != '443' and mantistable.settings.PORT != '80':
            return "%s:%s" % (mantistable.settings.HOST, mantistable.settings.PORT)
        
        return mantistable.settings.HOST


class LoginView(View):
    form_class = LoginForm
    template_name = 'account/login.html'

    def get(self, request):
        form = self.form_class()
        return render(request, self.template_name, {
            'login_form': form
        })

    def post(self, request):
        form = self.form_class(data=request.POST)
        
        if form.is_valid():
            username = form.cleaned_data.get('username')
            password = form.cleaned_data.get('password')
            user = authenticate(username=username, password=password)

            if user is not None:
                login(request, user)
                next_url = request.POST.get('next')

                if next_url:
                    redirect = next_url
                else:
                    redirect = reverse(mantistable.settings.ACCOUNT_SETTINGS["login"]["redirect"])

                return JsonResponse({
                    "redirect": redirect
                })
            else:
                return JsonResponse({}, status=400)
        else:
            return JsonResponse(form.errors, status=400)
        

class LogoutView(View):
    def get(self, request):
        logout(request)
        return redirect(mantistable.settings.ACCOUNT_SETTINGS["logout"]["redirect"])
