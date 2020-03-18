from django.shortcuts import render
from .forms import RegistrationForm, ImageUploadForm, LoginForm
from django.contrib.auth import authenticate, login, logout

def register(request):
    if request.method == "POST":
        form = RegistrationForm(data=request.POST)
        image_form = ImageUploadForm(request.POST, request.FILES)

        if form.is_valid() and image_form.is_valid():
            user = form.save(commit=False)
            user.is_active = False
            user.save()

            avatar = image_form.cleaned_data["avatar"]

            if avatar is not None:
                UserProfile(
                    user=user,
                    avatar=avatar
                ).save()
            else:
                UserProfile(
                    user=user,
                ).save()


            # Email send
            current_site = get_current_site(request)
            mail_subject = 'Mantistable Account Activation'
            message = render_to_string('account/email_activation.html', {
                'user': user,
                'domain': current_site.domain,
                'uid': urlsafe_base64_encode(force_bytes(user.pk)),
                'token': account_activation_token.make_token(user),
            })
            to_email = form.cleaned_data.get('email')
            email = EmailMessage(
                mail_subject, message, to=[to_email]
            )
            email.send()
            # ----------------

            redirect = reverse("annotations")
            return JsonResponse({
                "redirect": redirect
            })
        else:
            errors = {}
            if not form.is_valid():
                errors.update(form.errors)

            if not image_form.is_valid():
                errors.update(image_form.errors)

            data = errors
            return JsonResponse(data, status=400)

    registration_form = RegistrationForm()
    image_form = ImageUploadForm()
    return render(
        request, 
        "account/register.html", {
            "registration_form": registration_form,
            "image_form": image_form,
        }
    )


def login_view(request):
    if request.method == 'POST':
        form = LoginForm(data=request.POST)
        
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
                    redirect = reverse("index")

                return JsonResponse({
                    "redirect": redirect
                })
            else:
                return JsonResponse({}, status=400)
        else:
            return JsonResponse(form.errors, status=400)

    return render(
        request, 
        "account/login.html", {
            "login_form": LoginForm(),
        }
    )
        
def logout_view(request):
    logout(request)
    return redirect("index")
