import csv
import io
import json

from django import forms
from django.contrib.auth.forms import UserCreationForm, AuthenticationForm
from django.core.files.images import get_image_dimensions
from django.core.files.uploadedfile import SimpleUploadedFile
from django.core.validators import FileExtensionValidator

class LoginForm(AuthenticationForm):
    def clean_username(self):
        username = self.cleaned_data.get("username")

        if len(username) == 0:
            raise forms.ValidationError("Username is required")

        return username

    def clean_password(self):
        password = self.cleaned_data.get("password")

        if len(password) == 0:
            raise forms.ValidationError("Password is required")

        return password


class RegistrationForm(UserCreationForm):
    email = forms.EmailField(max_length=200, help_text='Email', required=False)

    def clean_username(self):
        username = self.cleaned_data.get("username")

        if len(username) == 0:
            raise forms.ValidationError("Username is required")

        return username

    def clean_email(self):
        email = self.cleaned_data.get("email")

        if len(email) == 0:
            raise forms.ValidationError("This field is required.")

        return email

    def clean_password1(self):
        password1 = self.cleaned_data.get("password1")

        if len(password1) == 0:
            raise forms.ValidationError("Password is required")

        return password1
    
class ImageUploadForm(forms.Form):
    avatar = forms.ImageField(
        widget=forms.ClearableFileInput(attrs={'onchange': 'loadFile(event);'}),
        required=False
    )

    def clean_avatar(self):
        avatar = self.cleaned_data.get('avatar')

        if avatar:
            try:
                w, h = get_image_dimensions(avatar)

                max_width = max_height = 128
                if w > max_width or h > max_height:
                    raise forms.ValidationError(
                        f'Please use an image that is {max_width} x {max_width} pixels or smaller')

                # validate content type
                main, sub = avatar.content_type.split('/')
                if not (main == 'image' and sub in ['jpg', 'jpeg', 'png']):
                    raise forms.ValidationError(f'Please use a JPEG or PNG image')

                # validate file size
                if len(avatar) > (100 * 1024):
                    raise forms.ValidationError('Avatar file size may not exceed 100k.')

            except AttributeError:
                pass

        return avatar
