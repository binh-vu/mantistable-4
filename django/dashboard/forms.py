from django import forms
from django.core.validators import FileExtensionValidator
from django.contrib.auth.forms import AuthenticationForm
from django.contrib.auth import (
    authenticate,
)
from django.core.exceptions import ValidationError

from dashboard.models import Table
import json
import unicodedata
from django.contrib.auth.models import User
from django.utils.text import capfirst

class ImportForm(forms.Form):
    name = forms.CharField(max_length=255)
    dataset = forms.FileField(
        widget=forms.FileInput(attrs={'accept': '.json, .zip, .csv'}),
        validators=[FileExtensionValidator(allowed_extensions=['json', 'zip','csv'])],
        label="Dataset",
        required=True,
        label_suffix=""
    )


class QueryServiceForm(forms.Form):
    query = forms.CharField(
        widget=forms.Textarea(
            attrs={
                "rows":5,
                "cols":20
            }
        ),
        initial='["batman", "nolan", "pfister"]'
    )

    def clean_query(self):
        jdata = self.cleaned_data['query']
        try:
            jdata = json.loads(jdata)
        except:
            raise forms.ValidationError("Invalid json")

        return jdata


#class LoginForm(AuthenticationForm):

class UsernameField(forms.CharField):
    def to_python(self, value):
        return unicodedata.normalize('NFKC', super().to_python(value))

    def widget_attrs(self, widget):
        return {
            **super().widget_attrs(widget),
            'autocapitalize': 'none',
            'autocomplete': 'username',
        }


class LoginForm(forms.Form):
    username = UsernameField(widget=forms.TextInput(attrs={'autofocus': True, 'class': 'form-control', 'placeholder': 'Username'}))
    password = forms.CharField(
        label="Password",
        strip=False,
        widget=forms.PasswordInput(attrs={'autocomplete': 'current-password', 'class': 'form-control', 'placeholder': 'Password'}),
    )

    error_messages = {
        'invalid_login': "Please enter a correct %(username)s and password. Note that both fields may be case-sensitive.",
        'inactive': "This account is inactive.",
    }

    def __init__(self, request=None, *args, **kwargs):
        self.request = request
        self.user_cache = None
        super().__init__(*args, **kwargs)

        # Set the max length and label for the "username" field.
        self.username_field = User._meta.get_field(User.USERNAME_FIELD)
        username_max_length = self.username_field.max_length or 254
        self.fields['username'].max_length = username_max_length
        self.fields['username'].widget.attrs['maxlength'] = username_max_length
        if self.fields['username'].label is None:
            self.fields['username'].label = capfirst(self.username_field.verbose_name)

    def clean(self):
        username = self.cleaned_data.get('username')
        password = self.cleaned_data.get('password')

        if username is not None and password:
            self.user_cache = authenticate(self.request, username=username, password=password)
            if self.user_cache is None:
                raise self.get_invalid_login_error()
            else:
                self.confirm_login_allowed(self.user_cache)

        return self.cleaned_data

    def confirm_login_allowed(self, user):
        if not user.is_active:
            raise ValidationError(
                self.error_messages['inactive'],
                code='inactive',
            )

    def get_user(self):
        return self.user_cache

    def get_invalid_login_error(self):
        return ValidationError(
            self.error_messages['invalid_login'],
            code='invalid_login',
            params={'username': self.username_field.verbose_name},
        )


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