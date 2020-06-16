from django import forms
from django.core.validators import FileExtensionValidator
from web.models import Table

import json


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
    json = forms.CharField(
        widget=forms.Textarea(
            attrs={
                "rows":5,
                "cols":20
            }
        ),
        help_text='Ex. ["batman", "nolan", "pfister"]'
    )

    def clean_json(self):
         jdata = self.cleaned_data['json']
         try:
             jdata = json.loads(jdata)
         except:
             raise forms.ValidationError("Invalid json")

         return jdata