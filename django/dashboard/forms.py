from django import forms
from django.core.validators import FileExtensionValidator
from dashboard.models import Table

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
