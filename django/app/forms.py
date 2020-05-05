from django import forms
from django.core.validators import FileExtensionValidator
from app.models import Table

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

class ExportForm(forms.Form):
    export_type = forms.ChoiceField(
        choices=(
            ("CEA", "CEA"),
            ("CPA", "CPA"),
            ("CTA", "CTA"),
        )
    )

class QueryServiceForm(forms.Form):
    json = forms.CharField(
        widget=forms.Textarea(
            attrs={
                "rows":5,
                "cols":20
            }
        ),
        help_text='Ex. ["batman", "nolan", "2005"]'
    )

    def clean_json(self):
         jdata = self.cleaned_data['json']
         try:
             jdata = json.loads(jdata)
         except:
             raise forms.ValidationError("Invalid json")

         return jdata