from django import forms
from django.core.validators import FileExtensionValidator
from app.models import Table


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