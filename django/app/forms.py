from django import forms
from django.core.validators import FileExtensionValidator
from app.models import Table


class ImportForm(forms.Form):
    table_name = forms.CharField(max_length=255)
    table_file = forms.FileField(
        widget=forms.FileInput(attrs={'accept': '.json, .zip'}),
        validators=[FileExtensionValidator(allowed_extensions=['json', 'zip'])],
        label="Table file",
        required=True,
        label_suffix=""
    )