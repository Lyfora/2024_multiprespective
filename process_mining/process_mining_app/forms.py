# process_mining_app/forms.py
from django import forms


class UploadFileForm(forms.Form):
    file = forms.FileField(label='Select a CSV file')
