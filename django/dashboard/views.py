from django.shortcuts import render, redirect
from django.views import View
from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.views.generic.edit import FormView
from django.urls import reverse, reverse_lazy, get_script_prefix
from django.contrib import messages

from api.models import Job
from dashboard.models import Table, Dataset
from dashboard.imports.dataset import DatasetImport
from dashboard.forms import ImportForm, QueryServiceForm
import mantistable.settings as settings

from celery import current_app
from http import HTTPStatus
import json
import requests


def _build_url(request, view_name):
    return "http://web:8000" + reverse(view_name)


class HomeView(FormView):
    template_name = 'dashboard/home.html'
    form_class = ImportForm
    success_url = reverse_lazy('home')

    def post(self, request, *args, **kwargs):
        if 'form' in request.POST:
            if request.POST.get('form') == 'import':
                form_class = self.get_form_class()
                form_name = 'form'

        form = self.get_form(form_class)

        # validate
        if form.is_valid():
            messages.success(request, 'Added')
            return self.form_valid(form)
        else:
            return self.form_invalid(**{form_name: form})

    def form_valid(self, form):
        if isinstance(form, self.get_form_class()):
            dataset_name = form.cleaned_data.get('name')
            table_file = form.cleaned_data.get('dataset')

            DatasetImport(dataset_name, table_file).load()
            return super().form_valid(form)

    def get_context_data(self, **kwargs):
        context = super(HomeView, self).get_context_data(**kwargs)
        context['loaded_dataset'] = Dataset.objects.all().count()
        if 'form' not in context:
            context['form'] = self.form_class()

        return context


class ProcessView(View):
    def post(self, request):
        ids = request.POST.getlist("ids[]", [])
        backend = request.POST.get("backend", None)

        if backend is None or backend not in settings.LAMAPI_BACKENDS:
            JsonResponse({
                "status": 400,
                "phrase": HTTPStatus(400).phrase,
                "message": {}
            })

        backend = settings.LAMAPI_BACKENDS[backend]

        if len(ids) == 0:
            datasets = Dataset.objects.all()
        else:
            datasets = Dataset.objects.filter(name__in=ids)

        tables = []
        for dataset in datasets:
            for table in dataset.table_set.all():
                tables.append((table.id, table.original))
        
        data = {
            "tables": json.dumps(tables),
            "backend_host": backend["host"],
            "backend_port": backend["port"],
            "backend_token": backend["accessToken"],
            "callback": _build_url(request, 'main-result')
        }

        uri = _build_url(request, 'api_job')
        print("Requesting job to", uri, data)
        response = requests.post(uri, json=data)
        status = response.status_code

        return JsonResponse({
            "status": status,
            "phrase": HTTPStatus(status).phrase,
            "message": response.json()
        })


class ExportView(View):
    def get(self, request):
        ids = request.GET.get("datasets", "")
        ids = ids.split(",")
        if len(ids) != 0:
            datasets = Dataset.objects.filter(name__in=ids)
        else:
            return redirect("home")

        tables = []
        for dataset in datasets:
            for table in dataset.table_set.all():
                tables.append((table.name, table.linkages))

        csv_export = ""
        for name, linkages in tables:
            table_name = name[0:-5]
            prefix = "http://dbpedia.org/resource/"
            for row_idx, row in enumerate(linkages):
                subject = None
                for col_idx, col in enumerate(row):
                    if col["confidence"] > 0.0:
                        content = f"\"{table_name}\",\"{col_idx+1}\",\"{row_idx+1}\",\"{prefix}{col['object']}\""
                        csv_export += content + "\n"
                    subject = col['subject']
                
                if subject is not None:
                    content = f"\"{table_name}\",\"0\",\"{row_idx+1}\",\"{prefix}{subject}\""
                    csv_export += content + "\n"
        
        response = HttpResponse(csv_export, content_type="text/csv")
        response['Content-Disposition'] = f'inline; filename=CEA.csv'
        return response


class ServiceView(FormView):
    template_name = 'dashboard/service.html'
    form_class = QueryServiceForm
    success_url = reverse_lazy('service')

    def form_valid(self, form):
        query = form.cleaned_data.get('json')

        callback_url = _build_url(self.request, "search-result")
        backend = settings.LAMAPI_BACKENDS["dbpedia"]
        data = {
            "tables": [
                (
                    -1, # Null id
                    [
                        {
                            f"col_{idx}": value
                            for idx, value in enumerate(query)
                        }
                    ]
                ),
            ],
            "backend_host": backend["host"],
            "backend_port": backend["port"],
            "backend_token": backend["accessToken"],
            "callback": callback_url
        }

        api_url = _build_url(self.request, "api_job")
        response = requests.post(api_url, json=data)

        return super().form_valid(form)