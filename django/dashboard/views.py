from django.shortcuts import render, redirect
from django.views import View
from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.views.generic.edit import FormView
from django.views.generic.list import ListView
from django.urls import reverse, reverse_lazy, get_script_prefix
from django.contrib import messages
from django.contrib.auth import (
    authenticate, login
)

from api.models import Job
from dashboard.models import Table, Dataset, Log
from dashboard.imports.dataset import DatasetImport
from dashboard.forms import ImportForm, QueryServiceForm, LoginForm
import mantistable.settings as settings

from celery import current_app
from http import HTTPStatus
import json
import requests
from django.contrib.auth.decorators import login_required
from django.utils.decorators import method_decorator


def _build_url(request, view_name):
    return "http://localhost:8000" + reverse(view_name)


@method_decorator(login_required, name='dispatch')
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


@method_decorator(login_required, name='dispatch')
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
                tables.append((table.id, table.name[0:-5], table.original))
        
        data = {
            "tables": tables,
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


@method_decorator(login_required, name='dispatch')
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
                tables.append((table.name, table.linkages, table.cols))

        csv_export = ""
        for name, linkages, cols in tables:
            table_name = name[0:-5]
            prefix = "" #"http://dbpedia.org/resource/"
            for row_idx, row in enumerate(linkages):
                subject = None
                for col_idx, col in enumerate(row):
                    if list(cols.values())[col_idx+1]["tags"]["col_type"] == "NE":
                        if col["confidence"] > 0.0:
                            content = f"\"{table_name}\",\"{row_idx+1}\",\"{col_idx+1}\",\"{prefix}{col['object']}\""
                            csv_export += content + "\n"
                    subject = col['subject']
                
                if subject is not None:
                    content = f"\"{table_name}\",\"{row_idx+1}\",\"0\",\"{prefix}{subject}\""
                    csv_export += content + "\n"
        
        response = HttpResponse(csv_export, content_type="text/csv")
        response['Content-Disposition'] = f'inline; filename=CEA.csv'
        return response


@method_decorator(login_required, name='dispatch')
class ServiceView(FormView):
    template_name = 'dashboard/service.html'
    form_class = QueryServiceForm
    success_url = reverse_lazy('service')

    def form_valid(self, form):
        query = form.cleaned_data.get('query')
        print(query)

        callback_url = _build_url(self.request, "search-result")
        backend = settings.LAMAPI_BACKENDS["dbpedia"]
        data = {
            "tables": [
                (
                    -1, # Null id
                    "query",
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
        print("posting to", api_url, data)
        response = requests.post(api_url, json=data)
        print("ok")

        return super().form_valid(form)


@method_decorator(login_required, name='dispatch')
class DebugLogsView(ListView):
    template_name = "dashboard/debug-logs.html"
    model = Log
    paginate_by = 2000
    queryset = model.objects.order_by('-publish_date')


class LoginView(FormView):
    template_name = "dashboard/login.html"
    form_class = LoginForm
    success_url = reverse_lazy('home')

    def form_valid(self, form):
        print("valid")
        username = form.cleaned_data.get('username')
        password = form.cleaned_data.get('password')
        user = authenticate(username=username, password=password)
        
        if user is not None:
            print("valid user")
            login(self.request, user)
            return super().form_valid(form)
        else:
            print("invalid user")
            return reverse('login')
