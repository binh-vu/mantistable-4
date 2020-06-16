from django.shortcuts import render, redirect
from django.views import View
from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.views.generic.edit import FormView
from django.urls import reverse, reverse_lazy, get_script_prefix
from django.contrib import messages

from api.models import Job
from web.models import Table, Dataset
from dashboard.imports.dataset import DatasetImport
from dashboard.forms import ImportForm, QueryServiceForm

from celery import current_app
from http import HTTPStatus
import json
import requests


def _build_url(request, view_name):
    return "http://web:8000" + reverse(view_name)


def index(request):
    context = {}
    return render(request, 'dashboard/index.html', context)


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


class JobView(View):
    def get(self, request):
        jobs = Job.objects.all()

        return JsonResponse([
            self._serialize_job(job)
            for job in jobs
        ], safe=False)

    def _serialize_job(self, job):
        return {
            "id": job.id,
            "created": job.created,
            "tables": len(job.tables),
            "progress": job.progress,
            "callback": job.callback
        }


class DatasetView(View):
    def get(self, request):
        offset_filter = int(request.GET.get("offset", "0"))
        limit_filter = int(request.GET.get("limit", "0"))
        sort_filter = request.GET.get("sort", "name")
        order_filter = {
            "asc": "",
            "desc": "-"
        }.get(request.GET.get("order", "asc"))

        datasets = Dataset.objects.all()
        datasets_count = datasets.count()
        datasets = datasets.order_by(order_filter + sort_filter)

        if limit_filter > 0:
            datasets = datasets[offset_filter:offset_filter+limit_filter]
        else:
            datasets = datasets[offset_filter:]

        return JsonResponse({
            "total": datasets_count,
            "rows": [
                self._serialize_datasets(dataset)
                for dataset in datasets
            ]
        }, safe=False)

    def _serialize_datasets(self, dataset):
        return {
            "name": dataset.name,
            "average_rows": dataset.average_rows,
            "average_cols": dataset.average_cols,
            "table_count": dataset.table_count,
            "has_annotations": dataset.has_annotations
        }

class CeleryLoadView(View):
    def get(self, request):
        worker_name = "celery@main"

        active = 0
        reserved = 0
        inspector = current_app.control.inspect([worker_name])
        if inspector.active() is not None:
            active = len(inspector.active()[worker_name])
            reserved = len(inspector.reserved()[worker_name])

        return JsonResponse({
            "active": active,
            "reserved": reserved
        })


class ServiceView(FormView):
    template_name = 'dashboard/service.html'
    form_class = QueryServiceForm
    success_url = reverse_lazy('service')

    def form_valid(self, form):
        query = form.cleaned_data.get('json')

        callback_url = _build_url(self.request, "search-result")
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
            "callback": callback_url
        }

        api_url = _build_url(self.request, "api_job")
        response = requests.post(api_url, json=data)

        return super().form_valid(form)


class MainResultView(View):
    @method_decorator(csrf_exempt)
    def dispatch(self, request, *args, **kwargs):
        return super(MainResultView, self).dispatch(request, *args, **kwargs)

    # TODO: In general this is a bit dangerous if someone other than api post on this
    def post(self, request):
        data = json.loads(request.body)

        job_id = data.get("job_id", -1)
        table_id = data.get("table_id", -1)
        header = data.get("header", "invalid")
        payload = data.get("payload", "invalid")

        if job_id < 0:
            return JsonResponse({"status": "bad format"}, safe=False)
        
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        print(job_id, table_id, header)

        try:
            table = Table.objects.get(id=table_id)
        except Table.DoesNotExist:
            table = None

        # TODO: Implement better websocket wrapper
        requests.post("http://mantistable4_tornado:5000", json={
            "channel": "main",
            "payload": {
                "command": "UPDATE",    # TODO: Making this up just for test
                "resource": "progress",
                "payload": {}
            }
        })

        if header == "column analysis":
            # TODO: Parse format
            if table is not None:
                table.cols = payload
                table.save()            
        elif header == "computation":
            cols = []
            for row in payload:
                subject = row[0]
                linkages = []
                for link in row[1]:
                    if link[0] is not None:
                        l = (link[0][1], link[0][2], round(link[1], 2))
                        linkages.append({
                            "subject": subject,
                            "predicate": l[0],
                            "object": l[1],
                            "confidence": l[2]
                        })
                    else:
                        linkages.append({
                            "subject": subject,
                            "predicate": None,
                            "object": None,
                            "confidence": 0.0
                        })
                cols.append(linkages)

            print(cols)
            if table is not None:
                table.linkages = cols
                table.has_annotations = True
                table.save()

                # TODO: Implement better websocket wrapper
                requests.post("http://mantistable4_tornado:5000", json={
                    "channel": "main",
                    "payload": {
                        "command": "UPDATE",    # TODO: Making this up just for test
                        "resource": "datasets",
                        "payload": {}
                    }
                })
        else:
            print(payload)
        print("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")

        return JsonResponse({"status": "ack"}, safe=False)

class SearchResultView(View):
    @method_decorator(csrf_exempt)
    def dispatch(self, request, *args, **kwargs):
        return super(SearchResultView, self).dispatch(request, *args, **kwargs)

    # TODO: In general this is a bit dangerous if someone other than api post on this
    def post(self, request):
        data = json.loads(request.body)

        job_id = data.get("job_id", -1)
        header = data.get("header", "invalid")
        payload = data.get("payload", "invalid")

        if job_id < 0:
            return JsonResponse({"status": "bad format"}, safe=False)

        if header == "computation":
            cols = []
            for row in payload:
                subject = row[0]
                linkages = []
                for link in row[1]:
                    if link[0] is not None:
                        l = (link[0][1], link[0][2], round(link[1], 2))
                        linkages.append({
                            "subject": subject,
                            "predicate": l[0],
                            "object": l[1],
                            "confidence": l[2]
                        })
                    else:
                        linkages.append({
                            "subject": subject,
                            "predicate": None,
                            "object": None,
                            "confidence": 0.0
                        })
                cols = linkages

            # TODO: Implement better websocket wrapper
            requests.post("http://mantistable4_tornado:5000", json={
                "channel": "service",
                "payload": {
                    "command": "UPDATE",    # TODO: Making this up just for test
                    "resource": "results",
                    "payload": cols
                }
            })

        return JsonResponse({"status": "ack"}, safe=False)
