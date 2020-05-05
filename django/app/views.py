from django.shortcuts import render
from django.views import View
from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.views.generic.edit import FormView
from django.urls import reverse, reverse_lazy, get_script_prefix
from django.contrib import messages

from app.imports.dataset import DatasetImport
from app.forms import ImportForm, ExportForm, QueryServiceForm
from app.models import Table, Dataset
from api.models import Job

from celery import current_app

from http import HTTPStatus
import json
import requests

#NOTE: This is really bad:
#NOTE: 0.0.0.0 is not always in the url...
def _build_url(request, view_name):
        return request.build_absolute_uri(reverse(view_name)).replace("0.0.0.0", "mantistable4web")

def index(request):
    context = {}
    return render(request, 'app/index.html', context)


class HomeView(FormView):
    template_name = 'app/home.html'
    form_class = ImportForm
    export_form_class = ExportForm
    success_url = reverse_lazy('home')

    def post(self, request, *args, **kwargs):
        if 'form' in request.POST:
            if request.POST.get('form') == 'import':
                form_class = self.get_form_class()
                form_name = 'form'
            else:
                form_class = self.export_form_class
                form_name = 'form2'

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
        else:
            export_type = form.cleaned_data.get('export_type')
            response = HttpResponse(self._export(export_type), content_type="text/csv")
            response['Content-Disposition'] = f'inline; filename={export_type}.csv'
            return response

    def get_context_data(self, **kwargs):
        context = super(HomeView, self).get_context_data(**kwargs)
        context['loaded_dataset'] = Dataset.objects.all().count()
        if 'form' not in context:
            context['form'] = self.form_class()
        if 'export_form' not in context:
            context['export_form'] = self.export_form_class()

        return context

    def _export(self, export_type):
        if export_type == "CEA":
            return "cea"
        elif export_type == "CPA":
            return "cpa"
        elif export_type == "CTA":
            return "cta"


class ProcessView(View):
    def post(self, request):
        ids = request.POST.getlist("ids[]", [])

        if len(ids) == 0:
            datasets = Dataset.objects.all()
        else:
            datasets = Dataset.objects.filter(name__in=ids)

        print(datasets)
        table_ids = []
        for dataset in datasets:
            for table in dataset.table_set.all():
                table_ids.append(table.id)

        assert(len(table_ids) == len(set(table_ids)))
        
        data = {
            "table_ids": json.dumps(table_ids),
            "callback": self._build_url(request, 'job-handler')
        }

        uri = _build_url(request, 'api_job')
        response = requests.post(uri, json=data)
        status = response.status_code

        return JsonResponse({
            "status": status,
            "phrase": HTTPStatus(status).phrase,
            "message": response.json()
        })


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
            "tables": len(job.table_ids),
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
        }


class JobHandler(View):
    @method_decorator(csrf_exempt)
    def dispatch(self, request, *args, **kwargs):
        return super(JobHandler, self).dispatch(request, *args, **kwargs)

    def post(self, request):
        data = json.loads(request.POST.get("progress"))
        print(data)
        print(data["current"], data["total"])

        return JsonResponse({"status": "ok"})


class CeleryLoadView(View):
    def get(self, request):
        worker_name = "celery@main"
        inspector = current_app.control.inspect([worker_name])
        active = len(inspector.active()[worker_name])
        reserved = len(inspector.reserved()[worker_name])

        return JsonResponse({
            "active": active,
            "reserved": reserved
        })


class ServiceView(FormView):
    template_name = 'app/service.html'
    form_class = QueryServiceForm
    success_url = reverse_lazy('service')

    def form_valid(self, form):
        query = form.cleaned_data.get('json')

        callback_url = _build_url(self.request, "search-result")
        data = {
            "tables": [
                [
                    {
                        f"col_{idx}": value
                        for idx, value in enumerate(query)
                    }
                ]
            ],
            "callback": callback_url
        }

        api_url = _build_url(self.request, "api_job")
        response = requests.post(api_url, json=data)

        return super().form_valid(form)


class SearchResultView(View):
    @method_decorator(csrf_exempt)
    def dispatch(self, request, *args, **kwargs):
        return super(SearchResultView, self).dispatch(request, *args, **kwargs)

    def post(self, request):
        job_id = request.POST.get("job_id")
        progress = request.POST.get("progress")
        results = request.POST.get("results")

        print()
        print(job_id)
        print(progress)
        print(results)

        return JsonResponse({"status": "Received"}, safe=False)