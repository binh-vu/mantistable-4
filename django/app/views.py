from django.shortcuts import render
from django.views import View
from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.views.generic.edit import FormView
from django.urls import reverse, reverse_lazy, get_script_prefix

from app.forms import ImportForm, ExportForm
from app.models import Table
from api.models import Job

from http import HTTPStatus
import json
import requests

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
            form_class = self.get_form_class()
            form_name = 'form'
        else:
            form_class = self.export_form_class
            form_name = 'form2'

        form = self.get_form(form_class)

        # validate
        if form.is_valid():
            return self.form_valid(form)
        else:
            return self.form_invalid(**{form_name: form})

    def form_valid(self, form):
        if isinstance(form, self.get_form_class()):
            table_file = form.cleaned_data.get('table_file')
            content = json.loads(table_file.read())
            Table(
                name=table_file.name,
                cols=[],
                rows=content
            ).save()

            return super().form_valid(form)
        else:
            export_type = form.cleaned_data.get('export_type')
            response = HttpResponse(self._export(export_type), content_type="text/csv")
            response['Content-Disposition'] = f'inline; filename={export_type}.csv'
            return response
        

    def get_context_data(self, **kwargs):
        context = super(HomeView, self).get_context_data(**kwargs)
        context['loaded_tables'] = Table.objects.all().count()
        context['current_jobs_count'] = Job.objects.all().count()
        context['jobs_list'] = Job.objects.all()
        context['tables_list'] = Table.objects.all()
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


class ProcessAllView(View):
    def get(self, request):
        tables = Table.objects.all()

        rows = [
            table.id
            for table in tables
        ]
        data = {
            "table_ids": json.dumps(rows),
            "callback": self._build_url(request, 'job-handler')
        }

        uri = self._build_url(request, 'api_job')
        response = requests.post(uri, json=data)
        status = response.status_code

        return JsonResponse({
            "status": status,
            "phrase": HTTPStatus(status).phrase,
            "message": response.json()
        })

    def _build_url(self, request, view_name):
        return request.build_absolute_uri(reverse(view_name)).replace("0.0.0.0", "mantistable4web")


class JobHandler(View):
    @method_decorator(csrf_exempt)
    def dispatch(self, request, *args, **kwargs):
        return super(JobHandler, self).dispatch(request, *args, **kwargs)

    def post(self, request):
        data = json.loads(request.POST.get("progress"))
        print(data)
        print(data["current"], data["total"])

        return JsonResponse({"status": "ok"})
