from django.shortcuts import render
from django.views import View
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.views.generic.edit import FormView
from django.urls import reverse, reverse_lazy

from app.forms import ImportForm
from app.models import Table

import json
import requests

def index(request):
    context = {}
    return render(request, 'app/index.html', context)


class HomeView(FormView):
    template_name = 'app/home.html'
    form_class = ImportForm
    success_url = reverse_lazy('home')

    def form_valid(self, form):
        table_file = form.cleaned_data.get('table_file')
        content = json.loads(table_file.read())
        Table(
            cols=[],
            rows=content
        ).save()

        return super().form_valid(form)

class ProcessAllView(View):
    def get(self, request):
        tables = Table.objects.all()

        data = [
            table.id
            for table in tables
        ]
        print(json.dumps(data))
        response = requests.post(reverse('api.api_job'), data=data)
        status = response.status_code

        return JsonResponse({"status": status})


class JobHandler(View):
    @method_decorator(csrf_exempt)
    def dispatch(self, request, *args, **kwargs):
        return super(JobHandler, self).dispatch(request, *args, **kwargs)

    def post(self, request):
        data = json.loads(request.POST.get("progress"))
        print(data["current"], data["total"])

        return JsonResponse({"status": "ok"})
