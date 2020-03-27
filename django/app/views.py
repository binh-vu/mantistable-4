from django.shortcuts import render
from django.views import View
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator

import json

# Create your views here.
def index(request):
    context = {}
    return render(request, 'app/index.html', context)


def home(request):
    context = {}
    return render(request, 'app/home.html', context)


class JobHandler(View):
    @method_decorator(csrf_exempt)
    def dispatch(self, request, *args, **kwargs):
        return super(JobHandler, self).dispatch(request, *args, **kwargs)

    def post(self, request):
        data = json.loads(request.POST.get("progress"))
        print(data["current"], data["total"])

        return JsonResponse({"status": "ok"})