from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework import generics

from api.process.utils.mongo.repository import Repository
from api.serializers import JobSerializer
from api.models import Job
from app.models import Table
import api.tasks as tasks

from celery import group, chord


class JobView2(APIView):
    def get(self, request):
        jobs = Job.objects.all()
        return Response([
            self._serialize_job(job)
            for job in jobs
        ])

    def post(self, request):
        data = request.data

        """
        serializer = JobSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        """
        return Response({
            "data": request.data,
        })

    def _serialize_job(self, job):
        return {
            "id": job.id,
            "created": str(job.created),
            "tables": job.tables,
            "callback": job.callback
        }

    """
    def post(self, request):
        pass
        serializer = SnippetSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    """

class JobView(generics.ListCreateAPIView):
    queryset = Job.objects.all()
    serializer_class = JobSerializer
    
    def perform_create(self, serializer):
        serializer.data
        job = Job(
            tables=serializer.data["tables"],
            callback=serializer.data["callback"],
            progress={
                "current": 1,
                "total": 5
            }
        )
        job.save()

        job.task_id = tasks.job_slot.apply_async(
            args=(job.id,),
            link=[tasks.rest_hook.s()]
        )
        job.save()
        """
        job = serializer.save()
        job.progress = {
            "current": 1,
            "total": 5
        }
        job.save()

        job.task_id = tasks.job_slot.apply_async(
            args=(job.id,),
            link=[tasks.rest_hook.s()]
        )
        job.save()
        """