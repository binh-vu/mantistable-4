from django.contrib import admin
from django.urls import path, include
from django.views.generic.base import RedirectView

from rest_framework_swagger import renderers
from rest_framework import exceptions
from rest_framework.permissions import AllowAny
from rest_framework.renderers import CoreJSONRenderer
from rest_framework.response import Response
from rest_framework.schemas import SchemaGenerator
from rest_framework.views import APIView

from api import urls as api_urls
from web_api import urls as webapi_urls

def get_swagger_view(title=None, url=None, patterns=None, urlconf=None, description=None):
    """
    Returns schema view which renders Swagger/OpenAPI.
    """
    class SwaggerSchemaView(APIView):
        _ignore_model_permissions = True
        exclude_from_schema = True
        permission_classes = [AllowAny]
        renderer_classes = [
            CoreJSONRenderer,
            renderers.OpenAPIRenderer,
            renderers.SwaggerUIRenderer
        ]

        def get(self, request):
            generator = SchemaGenerator(
                title=title,
                url=url,
                description=description,
                patterns=patterns,
                urlconf=urlconf
            )
            schema = generator.get_schema(request=request)

            if not schema:
                raise exceptions.ValidationError(
                    'The schema generator did not return a schema Document'
                )

            return Response(schema)

    return SwaggerSchemaView.as_view()


mantis_schema_view = get_swagger_view(
    title='MantisTable API',
    url="/api",
    urlconf=api_urls,
    description="MantisTable API allows to identify annotations (Entity Linking, Predicate Annotation, Concept Annotation) by using a non-destructive, incremental approach"
)

frontend_schema_view = get_swagger_view(
    title='Frontend API',
    url="/webapi",
    urlconf=webapi_urls,
    description="MantisTable Frontend API"
)

urlpatterns = [
    path('', RedirectView.as_view(url='dashboard', permanent=False), name='index'),
    path('dashboard/', include('dashboard.urls')),
    
    path('webapi/', include('web_api.urls')),
    path('webapi/', frontend_schema_view),

    path('api/', include('api.urls')),
    path('api/', mantis_schema_view),
    
    path('admin/', admin.site.urls)
]
