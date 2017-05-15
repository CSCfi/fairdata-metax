"""metax_api URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.11/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""
from django.conf.urls import url, include
# from django.contrib import admin
from rest_framework.schemas import get_schema_view
from rest_framework_swagger.views import get_swagger_view

from metax_api.api.base.router import api_urlpatterns as api_v1

urlpatterns = [
    url(r'^schema/$', get_schema_view(title='Metax API')),
    url(r'^swagger/$', get_swagger_view(title='Metax API')),

    # root of the api should always use the newest version
    url(r'^rest/', include(api_v1)),
    url(r'^rest/v1/', include(api_v1)),
]

# django default admin site
# urlpatterns.append(url(r'^admin/', admin.site.urls))