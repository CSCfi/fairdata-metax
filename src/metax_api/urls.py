# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

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
import debug_toolbar
from django.conf import settings as django_settings
from django.conf.urls import include, url
from django.urls import path, re_path

from metax_api.api.oaipmh.base.view import oaipmh_view as oaipmh
from metax_api.api.rest.base.router import api_urlpatterns as rest_api_v1
from metax_api.api.rest.v2.router import api_urlpatterns as rest_api_v2
from metax_api.api.rpc.base.router import api_urlpatterns as rpc_api_v1
from metax_api.api.rpc.v2.router import api_urlpatterns as rpc_api_v2
from metax_api.views.router import view_urlpatterns

v1_urls = [
    url("", include(view_urlpatterns)),
    url(r"^oai/", oaipmh, name="oai"),
    url(r"^rest/", include(rest_api_v1)),
    url(r"^rest/v1/", include(rest_api_v1)),
    url(r"^rpc/", include(rpc_api_v1)),
    url(r"^rpc/v1/", include(rpc_api_v1)),
]

v2_urls = [
    url(r"^rest/v2/", include(rest_api_v2)),
    url(r"^rpc/v2/", include(rpc_api_v2)),
]

urlpatterns = []

if "v1" in django_settings.API_VERSIONS_ENABLED:
    urlpatterns += v1_urls

if "v2" in django_settings.API_VERSIONS_ENABLED:
    urlpatterns += v2_urls

if django_settings.WATCHMAN_CONFIGURED:
    urlpatterns += [re_path(r"^watchman/", include("watchman.urls"))]

if django_settings.DEBUG:
    urlpatterns += [
        path("__debug__/", include(debug_toolbar.urls)),
    ]
