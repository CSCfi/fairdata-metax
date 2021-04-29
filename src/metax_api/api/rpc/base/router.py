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

from rest_framework.routers import DefaultRouter

from .views import DatasetRPC, ElasticsearchRPC, FileRPC, StatisticRPC


class CustomRouter(DefaultRouter):

    def get_default_basename(self, viewset):
        """
        When a viewset has no queryset set, or base_name is not passed to a router as the
        3rd parameter, automatically determine base name.
        """
        return viewset.__class__.__name__.split('RPC')[0]


router = CustomRouter(trailing_slash=False)
router.register(r'datasets/?', DatasetRPC)
router.register(r'files/?', FileRPC)
router.register(r'statistics/?', StatisticRPC)
router.register(r'elasticsearchs/?', ElasticsearchRPC)

api_urlpatterns = router.urls
