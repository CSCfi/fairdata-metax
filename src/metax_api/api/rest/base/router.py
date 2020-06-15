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
from rest_framework.routers import DefaultRouter, Route

from .views import (
    ContractViewSet,
    DataCatalogViewSet,
    DatasetViewSet,
    DirectoryViewSet,
    ApiErrorViewSet,
    FileViewSet,
    FileStorageViewSet,
    SchemaViewSet
)


class CustomRouter(DefaultRouter):

    def __init__(self, *args, **kwargs):
        """
        Override to allow PUT and PATCH methods in resource list url.
        """
        self.routes.pop(0)
        self.routes.insert(0, Route(
            url=r'^{prefix}{trailing_slash}$',
            mapping={
                'get': 'list',                  # original
                'post': 'create',               # original
                'put': 'update_bulk',           # custom
                'patch': 'partial_update_bulk', # custom
                'delete': 'destroy_bulk'        # custom
            },
            name='{basename}-list',
            detail=False,
            initkwargs={'suffix': 'List'}
        ))
        super(CustomRouter, self).__init__(*args, **kwargs)

    def get_default_basename(self, viewset):
        """
        When a viewset has no queryset set, or base_name is not passed to a router as the
        3rd parameter, automatically determine base name.
        """
        return viewset.__class__.__name__.split('View')[0]


router = CustomRouter(trailing_slash=False)
router.register(r'apierrors/?', ApiErrorViewSet)
router.register(r'contracts/?', ContractViewSet)
router.register(r'datasets/?', DatasetViewSet)
router.register(r'datacatalogs/?', DataCatalogViewSet)
router.register(r'directories/?', DirectoryViewSet)
router.register(r'files/?', FileViewSet)
router.register(r'filestorages/?', FileStorageViewSet)
router.register(r'schemas/?', SchemaViewSet)

# note: this somehow maps to list-api... but the end result works when
# the presence of the parameters is inspected in the list-api method.
router.register(
    r'datasets/(?P<identifier>.+)/metadata_versions/(?P<metadata_version_identifier>.+)/?',
    DatasetViewSet
)

api_urlpatterns = router.urls
