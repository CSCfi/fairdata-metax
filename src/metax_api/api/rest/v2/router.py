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
from rest_framework.routers import Route

from metax_api.api.rest.base import CustomRouter
from metax_api.api.rest.base.views import (
    ApiErrorViewSet,
    ContractViewSet,
    DataCatalogViewSet,
    DirectoryViewSet,
    FileStorageViewSet,
    FileViewSet,
    SchemaViewSet
)
from .views import (
    DatasetViewSet,
)


class CustomRouterV2(CustomRouter):

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        # routers to map "detail route" HTTP verbs to methods, in order to be able to use same
        # api endpoint with different verbs. alternative method would be to accept all verbs
        # using the @detail_route decorator, and then just handle them both in the same method...
        # but this is more in line with how the rest of the apis are laid out

        # - retrieve all file user metadata
        # - update file user metadata in bulk-manner
        self.routes.append(Route(
            url=r'^{prefix}/{lookup}/files/user_metadata{trailing_slash}$',
            mapping={
                'get': 'files_user_metadata_list',
                'put': 'files_user_metadata_update',
                'patch': 'files_user_metadata_update',
            },
            name='{basename}-files-user-metadata-list',
            detail=True,
            initkwargs={'suffix': 'FilesUserMetadataList'}
        ))

        # - retrieve single dataset file technical metadata
        self.routes.append(Route(
            url=r'^{prefix}/{lookup}/files/(?P<file_pk>.+){trailing_slash}$',
            mapping={
                'get': 'files_retrieve',
            },
            name='{basename}-files-retrieve',
            detail=True,
            initkwargs={'suffix': 'FilesRetrieve'}
        ))

        # - retrieve all dataset files technical metadata
        # - change files of a dataset
        self.routes.append(Route(
            url=r'^{prefix}/{lookup}/files{trailing_slash}$',
            mapping={
                'get': 'files_list',
                'post': 'files_post',
            },
            name='{basename}-files-list',
            detail=True,
            initkwargs={'suffix': 'FilesList'}
        ))


# v2 urls, but using v1 view classes, because nothing changes

router_v1 = CustomRouter(trailing_slash=False)
router_v1.register(r'apierrors/?', ApiErrorViewSet, basename='apierrors')
router_v1.register(r'contracts/?', ContractViewSet, basename='Contract')
router_v1.register(r'datacatalogs/?', DataCatalogViewSet, basename='DataCatalog')
router_v1.register(r'directories/?', DirectoryViewSet, basename='Directory')
router_v1.register(r'files/?', FileViewSet, basename='File')
router_v1.register(r'filestorages/?', FileStorageViewSet, basename='Filestorage')
router_v1.register(r'schemas/?', SchemaViewSet, basename='schemas')

# v2 urls, using v2 view classes with changes

router_v2 = CustomRouterV2(trailing_slash=False)
router_v2.register(r'datasets/?', DatasetViewSet, basename='CatalogRecord')
router_v2.register(
    r'datasets/(?P<identifier>.+)/metadata_versions/(?P<metadata_version_identifier>.+)/?',
    DatasetViewSet,
    basename='CatalogRecord'
)

api_urlpatterns = router_v1.urls + router_v2.urls
