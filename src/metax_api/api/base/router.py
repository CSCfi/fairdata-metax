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
from .views import FileViewSet, DatasetViewSet, DataCatalogViewSet, ContractViewSet

class CustomRouter(DefaultRouter):

    """
    Override default router to allow PUT and PATCH methods in resource list url
    """

    def __init__(self, *args, **kwargs):
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
            initkwargs={'suffix': 'List'}
        ))
        super(CustomRouter, self).__init__(*args, **kwargs)

router = CustomRouter(trailing_slash=False)
router.register(r'contracts/?', ContractViewSet)
router.register(r'datasets/?', DatasetViewSet)
router.register(r'datacatalogs/?', DataCatalogViewSet)
router.register(r'files/?', FileViewSet)

api_urlpatterns = router.urls
