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
from .views import FileViewSet, DatasetViewSet, DatasetCatalogViewSet

router = DefaultRouter(trailing_slash=False)
router.register(r'datasetcatalogs/?', DatasetCatalogViewSet)
router.register(r'datasets/?', DatasetViewSet)
router.register(r'files/?', FileViewSet)

api_urlpatterns = router.urls
