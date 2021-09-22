# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.conf.urls import url
from django.views.generic import TemplateView

from metax_api.views.secure import secure_view

view_urlpatterns = [
    url(r"^logout?", secure_view.SecureLogoutView.as_view()),
    url(r"^secure/login?", secure_view.SecureLoginView.as_view()),
    url(r"^swagger/v1", TemplateView.as_view(template_name='swagger/v1/swagger.html')),
    url(r"^swagger/v2", TemplateView.as_view(template_name='swagger/v2/swagger.html')),
]
