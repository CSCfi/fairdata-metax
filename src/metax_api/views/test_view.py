from django.views.generic import TemplateView

from metax_api.models import CatalogRecord


class TestView(TemplateView):
    template_name = "test_view.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)  # .filter(cast__in=self.request.user)

        cr = CatalogRecord.objects.first()

        context["cr"] = cr
        return context