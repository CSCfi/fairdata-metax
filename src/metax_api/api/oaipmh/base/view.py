from django.conf import settings
from django.http import HttpResponse
import oaipmh.metadata as oaimd
import oaipmh.server as oaiserver

from .metax_oai_server import MetaxOAIServer

def oaipmh_view(request):
    metax_server = MetaxOAIServer()
    metadata_registry = oaimd.MetadataRegistry()
    metadata_registry.registerReader('oai_dc', oaimd.oai_dc_reader)
    metadata_registry.registerWriter('oai_dc', oaiserver.oai_dc_writer)
    server = oaiserver.BatchingServer(metax_server,
                                 metadata_registry=metadata_registry,
                                 resumption_batch_size=settings.OAI['BATCH_SIZE'])
    xml = server.handleRequest(request.GET.dict())

    return HttpResponse(xml, content_type='text/xml; charset=utf-8')
