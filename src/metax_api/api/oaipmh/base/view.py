from django.conf import settings
from django.http import HttpResponse
from lxml import etree
from lxml.etree import SubElement
import oaipmh.metadata as oaimd
import oaipmh.server as oaiserver

from .metax_oai_server import MetaxOAIServer

NS_XSI = 'http://www.w3.org/2001/XMLSchema-instance'
NS_OAIDC = 'http://schema.datacite.org/oai/oai-1.0/'

def oai_datacite_writer(element, metadata):
    e_dc = SubElement(element, 'oai_datacite', nsmap={None: NS_OAIDC})
    e_dc.set('{%s}schemaLocation' % NS_XSI,
             'http://schema.datacite.org/oai/oai-1.0/ oai_datacite.xsd')

    e = SubElement(e_dc, 'schemaVersion')
    e.text = metadata['schemaVersion'][0]
    e = SubElement(e_dc, 'datacentreSymbol')
    e.text = metadata['datacentreSymbol'][0]
    e = SubElement(e_dc, 'payload')
    e.append(etree.fromstring(metadata['payload'][0]))

def oaipmh_view(request):
    metax_server = MetaxOAIServer()
    metadata_registry = oaimd.MetadataRegistry()
    metadata_registry.registerWriter('oai_dc', oaiserver.oai_dc_writer)
    metadata_registry.registerWriter('oai_dc_urnresolver', oaiserver.oai_dc_writer)
    metadata_registry.registerWriter('oai_datacite', oai_datacite_writer)

    server = oaiserver.BatchingServer(metax_server,
                                 metadata_registry=metadata_registry,
                                 resumption_batch_size=settings.OAI['BATCH_SIZE'])
    xml = server.handleRequest(request.GET.dict())

    return HttpResponse(xml, content_type='text/xml; charset=utf-8')
