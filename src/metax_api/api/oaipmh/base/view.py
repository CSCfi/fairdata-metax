from django.conf import settings
from django.http import HttpResponse
from lxml import etree
from lxml.etree import SubElement
import oaipmh.metadata as oaimd
import oaipmh.server as oaiserver

from .metax_oai_server import MetaxOAIServer


NS_OAIDC_DATACITE = 'http://schema.datacite.org/oai/oai-1.0/'

NS_OAIPMH = 'http://www.openarchives.org/OAI/2.0/'
NS_XSI = 'http://www.w3.org/2001/XMLSchema-instance'
NS_OAIDC = 'http://www.openarchives.org/OAI/2.0/oai_dc/'
NS_DC = "http://purl.org/dc/elements/1.1/"
NS_XML = "http://www.w3.org/XML/1998/namespace"


def nsoai(name):
    return '{%s}%s' % (NS_OAIPMH, name)


def nsoaidc(name):
    return '{%s}%s' % (NS_OAIDC, name)


def nsdc(name):
    return '{%s}%s' % (NS_DC, name)


def oai_dc_writer_with_lang(element, metadata):
    e_dc = SubElement(element, nsoaidc('dc'),
                      nsmap={'oai_dc': NS_OAIDC, 'dc': NS_DC, 'xsi': NS_XSI, 'xml': NS_XML})
    e_dc.set('{%s}schemaLocation' % NS_XSI,
             '%s http://www.openarchives.org/OAI/2.0/oai_dc.xsd' % NS_DC)
    map = metadata.getMap()
    for name in [
        'title', 'creator', 'subject', 'description', 'publisher',
        'contributor', 'date', 'type', 'format', 'identifier',
        'source', 'language', 'relation', 'coverage', 'rights'
    ]:

        for value in map.get(name, []):

            e = SubElement(e_dc, nsdc(name))
            if 'lang' in value:
                e.attrib['{http://www.w3.org/XML/1998/namespace}lang'] = value['lang']
            e.text = value['value']


def oai_datacite_writer(element, metadata):
    e_dc = SubElement(element, 'oai_datacite', nsmap={None: NS_OAIDC_DATACITE})
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
    metadata_registry.registerWriter('oai_dc', oai_dc_writer_with_lang)
    metadata_registry.registerWriter('oai_dc_urnresolver', oaiserver.oai_dc_writer)
    metadata_registry.registerWriter('oai_datacite', oai_datacite_writer)

    server = oaiserver.BatchingServer(metax_server,
                                      metadata_registry=metadata_registry,
                                      resumption_batch_size=settings.OAI['BATCH_SIZE'])
    xml = server.handleRequest(request.GET.dict())

    return HttpResponse(xml, content_type='text/xml; charset=utf-8')
