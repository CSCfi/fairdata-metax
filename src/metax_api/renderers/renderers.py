from rest_framework import renderers

"""
A fantastic XMLRenderer, because setting content-type to xml and returning a string
just isnt complicated enough in django's opinion.
"""

class XMLRenderer(renderers.BaseRenderer):

    media_type = 'application/xml'
    format = 'xml'

    def render(self, data, media_type=None, renderer_context=None):
        return data
