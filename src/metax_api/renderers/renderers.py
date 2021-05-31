# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from rest_framework import renderers


class HTMLToJSONRenderer(renderers.JSONRenderer):

    """
    Deals with web browser requests to the API when BrowsableAPIRenderer is not enabled
    in settings.py. A web browers generally send Accept header 'text/html', and some others,
    but not JSON.

    This renderer catches the 'text/html' Accept header, but returns JSON instead of html.
    """

    media_type = "text/html"
    charset = "utf-8"


class XMLRenderer(renderers.BaseRenderer):

    """
    A fantastic XMLRenderer, because setting content-type to xml and returning a string
    just isnt complicated enough in django's opinion.
    """

    media_type = "application/xml"
    format = "xml"

    def render(self, data, media_type=None, renderer_context=None):
        return data
