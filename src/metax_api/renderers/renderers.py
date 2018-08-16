# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

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
