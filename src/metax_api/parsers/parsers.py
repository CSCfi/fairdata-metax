# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from rest_framework import parsers

"""
Nearly as astonishing piece of engineering as its cousing, the XMLRenderer, the XMLParser
doesn't do squat to the passed data (such as, say, try to convert it into JSON...), instead
lets it pass through to the views in its original form.
"""

class XMLParser(parsers.BaseParser):

    media_type = 'application/xml'

    def parse(self, stream, media_type=None, parser_context=None):
        return stream.read().decode('utf-8')
