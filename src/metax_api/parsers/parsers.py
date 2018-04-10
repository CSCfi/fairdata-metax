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
