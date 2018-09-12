# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging

from django.conf import settings as django_settings
from requests import post, put, delete, exceptions

from .utils import executing_test_case, executing_travis

_logger = logging.getLogger(__name__)


def DataciteAPI(*args, **kwargs):
    """
    A factory for the Datacite API client.

    Returns dummy Datacite API with methods that do nothing when executing inside travis or testing
    """
    if executing_travis() or executing_test_case() or kwargs.pop('dummy', False):
        return _DataciteAPIDummy(*args, **kwargs)
    else:
        return _DataciteAPI(*args, **kwargs)


class _DataciteAPI():
    """
    https://support.datacite.org/docs/mds-api-guide

    DOIs can exist in three states: draft, registered, and findable.
    DOIs are in the draft state when metadata have been registered,
    and will transition to the findable state when registering a URL.

    Findable DOIs can be transitioned to the registered state (the metadata are no longer included in the search index)
    """

    def __init__(self, settings=django_settings):
        if not isinstance(settings, dict):
            if hasattr(settings, 'DATACITE'):
                settings = settings.DATACITE
            else:
                raise Exception('Missing configuration from settings.py: DATACITE')

        if not settings.get('USERNAME', None):
            raise Exception('Missing configuration from settings for DATACITE: USERNAME')
        if not settings.get('PASSWORD', None):
            raise Exception('Missing configuration from settings for DATACITE: PASSWORD')
        if not settings.get('METADATA_BASE_URL', None):
            raise Exception('Missing configuration from settings for DATACITE: METADATA_BASE_URL')
        if not settings.get('DOI_BASE_URL', None):
            raise Exception('Missing configuration from settings for DATACITE: DOI_BASE_URL')

        self.api_user = settings['USERNAME']
        self.api_pw = settings['PASSWORD']
        self.metadata_base_url = settings['METADATA_BASE_URL']
        self.doi_base_url = settings['DOI_BASE_URL']

    def register_metadata(self, datacite_xml_metadata):
        """
        Create DOI and its metadata by entering the "draft" state

        :param datacite_xml_metadata:
        :return:
        """
        self._do_post_request(self.metadata_base_url, datacite_xml_metadata, 'application/xml')

    def register_url(self, doi, url):
        """
        Transition from "draft" state to "findable" state, meaning DOI will become resolvable.

        :param doi:
        :param url:
        :return:
        """
        self._do_put_request(self.doi_base_url + doi, 'doi= {0}\nurl= {1}'.format(doi, url), 'text_plain')

    def delete_draft_metadata(self, doi):
        """
        Delete metadata before DOI is in "findable" state.

        :param doi:
        :return:
        """
        self._do_delete_request(self.doi_base_url + doi, 'application/plain')

    def delete_registered_metadata(self, doi):
        """
        Transition from "findable" state to "registered" state by deleting registered metadata.

        :param doi_identifier:
        :return:
        """
        self._do_delete_request(self.metadata_base_url + '/' + doi, 'application/plain')

    def _do_post_request(self, url, data, content_type):
        headers = {'Content-Type': '{0};charset=UTF-8'.format(content_type)}
        self._handle_request_response(post(url, headers=headers, data=data, auth=(self.api_user, self.api_pw)))

    def _do_put_request(self, url, data, content_type):
        headers = {'Content-Type': '{0};charset=UTF-8'.format(content_type)}
        self._handle_request_response(put(url, headers=headers, data=data, auth=(self.api_user, self.api_pw)))

    def _do_delete_request(self, url, content_type):
        headers = {'Content-Type': '{0};charset=UTF-8'.format(content_type)}
        self._handle_request_response(delete(url, headers=headers, auth=(self.api_user, self.api_pw)))

    def _handle_request_response(self, response):
        try:
            response.raise_for_status()
        except exceptions.Timeout:
            _logger.error("Request to Datacite API timed out")
            raise
        except exceptions.ConnectionError:
            _logger.error("Unable to connect to Datacite API")
            raise
        except exceptions.HTTPError as e:
            _logger.error("Datacite API responded with status code: {0}".format(response.status_code))
            _logger.error(e)
            raise
        except Exception as e:
            _logger.error("Unknown error occurred when performing request to Datacite API")
            _logger.error(e)
            raise


class _DataciteAPIDummy():

    """
    A dummy Datacite API client that doesn't connect anywhere and doesn't do jack actually.
    """

    def __init__(self, settings=django_settings):
        pass

    def register_metadata(self, datacite_xml_metadata):
        pass

    def register_url(self, doi, url):
        pass

    def delete_draft_metadata(self, doi):
        pass

    def delete_registered_metadata(self, doi):
        pass
