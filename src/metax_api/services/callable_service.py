# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging


_logger = logging.getLogger(__name__)


class CallableService():

    """
    Methods to handle adding and executing callable objects, which will be executed
    as last thing before the transaction is committed to the db. The callables
    will be executed in the order they are appended.

    If executing the callable raises an exception, the transaction is not committed.

    Why not use django signals instead? Signal receivers get executed immediately,
    whether the end-result of the request is a success or not.

    Note: To execute callables only AFTER a successful commit, use django's built-in method
    django.db.transaction.on_commit(callable).
    """

    post_request_callables = []

    @classmethod
    def add_post_request_callable(cls, callable):
        if not cls.post_request_callables:
            cls.post_request_callables = []
        cls.post_request_callables.append(callable)

    @classmethod
    def run_post_request_callables(cls):
        """
        Execute all callable objects inserted into cls.post_request_callables during the ongoing request.
        """
        if not cls.post_request_callables:
            return

        _logger.debug('Executing %d post_request_callables...' % len(cls.post_request_callables))

        for callable_obj in cls.post_request_callables:
            try:
                callable_obj()
            except:
                _logger.exception('Failed to execute post_request_callables')
                # failure to execute a callable should fail the entire request
                cls.clear_callables()
                raise

        cls.clear_callables()

    @classmethod
    def clear_callables(cls):
        """
        If the callable objects are never cleared, and some request some time for whatever reason
        fails to execute them, they would get executed during the next request made by the same
        process, since this static class is persistent during the lifetime of the process.

        Why not use this as a feature to retry, when encountering errors?
        1) Processes get restarted by gunicorn every x requests, so it would get cleared at
            some point anyway, never to be retried again.
        2) The retry would be quite random: A chance only when there is a request, and it happens
            to hit the same process.
        """
        if cls.post_request_callables:
            cls.post_request_callables = []
