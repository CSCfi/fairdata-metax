# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging
import threading

_logger = logging.getLogger(__name__)

# Callableservice is thread local to prevent callables leaking to concurrent requests
class _CallableService(threading.local):

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

    def __init__(self):
        super().__init__()
        self.post_request_callables = []

    def add_post_request_callable(self, callable):
        if not self.post_request_callables:
            self.post_request_callables = []
        self.post_request_callables.append(callable)

    def run_post_request_callables(self):
        """
        Execute all callable objects inserted into self.post_request_callables during the ongoing request.
        """
        if not self.post_request_callables:
            return

        _logger.debug("Executing %d post_request_callables..." % len(self.post_request_callables))

        for callable_obj in self.post_request_callables:
            try:
                callable_obj()
            except:
                _logger.exception("Failed to execute post_request_callables")
                # failure to execute a callable should fail the entire request
                self.clear_callables()
                raise

        self.clear_callables()

    def clear_callables(self):
        """
        If the callable objects are never cleared, and some request some time for whatever reason
        fails to execute them, they would get executed during the next request made by the same
        process, since the instantiated object is persistent during the lifetime of the process.

        Why not use this as a feature to retry, when encountering errors?
        1) Processes get restarted by gunicorn every x requests, so it would get cleared at
            some point anyway, never to be retried again.
        2) The retry would be quite random: A chance only when there is a request, and it happens
            to hit the same process.
        """
        if self.post_request_callables:
            self.post_request_callables = []


CallableService = _CallableService()
