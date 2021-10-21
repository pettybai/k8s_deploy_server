# -*- coding: utf-8 -*-
import json
import logging
import sys
from typing import Optional, Awaitable

import tornado.web


class BaseHandler(tornado.web.RequestHandler):

    def initialize(self, **kwargs):
        """
        初始化时注入db、redis等
        :param kwargs:
        :return:
        """
        [setattr(self, _key, _obj) for _key, _obj in kwargs.items()]

    def data_received(self, chunk: bytes) -> Optional[Awaitable[None]]:
        pass

    def prepare(self):
        logging.info("request.body: {}".format(self.request.body))
        try:
            setattr(self, 'params', json.loads(self.request.body.decode("utf-8", "ignore"), strict=False))
        except ValueError:
            logging.error("parse params failed")
            setattr(self, 'params', None)

    def _handle_request_exception(self, e):
        logging.exception(e)
        if self._finished:
            return
        self.send_error(500, exc_info=sys.exc_info())
