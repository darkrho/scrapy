import os
from scrapy.xlib.pydispatch import dispatcher

from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.http import Response
from scrapy.http.cookies import CookieJar, FileCookieJar
from scrapy.utils.project import data_path
from scrapy.conf import settings
from scrapy import log


class CookiesMiddleware(object):
    """This middleware enables working with sites that need cookies"""
    debug = settings.getbool('COOKIES_DEBUG')

    def __init__(self):
        if not settings.getbool('COOKIES_ENABLED'):
            raise NotConfigured
        self.persist = settings.getbool('COOKIES_PERSIST')
        self.cookies_dir = data_path(settings['COOKIES_DIR'])
        if not os.path.exists(self.cookies_dir):
            os.makedirs(self.cookies_dir)
        self.jars = {}
        dispatcher.connect(self.spider_opened, signals.spider_opened)
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def process_request(self, request, spider):
        if 'dont_merge_cookies' in request.meta:
            return

        jar = self.jars[spider]
        cookies = self._get_request_cookies(jar, request)
        for cookie in cookies:
            jar.set_cookie_if_ok(cookie, request)

        # set Cookie header
        request.headers.pop('Cookie', None)
        jar.add_cookie_header(request)
        self._debug_cookie(request, spider)

    def process_response(self, request, response, spider):
        if 'dont_merge_cookies' in request.meta:
            return response

        # extract cookies from Set-Cookie and drop invalid/expired cookies
        jar = self.jars[spider]
        jar.extract_cookies(response, request)
        self._debug_set_cookie(response, spider)

        return response

    def spider_opened(self, spider):
        if self.persist:
            filename = os.path.join(self.cookies_dir, "%s.txt" % spider.name)
            self.jars[spider] = FileCookieJar(filename)
            if os.path.exists(filename):
                self.jars[spider].load()
        else:
            self.jars[spider] = CookieJar()

    def spider_closed(self, spider):
        jar = self.jars.pop(spider)
        if self.persist:
            jar.save()

    def _debug_cookie(self, request, spider):
        if self.debug:
            cl = request.headers.getlist('Cookie')
            if cl:
                msg = "Sending cookies to: %s" % request + os.linesep
                msg += os.linesep.join("Cookie: %s" % c for c in cl)
                log.msg(msg, spider=spider, level=log.DEBUG)

    def _debug_set_cookie(self, response, spider):
        if self.debug:
            cl = response.headers.getlist('Set-Cookie')
            if cl:
                msg = "Received cookies from: %s" % response + os.linesep
                msg += os.linesep.join("Set-Cookie: %s" % c for c in cl)
                log.msg(msg, spider=spider, level=log.DEBUG)

    def _get_request_cookies(self, jar, request):
        headers = {'Set-Cookie': ['%s=%s;' % (k, v) for k, v in request.cookies.iteritems()]}
        response = Response(request.url, headers=headers)
        cookies = jar.make_cookies(response, request)
        return cookies


