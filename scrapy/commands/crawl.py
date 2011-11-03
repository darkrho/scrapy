from w3lib.url import is_url
from scrapy.command import ScrapyCommand
from scrapy.http import Request
from scrapy.utils.conf import arglist_to_dict
from scrapy.exceptions import UsageError

class Command(ScrapyCommand):

    requires_project = True

    def syntax(self):
        return "[options] <spider> [url] ..."

    def short_desc(self):
        return ("Start crawling from a spider. "
                "If url is given it's used as start url for given spider.")

    def add_options(self, parser):
        ScrapyCommand.add_options(self, parser)
        parser.add_option("-a", dest="spargs", action="append", default=[], metavar="NAME=VALUE", \
            help="set spider argument (may be repeated)")
        parser.add_option("-c", "--callback", dest="callback", action="store", default='parse',
            help="spider method to use as callback for given start urls (default: parse)")
        parser.add_option("-o", "--output", metavar="FILE", \
            help="dump scraped items into FILE (use - for stdout)")
        parser.add_option("-t", "--output-format", metavar="FORMAT", default="jsonlines", \
            help="format to use for dumping items with -o (default: %default)")

    def process_options(self, args, opts):
        ScrapyCommand.process_options(self, args, opts)
        if not args:
            raise UsageError()
        for url in args[1:]:
            if not is_url(url):
                raise UsageError("Invalid url: {0!r}".format(url))
        try:
            opts.spargs = arglist_to_dict(opts.spargs)
        except ValueError:
            raise UsageError("Invalid -a value, use -a NAME=VALUE", print_help=False)
        if opts.output:
            if opts.output == '-':
                self.settings.overrides['FEED_URI'] = 'stdout:'
            else:
                self.settings.overrides['FEED_URI'] = opts.output
            self.settings.overrides['FEED_FORMAT'] = opts.output_format

    def run(self, args, opts):
        spider = self.crawler.spiders.create(args[0], **opts.spargs)
        reqs = self._start_requests(spider, args[1:], opts.callback)
        self.crawler.crawl(spider, reqs)
        self.crawler.start()

    def _start_requests(self, spider, urls, callback):
        if urls:
            callback = self._callback(spider, callback)
            return [Request(url, callback=callback, dont_filter=True) for url in urls]

    def _callback(self, spider, name):
        try:
            callback = getattr(spider, name)
        except AttributeError:
            raise UsageError("Callback not found in spider '{0}'".format(spider.name))
        if not callable(callback):
            raise UsageError("Invalid callback: {0!r}".format(callback))
        return callback

