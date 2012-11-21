from scrapy.command import ScrapyCommand
from scrapy.utils.conf import arglist_to_dict
from scrapy.exceptions import UsageError

class Command(ScrapyCommand):

    requires_project = True

    def syntax(self):
        return "[options] <spider> [start url] ..."

    def short_desc(self):
        return "Start crawling from a spider or URL"

    def add_options(self, parser):
        ScrapyCommand.add_options(self, parser)
        parser.add_option("-a", dest="spargs", action="append", default=[], metavar="NAME=VALUE", \
            help="set spider argument (may be repeated)")
        parser.add_option("-c", dest="callback", help="use this callback in the start urls")
        parser.add_option("-o", "--output", metavar="FILE", \
            help="dump scraped items into FILE (use - for stdout)")
        parser.add_option("-t", "--output-format", metavar="FORMAT", default="jsonlines", \
            help="format to use for dumping items with -o (default: %default)")

    def process_options(self, args, opts):
        ScrapyCommand.process_options(self, args, opts)
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
        if len(args) < 1:
            raise UsageError()
        spname = args[0]
        urls = args[1:]
        spider = self.crawler.spiders.create(spname, **opts.spargs)
        requests = self._requests(spider, urls, opts.callback) if urls else None
        self.crawler.crawl(spider, requests)
        self.crawler.start()

    def _resolve_callback(self, spider, cbname):
        try:
            callback = getattr(spider, cbname)
            if not callable(callback):
                raise UsageError("Callback '{0}' is not a method.",
                                 print_help=False)
            return callback
        except AttributeError:
            raise UsageError("Callback '{0}' not found in the"
                             " spider '{1}'".format(cbname, spider.name),
                             print_help=False)

    def _requests(self, spider, urls, callback):
        if callback:
            callback = self._resolve_callback(spider, callback)
        for url in urls:
            req = spider.make_requests_from_url(url)
            req.callback = callback
            yield req
