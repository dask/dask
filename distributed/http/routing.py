from tornado import web
import tornado.httputil


class RoutingApplication(web.Application):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.applications = []

    def find_handler(self, request: tornado.httputil.HTTPServerRequest, **kwargs):
        handler = super().find_handler(request, **kwargs)
        if handler and not issubclass(handler.handler_class, web.ErrorHandler):
            return handler
        else:
            for app in self.applications:
                handler = app.find_handler(request, **kwargs) or handler
                if handler and not issubclass(handler.handler_class, web.ErrorHandler):
                    break
            return handler

    def add_application(self, application: web.Application):
        self.applications.append(application)
