from __future__ import print_function, division, absolute_import

import os

from bokeh.core.properties import Int, String
from bokeh.driving import cosine
from bokeh.embed import file_html
from bokeh.io import curdoc
from bokeh.models import Tool
from bokeh.plotting import figure
from bokeh.resources import CDN
from bokeh.util.compiler import JavaScript


fn = __file__
fn = os.path.join(os.path.dirname(fn), 'export_tool.js')
with open(fn) as f:
    JS_CODE = f.read()


class ExportTool(Tool):
    __implementation__ = JavaScript(JS_CODE)
    event   = Int(default=0)
    content = String()

    def register_plot(self, plot):
        def export_callback(attr, old, new):
            # really, export the doc as JSON
            self.content = None
            html = file_html(plot, CDN, "Task Stream")
            self.content = html

        self.on_change('event', export_callback)
