from __future__ import print_function, division, absolute_import

from bokeh.io import curdoc

from distributed.bokeh import messages
from distributed.bokeh.components import WorkerTable

component = WorkerTable(sizing_mode='stretch_both')

doc = curdoc()
doc.add_periodic_callback(lambda: component.update(messages),
                          messages['workers']['interval'])
doc.add_root(component.root)
