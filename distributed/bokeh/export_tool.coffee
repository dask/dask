p = require "core/properties"
ActionTool = require "models/tools/actions/action_tool"


class ExportToolView extends ActionTool.View

  initialize: (options) ->
    super(options)
    @listenTo(@model, 'change:content', @export)

  do: () ->
    # This is just to trigger an event a python callback can respond to
    @model.event = @model.event + 1

  export: () ->
    if @model.content?
      blob = new Blob([@model.content], {type: "text/plain"})
      url = window.URL.createObjectURL(blob);

      a = document.createElement("a")
      a.id = "bk-export-tool-link"
      a.style = "display: none"
      a.href = url
      a.download = 'task-stream.html'
      document.body.appendChild(a)
      a.click()

      document.getElementById('bk-export-tool-link').remove()
      window.URL.revokeObjectURL(url);

class ExportTool extends ActionTool.Model
  default_view: ExportToolView
  type: "ExportTool"
  tool_name: "Export"
  icon: "bk-tool-icon-save"

  @define {
    event:   [ p.Int,   0 ]
    content: [ p.String   ]
  }

module.exports =
  Model: ExportTool
  View: ExportToolView
