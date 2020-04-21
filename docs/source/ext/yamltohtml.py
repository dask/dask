import requests
import yaml

from docutils import nodes
from docutils.parsers.rst import Directive


def get_remote_yaml(url):
    r = requests.get(url)
    return yaml.safe_load(r.text)


class DaskConfigDirective(Directive):
    optional_arguments = 3

    def run(self):
        location, config, schema = self.arguments

        config = get_remote_yaml(config)
        schema = get_remote_yaml(schema)

        for k in location.split("."):
            config = config[k]
            schema = schema["properties"].get(k, {})
        html = generate_html(config, schema, location)
        return [nodes.raw("", html, format="html")]


def setup(app):
    app.add_directive("dask-config-block", DaskConfigDirective)

    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }


def process_thing(key, value, schema, prefix=""):
    if isinstance(value, dict):
        return sum(
            [
                process_thing(
                    k,
                    v,
                    schema.get("properties", {}).get(k, {"properties": {}}),
                    prefix=prefix + key + ".",
                )
                for k, v in value.items()
            ],
            [],
        )

    else:

        try:
            description = schema["description"]
            description = description.strip()
        except KeyError:
            description = "No Comment"

        key = prefix + key
        value = str(value)
        node = f"""<dl class="py data">
                     <dt id="{key}">
                       <code class="sig-prename descclassname">{key}</code>
                       <em class="property">&nbsp;&nbsp;{value}</p></em>
                       <a class="headerlink" href="#{key}" title="Permalink to this definition">¶</a>
                     </dt>
                    <dd><p>{description}</p></dd>
                    </dl>

                """
        return [node]


def generate_html(config, schema, location):
    nested_html = process_thing(key="", value=config, schema=schema, prefix=location)
    return "".join(nested_html)
