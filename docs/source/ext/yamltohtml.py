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
        return [nodes.raw('', html, format="html")]

def setup(app):
    app.add_directive("yamltohtml", DaskConfigDirective)

    return {
        'version': '0.1',
        'parallel_read_safe': True,
        'parallel_write_safe': True,
    }


def process_thing(key, value, schema, prefix=""):
    if isinstance(value, dict):
        return sum(
            [
                process_thing(
                    k,
                    v,
                    schema.get("properties", {}).get(k, {"properties": {}}),
                    prefix = prefix + key + ".",
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
                    </dl>
                    <p>{description}</p>
                """
        return [node]

def generate_html(config, schema, location):
    nested_html = process_thing(key="", value=config, schema=schema, prefix=location)
    return ''.join(nested_html)

# <div class="section" id="gateway-server">
# <h2>Gateway Server<a class="headerlink" href="#gateway-server" title="Permalink to this headline">¶</a></h2>
# <dl class="py data">
# <dt id="c.DaskGateway.address">
# <code class="sig-prename descclassname">c.DaskGateway.</code><code class="sig-name descname">address</code><em class="property"> = Unicode('')</em><a class="headerlink" href="#c.DaskGateway.address" title="Permalink to this definition">¶</a></dt>
# <dd><p>The address the private api server should <em>listen</em> at.</p>
# <p>Should be of the form <code class="docutils literal notranslate"><span class="pre">{hostname}:{port}</span></code></p>
# <p>Where:</p>
# <ul class="simple">
# <li><p><code class="docutils literal notranslate"><span class="pre">hostname</span></code> sets the hostname to <em>listen</em> at. Set to <code class="docutils literal notranslate"><span class="pre">&quot;&quot;</span></code> or
# <code class="docutils literal notranslate"><span class="pre">&quot;0.0.0.0&quot;</span></code> to listen on all interfaces.</p></li>
# <li><p><code class="docutils literal notranslate"><span class="pre">port</span></code> sets the port to <em>listen</em> at.</p></li>
# </ul>
# <p>Defaults to <code class="docutils literal notranslate"><span class="pre">127.0.0.1:0</span></code>.</p>
# </dd></dl>
