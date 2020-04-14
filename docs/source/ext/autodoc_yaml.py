"""autodoc extension for configurable traitlets
Borrowed and heavily modified from jupyterhub/kubespawner:
https://github.com/jupyterhub/kubespawner/blob/master/docs/sphinxext/autodoc_traits.py
"""

import pickle
import types
import codecs
import requests
import yaml
from traitlets import TraitType, Undefined
from sphinx.ext.autodoc import DataDocumenter, ModuleDocumenter


def get_remote_yaml(url):
    r = requests.get(url)
    return yaml.safe_load(r.text)


class ConfigurableDocumenter(ModuleDocumenter):
    """Specialized Documenter subclass for traits with config=True"""

    objtype = "configurable"
    directivetype = "class"

    def __init__(self, *args, **kwargs):
        self.location, self.config, self.schema = args[1].split('\n')

        self.config = get_remote_yaml(self.config)
        self.schema = get_remote_yaml(self.schema)

        for k in self.location.split("."):
            self.config = self.config[k]
            self.schema = self.schema["properties"][k]

        new_args = [args[0], self.location]
        super().__init__(*new_args, **kwargs)

    def get_object_members(self, want_all):
        """Add traits with .tag(config=True) to members list"""
        # breakpoint()

        members = self.process_thing(key="", value=self.config, schema=self.schema, prefix=self.location)
        # for name, trait in sorted(get_traits(config=True).items()):
        #     # put help in __doc__ where autodoc will look for it
        #     trait.__doc__ = trait.help
        #     trait_members.append((name, trait))
        return True, members

    def add_directive_header(self, sig):
        pass

    def add_content(self, more_content):
        pass

    def process_thing(self, key, value, schema, prefix=""):
        if isinstance(value, dict):
            return sum(
                [
                    self.process_thing(
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
            obj = types.SimpleNamespace()
            obj.__doc__ = description
            pickled = codecs.encode(pickle.dumps(obj), "base64").decode()
            key = f"{key}DASKSPLIT{value}DASKSPLIT{pickled}"
            # breakpoint()
            return [(key, obj)]


class ListDocumenter(DataDocumenter):
    objtype = "auto"
    directivetype = "data"
    member_order = 1
    content_indent = '   '
    priority = 100

    def __init__(self, *args, **kwargs):
        self.key_name, self.default_value, pickled = args[1].split("DASKSPLIT")
        self.unpickled = pickle.loads(codecs.decode(pickled.encode(), "base64"))
        super().__init__(*args, **kwargs)


    @classmethod
    def can_document_member(cls, member, membername, isattr, parent):
        return isinstance(member, types.SimpleNamespace)

    def parse_name(self) -> bool:
        return True

    def import_object(self, *args, **kwargs):
        self.object = self.unpickled
        # return ['', '', self.key_name, self.unpickled]
        return True

    def format_name(self):
        # breakpoint()
        _, name = self.key_name.split('::')
        return name

    def add_directive_header(self, sig):
        # breakpoint()
        default = self.default_value

        # Ensures escape sequences render properly
        default_s = repr(repr(default))[1:-1]
        val = "{}".format(default_s)
        self.options.annotation = val
        self.modname = ""
        return super().add_directive_header(sig)


def setup(app):
    app.add_autodocumenter(ConfigurableDocumenter)
    app.add_autodocumenter(ListDocumenter)
