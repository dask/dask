import os
import requests
from docutils.parsers.rst.directives import tables
import distributed
from docutils import statemachine

distributed_path = os.path.dirname(distributed.__file__)
distributed_config = os.path.join(distributed_path, "distributed.yaml")
import yaml
# from ruamel.yaml import YAML
# from ruamel.yaml.comments import CommentedMap

# yaml = YAML()
# yaml.preserve_quotes = True

def get_remote_yaml(url):
    r = requests.get(url)
    return yaml.safe_load(r.text)

with open(distributed_config) as f:
    _data = f.read()
    yaml_conf = yaml.load(_data)


class YamlToTable(tables.CSVTable):

    required_arguments = 1

    def __init__(self, *args, **kwargs):
        # .. yamltotable:: SECTION CONFIG-FILE JSONSCHEMA
        self.section = args[1][0]
        self.config, self.schema = args[1][1].split()
        super().__init__(*args, **kwargs)

        self.config = get_remote_yaml(self.config)
        self.schema = get_remote_yaml(self.schema)

        for k in self.section.split('.'):
            self.config = self.config[k]
            self.schema = self.schema['properties'][k]

        # self.data = yaml_conf["distributed"][[self.key]

    def get_csv_data(self):
        return 1, self.state_machine.get_source(self.lineno - 1)

    def process_thing(self, key, value, schema, prefix=""):
        if isinstance(value, dict):
            return sum([self.process_thing(k, v, schema['properties'].get(k, {"properties": {}}), prefix=prefix+'.'+key) for k, v in value.items()], [])

        else:

            try:
                description = schema['description']
                description = description.strip()
            except KeyError:
                description = "No Comment"

            key = (0, 0, 0, statemachine.StringList([prefix+"."+key], source=None))

            value = str(value)
            value = (0, 0, 0, statemachine.StringList([value], source=None))

            description = (0, 0, 0, statemachine.StringList([description], source=None))
            # breakpoint()
            return [[key, value, description]]

    def parse_csv_data_into_rows(self, csv_data, dialect, source):
        rows = self.process_thing(key="", value=self.config, schema=self.schema)
        # breakpoint()
        max_cols = 3  # size of each row
        return rows, max_cols

    def process_header_option(self):

        table_head = []
        row = []
        for col in self.options["header"].split(","):
            val = (0, 0, 0, statemachine.StringList([col], source=None))
            row.append(val)
        table_head.append(row)
        max_header_cols = len(table_head)
        return table_head, max_header_cols


def setup(app):
    app.add_directive("yamltotable", YamlToTable)

    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }


if __name__ == "__main__":
    pass
