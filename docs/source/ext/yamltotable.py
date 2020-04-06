import os
from docutils.parsers.rst.directives import tables
import distributed
from docutils import statemachine

distributed_path = os.path.dirname(distributed.__file__)
distributed_config = os.path.join(distributed_path, "distributed.yaml")
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

yaml = YAML()
yaml.preserve_quotes = True

with open(distributed_config) as f:
    _data = f.read()
    yaml_conf = yaml.load(_data)


class YamlToTable(tables.CSVTable):

    required_arguments = 1

    def __init__(self, *args, **kwargs):
        # .. yamltotable:: scheduler argument
        self.key = args[1][0]

        super().__init__(*args, **kwargs)
        self.data = yaml_conf["distributed"][self.key]

    def get_csv_data(self):
        return 1, self.state_machine.get_source(self.lineno - 1)

    def parse_csv_data_into_rows(self, csv_data, dialect, source):
        rows = []
        for idx in self.data:
            if not isinstance(self.data[idx], CommentedMap):
                comment_token = self.data.ca.items.get(idx)
                if comment_token is None:
                    comment = ""
                else:
                    comment = comment_token[2].value

                comment = comment.replace("#", "")
                comment = comment.strip()

                key = (0, 0, 0, statemachine.StringList([idx], source=None))

                val = str(self.data[idx])
                value = (0, 0, 0, statemachine.StringList([val], source=None))

                comment = (0, 0, 0, statemachine.StringList([comment], source=None))
                rows.append([key, value, comment])

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
