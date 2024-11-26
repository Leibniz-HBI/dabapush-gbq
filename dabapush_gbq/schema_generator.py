import json
from dataclasses import asdict, dataclass
from functools import reduce
from typing import Any, Dict, List, Optional, Tuple

import click
from tqdm.autonotebook import tqdm

type_lookup = {
    None: "NONE",
    str: "STRING",
    int: "INTEGER",
    float: "FLOAT",
    bool: "BOOLEAN",
}


@dataclass
class Item:
    key: str
    type: str
    nullable: bool
    repeated: bool

    def __repr__(self):
        return f"{self.key}: {self.type}\n"

    def merge(self, other: "Item") -> "Item":
        # print("A:", self, "B:", other)
        if self.key != other.key:
            raise ValueError(f"Keys of {self} and {other} are different.")
        if self == other:
            return self

        # if we find a None value, set the type to be nullable.
        if self.nullable is False and other.type is None:
            self.nullable = True
        # update the type if we have a None and another type incoming
        if self.type is None and other.type is not None:
            self.type = other.type
            self.nullable = True
            return self
        # if types are inconsistent, use str as a fallback
        if self.type != other.type:
            self.type = self.type or other.type  # catch Nones again?

        return self


@dataclass
class Record(Item):
    columns: Optional[List["Item"]]

    def __repr__(self):
        _children_ = "".join([str(_) for _ in self.columns])

        return f"""{self.key}:{_children_}\n"""

    # def todict(self):
    #    return {self.key: self.type, "columns": {_.key: _.type for _ in self.columns}}


def get_schema_for(dicts: List[Dict[str, Any]]):
    def get_schema(key, obj) -> Item:
        if isinstance(obj, dict):
            return Record(
                key,
                "RECORD",
                nullable=False,
                repeated=False,
                columns=[get_schema(_key, _value) for _key, _value in obj.items()],
            )
        if isinstance(obj, list):
            if obj:
                ret = reduce(merge_schemas, [get_schema(key, _) for _ in obj])
                ret.repeated = True
            else:
                ret = Item(key, None, False, True)
            return ret
        else:
            return Item(
                key, type_lookup.get(type(obj), None), nullable=False, repeated=False
            )

    def is_in_items(item: Item, items: List[Item]) -> bool:
        if not item:
            return False
        # compare by keys
        return item.key in [_.key for _ in items if _]

    def get_item_from_items(item: Item, items: List[Item]) -> Optional[Item]:
        for _item in items:
            if item.key == _item.key:
                return _item

    def merge_schemas(old, new):
        # print("NEW:", new, "OLD:", old)
        if not old:
            return new
        if isinstance(new, list) and isinstance(old, list):
            # stuff that's overlapping

            only_new = [_ for _ in new if not is_in_items(_, old)]
            only_old = [_ for _ in old if not is_in_items(_, new)]
            rest = [(_, get_item_from_items(_, new)) for _ in old if _ not in only_old]

            return [
                *only_old,
                *[merge_schemas(_old, _new) for _old, _new in rest],
                *only_new,
            ]
        if isinstance(new, Record) and isinstance(old, Record):
            # print("RECORD!!!1")
            old.columns = merge_schemas(old.columns, new.columns)
        elif isinstance(new, Record) and not isinstance(old, Record):
            return new
        elif isinstance(new, Item) and isinstance(old, Item):
            return old.merge(new)
        return old

    schemas = []
    for dictionary in tqdm(dicts, "Opening Dicts"):
        schemas.append(get_schema("root", dictionary))

    return reduce(
        merge_schemas, tqdm(schemas, desc="Reducing schemas"), Record("root", "RECORD", False, False, [])
    )


def parse_schema_for_bq(schema, max_depth=4, depth=0):
    keys_to_ignore = [
        "cached_page",
        "media",
    ]
    if not schema.type:
        raise ValueError(f"Type {schema.type} is invalid for key {schema.key}")

    if isinstance(schema, Record):
        # print(f"{schema.key}: {[_.key for _ in schema.columns]}")
        return {
            "name": schema.key,
            "type": "RECORD",
            "fields": [parse_schema_for_bq(_, max_depth=max_depth, depth=depth + 1)
                       for _ in schema.columns if _.type and _.key not in keys_to_ignore] if depth < max_depth else [],
            "mode": "REPEATED" if schema.repeated else "NULLABLE"
        }
    return {
            "name": schema.key,
            "type": schema.type,  #   if schema.type else "STRING"
            "mode": "REPEATED" if schema.repeated else "NULLABLE"
        }


@click.command()
@click.option("--output", "-o", type=click.File("wt"), default="-")
@click.argument("files", nargs=-1, type=click.Path(exists=True))
def main(output, files: Tuple[str]):
    if len(files) == 0:
        raise click.UsageError("At least one file is required.")
    dicts = []
    for file in files:
        with open(file, "rt", encoding="utf8") as f:
            for line in f:
                dicts.append(json.loads(line))

    schema = get_schema_for(dicts)
    with output as f:
        json.dump(parse_schema_for_bq(schema).get("fields", {}), f)
