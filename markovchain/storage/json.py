import sys
import json
from collections import deque, Counter
from itertools import chain, repeat, tee

from .base import Storage


NONE_VALUE = "\x00\x00"


def dehydrate_key(key):
    return (NONE_VALUE if key is None else key)


def hydrate_key(key):
    return (None if key == NONE_VALUE else key)


def counterify(node_list):
    if not node_list:
        return Counter()
    if isinstance(node_list, dict):  # Plain dict, probably just cast it
        return Counter({hydrate_key(key): value for (key, value) in node_list.items()})
    if isinstance(node_list[0], int):
        value, key = node_list
        return Counter({hydrate_key(key): value})
    return Counter({hydrate_key(key): value for (value, key) in zip(*node_list)})


def hydrate_nodes(nodes):
    if not nodes:
        return nodes
    assert isinstance(nodes, dict)
    nodes = nodes.copy()
    for statename in list(nodes):
        nodes[statename] = {
            hydrate_key(src): counterify(dst)
            for (src, dst)
            in nodes[statename].items()
        }
    return nodes


def dehydrate_nodes(nodes):
    if not nodes:
        return nodes
    return {
        statename: {
            dehydrate_key(src): {dehydrate_key(dst): val for (dst, val) in dsts.items()}
            for src, dsts
            in state.items()
        }
        for (statename, state)
        in nodes.items()
    }


class JsonStorage(Storage):
    """JSON storage.

    Attributes
    ----------
    nodes : Dict[str, Counter]
    backward : Dict[str, Counter] | None
    """
    def __init__(self, nodes=None, backward=None, settings=None):
        """JSON storage constructor.

        Parameters
        ----------
            nodes : Dict[str, Counter], optional
            backward : `bool` or Dict[str, Counter], optional
        """
        if nodes is None:
            nodes = {}

        if backward is None and settings and settings.get('storage', {}).get('backward', False):
            backward = {}
        elif backward is False:
            backward = None
        elif backward is True:
            backward = {}

        super().__init__(settings)
        self.nodes = nodes
        self.backward = backward

    def __eq__(self, storage):
        return (self.nodes == storage.nodes
                and self.backward == storage.backward
                and super().__eq__(storage))

    @staticmethod
    def do_replace_state_separator(data, old, new):
        """Replace state separator.

        Parameters
        ----------
        data : Dict[str, Dict[str, Counter]]
            Data.
        old : `str`
            Old separator.
        new : `str`
            New separator.
        """
        # TODO: does this work correctly?
        for key, dataset in data.items():
            data[key] = dict(
                (k.replace(old, new), v)
                for k, v in dataset.items()
            )

    @staticmethod
    def do_get_dataset(data, key, create=False):
        """Get a dataset.

        Parameters
        ----------
        data : `None` or Dict[str, Dict[str, Counter]]
            Data.
        key : `str`
            Dataset key.
        create : `bool`, optional
            Create a dataset if it does not exist.

        Returns
        -------
        `None` or Dict[str, Counter]
        """
        if data is None:
            return None

        if not create:
            return data[key]

        v = data.get(key)
        if v is None:
            data[key] = v = {}
        return v

    @staticmethod
    def add_link(dataset, source, target, count=1):
        """Add a link.

        Parameters
        ----------
        dataset : Dict[str, Counter]
            Dataset.
        source : `iterable` of `str`
            Link source.
        target : `str`
            Link target.
        count : `int`, optional
            Link count (default: 1).
        """
        if source not in dataset:
            dataset[source] = Counter()
        dataset[source][target] += count

    def replace_state_separator(self, old_separator, new_separator):
        self.do_replace_state_separator(
            self.nodes,
            old_separator,
            new_separator
        )
        if self.backward is not None:
            self.do_replace_state_separator(
                self.backward,
                old_separator,
                new_separator
            )

    def get_dataset(self, key, create=False):
        return (
            self.do_get_dataset(self.nodes, key, create),
            self.do_get_dataset(self.backward, key, create)
        )

    def add_links(self, links, dataset_prefix=''):
        for dataset, src, dst in links:
            forward, backward = self.get_dataset(dataset_prefix + dataset, True)
            if backward is not None and dst is not None:
                src, src2 = tee(src)
                dst2 = next(src2)
                src2 = self.join_state(chain(src2, (dst,)))
                self.add_link(backward, src2, dst2)
            src = self.join_state(src)
            self.add_link(forward, src, dst)

    def get_state(self, state, size):
        return deque(chain(repeat('', size), state), maxlen=size)

    def get_states(self, dataset, string):
        dataset = self.get_dataset(dataset)[0]
        string = string.lower()
        return [key for key in dataset.keys() if string in key.lower()]

    def get_links(self, dataset, state, backward=False):
        """
        Raises
        ------
        ValueError
            If backward == `True` and self.backward is `None`.
        """
        if backward and self.backward is None:
            raise ValueError('no backward nodes')
        try:
            node = dataset[int(backward)][self.join_state(state)]
            return [(num, w) for (w, num) in node.items()]
        except KeyError:
            return []

    def follow_link(self, link, state, backward=False):
        value = link[1]
        if backward:
            state.appendleft(value)
        else:
            state.append(value)
        return state

    def do_save(self, fp=None):
        """Save to file.

        Parameters
        ----------
        fp : `file` or `str`, optional
            Output file (default: stdout).
        """

        data = {
            'settings': self.settings,
            'nodes': dehydrate_nodes(self.nodes),
            'backward': dehydrate_nodes(self.backward),
        }

        if fp is None:
            json.dump(data, sys.stdout, ensure_ascii=False)
        elif isinstance(fp, str):
            with open(fp, 'w+') as fp2:
                json.dump(data, fp2, ensure_ascii=False)
        else:
            json.dump(data, fp, ensure_ascii=False)

    def close(self):
        pass

    @classmethod
    def load(cls, fp):
        if isinstance(fp, str):
            with open(fp, 'rt') as fp2:
                data = json.load(fp2)
        else:
            data = json.load(fp)

        if 'nodes' in data:
            data['nodes'] = hydrate_nodes(data['nodes'])
        if 'backward' in data:
            data['backward'] = hydrate_nodes(data['backward'])

        return cls(**data)
