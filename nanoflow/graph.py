"""
Nanofibers Graph:

Define a computational graph, run in a session.
All operations will be run async. 

Feed data into an node by providing a feed_dict
to the session.
"""

import asyncio



class Node(object):
    """
    Nanofiber nodes contain one computational
    operation. (Like unpacking request data,
    or serializing json).
    """

    def __init__(self, inputs=[], name=None, fn=None):
        """Initialize new operation node"""
        if not isinstance(inputs, (list, tuple)):
            inputs = [inputs]

        self.inputs = inputs
        self.name = name

        if not fn:
            # Use class method if present
            fn = getattr(self.__class__, 'fn', None)

        self.fn = fn

    def fetch_inputs(self):
        inputs = [node_in.result() for node_in in self.inputs]  # Make async
        return inputs


    def fetch_input(self, i):
        return self.inputs[i].result()


    def result(self):
        if self.fn:
            inputs = self.fetch_inputs()
            return self.fn(*inputs)
        return None


# Operation decorator
def op(fn):
    def wrap(*args, **kwargs):
        kwargs['fn'] = fn
        return Node(*args, **kwargs)
    return wrap


class Placeholder(Node):

    def feed(self, value):
        self.value = value

    def result(self):
        return self.value


class Sum(Node):
    def result(self):
        data = self.fetch_inputs()
        return sum(data)



class Quad(Node):
    def result(self):
        v = self.fetch_input(0)
        return v*v


def _find_named_node(node, name):
    if node.name == name:
        return node

    # check if the node is present as input
    for node_in in node.inputs:
        res = _find_named_node(node_in, name)
        if res:
            return res

    return None



def run(output, feed_dict={}):
    # Set placeholders
    for key, val in feed_dict.items():
        input_node = _find_named_node(output, key)
        input_node.feed(val)

    # Run graph
    return output.result()
