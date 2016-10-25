"""
Nanoflow Graph:

Define a computational graph.
All operations will be run async. 

Feed data into an node by providing a feed_dict
to the run operation.
"""

import asyncio


class Node(object):
    """
    Nanoflow nodes contain  computational operations.
    These operations are exectued async
    """

    def __init__(self, inputs=[], name=None, fn=None):
        """Initialize new operation node"""
        if not isinstance(inputs, (list, tuple)):
            inputs = [inputs]

        self.inputs = inputs
        self.name = name

        self.fn = fn

        self.has_cached_result = False
        self.cached_result = None


    async def fetch_inputs(self):
        fetchers = [asyncio.ensure_future(node_in.result())
                    for node_in in self.inputs]

        inputs = await asyncio.gather(*fetchers)
        return inputs


    async def fetch_input(self, i):
        result = await self.inputs[i].result()
        return result


    async def result(self):
        if self.has_cached_result:
            return self.cached_result

        if self.fn:
            inputs = await self.fetch_inputs()

            if asyncio.iscoroutinefunction(self.fn):
                res = await self.fn(*inputs)
            else:
                res = self.fn(*inputs)

            self.cached_result = res
            self.has_cached_result = True

            return res

        return None



def op(fn):
    """
    Operation decorator:
    Wrap a (async) function in a Node
    """
    def wrap(*args, **kwargs):
        kwargs['fn'] = fn
        return Node(*args, **kwargs)
    return wrap


class Placeholder(Node):
    """
    Placeholder keep data and will be initialized
    from the feed_dict.

    It's important to give your placholders a name, so
    we can look them up and feed the from the feed_dict
    before executing the graph.
    """

    def feed(self, value):
        self.value = value

    async def result(self):
        return self.value



def _find_named_node(node, name):
    """Small helper to find a node by name"""
    if node.name == name:
        return node

    # check if the node is present as input
    for node_in in node.inputs:
        res = _find_named_node(node_in, name)
        if res:
            return res

    return None


def _clear_caches(node):
    """Reset all caches in graph"""
    if node == None:
        return
    node.has_cached_result = False
    node.cached_result = None

    for node_in in node.inputs:
        _clear_caches(node_in)



def run_async(output, feed_dict={}):
    # Set placeholders
    for key, val in feed_dict.items():
        input_node = _find_named_node(output, key)
        if input_node:
            input_node.feed(val)

    # Clear caches
    _clear_caches(output)

    future = asyncio.ensure_future(output.result())
    return future


def run(output, feed_dict={}):
    """Run the graph on the event loop"""
    loop = asyncio.get_event_loop()

    # Run graph
    future = run_async(output, feed_dict)
    result = loop.run_until_complete(future)
    return result
