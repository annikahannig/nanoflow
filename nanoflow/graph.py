"""
Nanoflow Graph:

Define a computational graph.
All operations will be run async. 

Feed data into an node by providing a feed_dict
to the run operation.
"""

import asyncio

from .session import Session

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


    async def fetch_inputs(self, session):
        fetchers = [asyncio.ensure_future(node_in.result(session))
                    for node_in in self.inputs]

        inputs = await asyncio.gather(*fetchers)
        return inputs


    async def fetch_input(self, session, i):
        result = await self.inputs[i].result(session)
        return result


    async def result(self, session):
        if session.get(self, 'has_cached_result'):
            return session.get(self, 'cached_result')

        if self.fn:
            inputs = await self.fetch_inputs(session)

            if asyncio.iscoroutinefunction(self.fn):
                res = await self.fn(*inputs)
            else:
                res = self.fn(*inputs)

            session.set(
                self, True, key='has_cached_result'
            ).set(
                self, res, key='cached_result')

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

    def feed(self, session, value):
        """Store value in session"""
        session.set(self, value)

    async def result(self, session):
        return session.get(self)


class Constant(Node):
    """
    Return a constant
    """
    def __init__(self, value):
        self.value = value;

    async def result(self, session):
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




def run_async(output, feed_dict={}):
    """Setup graph and return futrue for running on event loop"""

    # Create a new session for storing and
    # retriving values for nodes.
    session = Session()

    # Set placeholders
    for key, val in feed_dict.items():
        input_node = _find_named_node(output, key)
        if input_node:
            input_node.feed(session, val)


    future = asyncio.ensure_future(output.result(session))
    return future


def run(output, feed_dict={}):
    """Run the graph on the event loop"""
    loop = asyncio.get_event_loop()

    # Run graph
    future = run_async(output, feed_dict)
    result = loop.run_until_complete(future)
    return result
