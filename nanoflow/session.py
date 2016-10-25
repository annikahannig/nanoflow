"""
Provide a session store for separating
dataflow between sessions
"""


class Session(object):
    """Nanoflow sessio"""

    def __init__(self):
        """Initialize datastorage"""
        self.data = {}

    def get(self, node, key=0, default=None):
        """Retrieve node data"""
        return self.data.get('{}_{}'.format(node.__hash__(), key), default)

    def set(self, node, key, value):
        """Store node data"""
        self.data['{}_{}'.format(node.__hash__(), key)] = value
        return self

