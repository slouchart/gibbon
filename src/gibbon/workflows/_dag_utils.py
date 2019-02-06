class _DirectedAcyclicGraph:
    def __init__(self):
        self._nodes = dict()
        self._roots = []

    def create_node(self, parent, cls, *args, **kwargs):
        node = cls(*args, **kwargs)

        if node is None:
            return None

        if parent:
            node.set_source(parent)

        if not node.has_source:
            self._roots.append(node)

        self._nodes[node.id] = node

        return node
