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

    def bfs_traverse_links(self, callback):
        queue = []
        visited = set()
        for n in self._roots:
            queue.append(n)
        while len(queue):
            node = queue.pop(0)
            if node in visited:
                continue
            else:
                visited.add(node)
                if node.has_target:
                    for c in node.targets:
                        callback(node, c)
                        queue.append(c)

    def bfs_traverse(self, callback):
        queue = []
        visited = set()
        for n in self._roots:
            queue.append(n)
        while len(queue):
            node = queue.pop(0)
            if node in visited:
                continue
            else:
                callback(node)
                visited.add(node)
                if node.has_target:
                    for c in node.targets:
                        queue.append(c)
