class DisjointSet():
    def __init__(self, raw: set):
        self._parents = {x: x for x in raw}
    
    def find(self, u):
        if (u != self._parents[u]):
            self._parents[u] = self.find(self._parents[u])
        return self._parents[u]
    
    def is_connect(self, u, v):
        return self.find(u) == self.find(v)
    
    def union(self, u, v):
        root_u, root_v = self.find(u), self.find(v)
        if root_u != root_v:
            self._parents[root_u] = root_v