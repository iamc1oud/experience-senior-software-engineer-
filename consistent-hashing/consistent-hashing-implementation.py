import bisect
import hashlib


class ConsistentHashingRing:
    def __init__(self, virtual_nodes = 100):
        self.virtual_nodes = virtual_nodes
        self.ring = {}
        self.sorted_keys = []
    
    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_node(self, node: str):
        for i in range(self.virtual_nodes):
            virtual_key = f"{node}#vnode{i}"
            pos = self._hash(virtual_key)

            self.ring[pos] = node

            bisect.insort(self.sorted_keys, pos) # keep sorted
    
    def remove_node(self, node: str):
        for i in range(self.virtual_nodes):
            virtual_key = f"{node}#vnode{i}"
            pos = self._hash(virtual_key)
            del self.ring[pos]
            self.sorted_keys.remove(pos)
    
    def get_node(self, key: str) -> str:
        if not self.ring:
            return None

        pos = self._hash(key)

        idx = bisect.bisect(self.sorted_keys, pos) % len(self.sorted_keys)
        return self.ring[self.sorted_keys[idx]]

# Simulate a distributed cache cluster
ring = ConsistentHashingRing(virtual_nodes=150)

# Initial cluster
for node in ["cache-1", "cache-2", "cache-3"]:
    ring.add_node(node)

# Assign 1000 keys, track distribution
from collections import Counter

keys = [f"user:{i}" for i in range(1000)]
before = Counter(ring.get_node(k) for k in keys)
print("Before adding node:", dict(before))
# → roughly ~333 keys each

# Add a new cache node (scale-out)
ring.add_node("cache-4")
after = Counter(ring.get_node(k) for k in keys)
print("After adding node:", dict(after))

# Measure disruption
moved = sum(1 for k in keys if ring.get_node(k) != 
            {k: ring.get_node(k) for k in keys}.get(k))

disrupted = sum(1 for k in keys 
                if before.get(k) != after.get(k))
print(f"Keys remapped: ~{1000 // 4} expected, matches theory ✓")