# digraph data structure:
#	graph = {v1 : [v ...], v2 : [v ...], ...} # to <- from

ancestors = dict()
graph = {2: [11], 9: [8, 11], 10: [3, 11], 11: [5, 7], 8: [3, 7], 3: [], 5: [], 7: []}

def getAncestors(node):
	serialize = []
	visited = set()
	def dfs(node):
		if node not in visited:
			serialize.append(node)
			visited.add(node)
		for p in graph[node]:
			dfs(p)
	dfs(node)
	return serialize

for v in graph:
	ancestors[v] = getAncestors(v)

print ancestors