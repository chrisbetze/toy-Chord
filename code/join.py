import os

nodes = ["node1a", "node1b", "node2a", "node2b", "node3a", "node3b", "node4a", "node4b", "node5a", "node5b"]

joins = open("joins.txt", "w")
for node in nodes[1:]:
	joins.write("{}, join\n".format(node))

joins.close()

os.system("cat joins.txt | python exp_cli.py")
