import sys, os, time
from random import choice

if len(sys.argv) != 2:
	print("Wrong number of arguements")
	sys.exit()

exp = sys.argv[1]
nodes = ["node1a", "node1b", "node2a", "node2b", "node3a", "node3b", "node4a", "node4b", "node5a", "node5b"]

if exp == "insert":
	f = open("input/insert.txt", "r")
	cmds = open("input/in.txt", "w")
	lines = f.readlines()

	for line in lines:
		command = choice(nodes) + ", insert, " + line
		cmds.write(command)

elif exp == "query":
	f = open("input/query.txt", "r")
	cmds = open("input/in.txt", "w")
	lines = f.readlines()

	for line in lines:
		command = choice(nodes) + ", query, " + line
		cmds.write(command)

elif exp == "requests":
	f = open("input/requests.txt", "r")
	cmds = open("input/in.txt", "w")
	lines = f.readlines()

	for line in lines:
		l = line.split(',')
		command = choice(nodes) + ", " + line
		cmds.write(command)

else:
	print("Wrong experiment name")
	sys.exit()

f.close()
cmds.close()

t = time.time()
os.system("cat input/in.txt | python exp_cli.py")
print("Experiment time: {:.2f} seconds".format(time.time() - t))
