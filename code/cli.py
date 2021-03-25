from __future__ import print_function
import requests
import sys
import pyfiglet
from hashlib import sha1
from os import system


def encrypt(id):
  return int(sha1(id.encode()).hexdigest(), 16) % 2**160

system('clear')
print(pyfiglet.figlet_format("Welcome to Chord"))

nodes = {
    'node1a': {'ip': "192.168.1.1:5000", 'id': encrypt("192.168.1.1:5000")},
	'node1b': {'ip': "192.168.1.1:5001", 'id': encrypt("192.168.1.1:5001")},
	'node2a': {'ip': "192.168.1.2:5000", 'id': encrypt("192.168.1.2:5000")},
	'node2b': {'ip': "192.168.1.2:5001", 'id': encrypt("192.168.1.2:5001")},
	'node3a': {'ip': "192.168.1.3:5000", 'id': encrypt("192.168.1.3:5000")},
	'node3b': {'ip': "192.168.1.3:5001", 'id': encrypt("192.168.1.3:5001")},
	'node4a': {'ip': "192.168.1.4:5000", 'id': encrypt("192.168.1.4:5000")},
	'node4b': {'ip': "192.168.1.4:5001", 'id': encrypt("192.168.1.4:5001")},
	'node5a': {'ip': "192.168.1.5:5000", 'id': encrypt("192.168.1.5:5000")},
	'node5b': {'ip': "192.168.1.5:5001", 'id': encrypt("192.168.1.5:5001")}
}


while 1:
	print("\n\033[92mEnter command\033[00m: ", end="")
	line_list = sys.stdin.readline().replace('\n', '').split(', ')

	if (line_list[0] == ''):
		print()
		sys.exit()

	
	if (len(line_list) < 2 or len(line_list) > 4) and line_list[0] != 'help' and line_list[0] != 'exit':
		print('\033[91mError!\033[00m Wrong number of arguements. Try \033[1m"help"\033[0m command.')
		continue
	

	if line_list[0] == 'help':
		print('\033[1mCommand format\033[0m: nodeXX command.')
		print('\033[1mAvailable commands\033[0m:')
		print('nodeXX, \033[1mjoin\033[0m: nodeXX joins the chord.')
		print('nodeXX, \033[1mdepart\033[0m: nodeXX departs the chord.')
		print('nodeXX, \033[1minsert, key, value\033[0m: nodeXX inserts key, value to the chord.')
		print('nodeXX, \033[1mdelete, key\033[0m: nodeXX deletes key from the chord.')
		print('nodeXX, \033[1mquery, key\033[0m: nodeXX queries the chord for key.')
		print('                 ("*" as key returns all entries from each node)')
		print('nodeXX, \033[1moverlay\033[0m: nodeXX prints chord structure.')
		print('\033[1mexit\033[0m: exit the CLI.')
		continue

	if line_list[0] == 'exit':
		print('\033[36mBye.\033[0m')
		break
  
	node = line_list[0]
	action = line_list[1]

	if node not in nodes:
		print("\033[91mError!\033[00m No node named {}.".format(node))
		continue

	node_ip = nodes[node]['ip']
	node_id = nodes[node]['id']
  
	if action == 'join':
		if (len(line_list) != 2):
			print('\033[91mError!\033[00m Wrong number of arguements. Try \033[1m"help"\033[0m command.')
			continue
			
		# Send to node
		url_get = 'http://'+node_ip+'/init'
		x = requests.post(url_get, data={'id':node_id})
		print(x.text)


	elif action == 'depart':
		if (len(line_list) != 2):
			print('\033[91mError!\033[00m Wrong number of arguements. Try \033[1m"help"\033[0m command.')
			continue
			
		url = 'http://'+node_ip+'/depart'
		x = requests.post(url)
		print(x.text)

	elif action == 'insert':
		if (len(line_list) != 4):
			print('\033[91mError!\033[00m Wrong number of arguements. Try \033[1m"help"\033[0m command.')
			continue
			
		key = line_list[2].replace(',','')
		value = line_list[3]
		encrypted_key = encrypt(key)
    
		url = 'http://'+node_ip+'/execute'
		obj = {'action':'insert', 'key':encrypted_key, 'value':[value,key], 'src_ip':node_ip}
		x = requests.post(url, data = obj)
		for node in nodes:
			if nodes[node]['id'] == int(x.text.split()[-1]):
				name = node
		print(x.text+" ("+name+")")
	
	elif action == 'delete':
		if (len(line_list) != 3):
			print('\033[91mError!\033[00m Wrong number of arguements. Try \033[1m"help"\033[0m command.')
			continue
			
		key = line_list[2]
		encrypted_key = encrypt(key)
		
		url = 'http://'+node_ip+'/execute'
		obj = {'action':'delete', 'key':encrypted_key, 'value':['None',key], 'src_ip':node_ip}
		x = requests.post(url, data = obj)
		for node in nodes:
			if nodes[node]['id'] == int(x.text.split()[-1]):
				name = node
		print(x.text+" ("+name+")")

	
	elif action == 'query':
		if (len(line_list) != 3):
			print('\033[91mError!\033[00m Wrong number of arguements. Try \033[1m"help"\033[0m command.')
			continue
			
		key = line_list[2]
		
		if key != '*':
			encrypted_key = encrypt(key)
			url = 'http://'+node_ip+'/execute'
			obj = {'action':'query', 'key':encrypted_key, 'src_ip':node_ip, 'value':['None',key]}
			x = requests.post(url, data = obj)
			for node in nodes:
				if nodes[node]['id'] == int(x.text.split()[-1]):
					name = node
			print(x.text+" ("+name+")")

		else:
			url = 'http://'+node_ip+'/star'
			x = requests.get(url, params={'src_ip':node_ip, 'id':'None'})
			for ids in x.json().keys():
				for node in nodes:
					if nodes[node]['id'] == int(ids):
						name = node

				print (name,int(ids))
				items = x.json()[ids]
				for key, value in items.items():
					print ("({}->{}, {})".format(value[1],key,value[0]))
				if len(items.items()) == 0:
					print ("No key, value pairs found")
				print("")
	
	elif action == 'overlay':
		if (len(line_list) != 2):
			print('\033[91mError!\033[00m Wrong number of arguements. Try \033[1m"help"\033[0m command.')
			continue
			
		url = 'http://'+node_ip+'/overlay'
		obj = {'id_list':['None']}
		x = requests.post(url, data = obj)
		print("The topology of the chord is:")
		for ids in x.json()['id_list']:
			for node in nodes:
				if nodes[node]['id'] == int(ids):
					name = node
			print (name, int(ids))

	else:
		print('\033[91mError!\033[00m No command "{}". Try \033[1m"help"\033[0m to see available commands.'.format(action))
