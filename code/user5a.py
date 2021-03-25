from flask import Flask, render_template, request
from time import sleep
import requests
import logging
import sys
import threading

app = Flask(__name__)
log = logging.getLogger('werkzeug')
log.disabled = True

items = {}
info = {'my_id':None, 'my_ip':'192.168.1.5:5000', 'prev_id':None, 'next_id':None, 'prev_ip':None, 'next_ip':None, 'my_replica_id':None}
linearizability = (sys.argv[1] == "lin")
replication_factor = int(sys.argv[2])

if replication_factor == 1:
	linearizability = False


def key_in_area(key, start, end):
	# Returns True if key in (start, end], otherwise False
	a = False
	if start == 'None':
		a = True
	elif start < end:
		if key > start and key <= end:
			a = True
	elif start > end:
		if key > start or key <= end:
			a = True	
	else:
		a = True
	return a


# Route for initializing join procedure
@app.route('/init',methods = ['POST'])
def init():
	info['my_id'] = int(request.form['id'])
	url ="http://192.168.1.1:5000/join"
	x = requests.post(url, data={'ip':info['my_ip'], 'id':info['my_id']})

	if replication_factor != 1:
		url_replica = "http://"+info['my_ip']+"/replica"
		x = requests.post(url_replica, data = {'k':replication_factor, 'k_next':replication_factor, 'src_ip':info['my_ip'], 'replica_id':-1, 'join_ip':info['my_ip']})

	return "OK"


# Route for handling node join
@app.route('/join',methods = ['POST', 'GET'])
def join():
	if request.method == 'POST':
		node_ip = request.form['ip']
		node_id = int(request.form['id'])
		
		if key_in_area(node_id, info['prev_id'], info['my_id']):
			# Inform new node myself
			url = 'http://'+node_ip+'/join'
			obj = {'prev_id':info['prev_id'], 'next_id':info['my_id'], 'prev_ip':info['prev_ip'], 'next_ip':info['my_ip']}
			for key, value in items.items():
				# Send new node its (key, value) pairs
				if key_in_area(int(key), info['prev_id'], node_id):
					items.pop(int(key))
					obj.update({int(key):value})
			x = requests.get(url, params = obj)
		else:
			# Pass to next node
			url_next = "http://"+info['next_ip']+"/join"
			x = requests.post(url_next, data = {'ip':node_ip, 'id':node_id})
			
	if request.method == 'GET':
		temp = request.args.to_dict(flat=False)
		info['prev_id'] = int(temp.pop('prev_id')[0])
		info['next_id'] = int(temp.pop('next_id')[0])
		info['prev_ip'] = temp.pop('prev_ip')[0]
		info['next_ip'] = temp.pop('next_ip')[0]
		print "I joined the chord with id",info['my_id']
		print ("My (prev, next) neighbors are: ({}, {})".format(info['prev_id'],info['next_id']))

		for key, value in temp.items():
			# Store (key, value) pairs
			items.update({int(key):value})

		# Inform neighbors
		url_prev = "http://"+info['prev_ip']+"/neighbors"
		url_next = "http://"+info['next_ip']+"/neighbors"
		x = requests.post(url_prev, data = {'next_id': info['my_id'], 'next_ip':info['my_ip']})
		y = requests.post(url_next, data = {'prev_id': info['my_id'], 'prev_ip':info['my_ip']})
	return "OK"


# Route for handling node depart
@app.route('/depart',methods = ['POST', 'GET'])
def depart():
	if request.method == 'POST':
		url_prev = "http://"+info['prev_ip']+"/neighbors"
		url_next = "http://"+info['next_ip']+"/neighbors"
		url_next_get = "http://"+info['next_ip']+"/depart"

		# Inform neighbors
		x = requests.post(url_prev, data = {'next_id': info['next_id'], 'next_ip':info['next_ip']})
		y = requests.post(url_next, data = {'prev_id': info['prev_id'], 'prev_ip':info['prev_ip']})
		w = requests.get(url_next_get, params = items)

		# Clear (key, value) pairs
		items.clear()
		print "I departed from the chord"
		
		if replication_factor != 1:
			# Update replica managers
			url_next_replica = "http://"+info['next_ip']+"/replica"
			obj = {'k':replication_factor, 'k_next':replication_factor, 'src_ip':info['next_ip'], 'replica_id':-1, 'join_ip':info['next_ip']}
			x = requests.post(url_next_replica, data = obj)


	if request.method == 'GET':
		# Update keys in case another node departs
		for key, value in request.args.to_dict(flat=False).items():
			items.update({int(key):value})
		#print "I updated my key, value pairs"
	return "OK"


# Route for updating neighbors during joins/departs
@app.route('/neighbors',methods = ['POST'])
def neighbor():
	if request.form.keys()[0] == 'next_id':
		info['next_id'] = int(request.form['next_id'])
		info['next_ip'] = request.form['next_ip']
	else:
		info['prev_id'] = int(request.form['prev_id'])
		info['prev_ip'] = request.form['prev_ip']
	print ("My new (prev, next) neighbors are: ({}, {})".format(info['prev_id'],info['next_id']))
	return "OK"


# Route for executing inserts/deletes/queries
@app.route('/execute',methods = ['POST', 'GET'])
def execute():
	if request.method == 'POST':
		action = request.form['action']
		key = int(request.form['key'])
		value = request.form.to_dict(flat=False)['value']
		src_ip = request.form['src_ip']

		if action == 'query' and not linearizability and replication_factor != 1:
			# In an eventual consistency query, node can return a replica
			last_id = info['my_replica_id']
		else:
			last_id = info['prev_id']

		if key_in_area(key, last_id, info['my_id']):
			# Execute action
			if action == 'insert': 
				items[key] = value
				#print("Store (key, value): ({}->{}, {})".format(value[1],key,value[0]))
			elif action == 'delete':
				value = items.pop(key, ['No_key_found',value[1]])
			elif action == 'query':
				value = items.get(key, ['No_key_found',value[1]])

			# Pass action to replica managers
			if linearizability:
				if info['next_ip'] != info['my_ip']:
					url_next = "http://"+info['next_ip']+"/execute"
					obj = {'action':action, 'key':key, 'value':value, 'src_ip':src_ip, 'k':replication_factor, 'rm':info['my_id']}
					x = requests.get(url_next, params = obj)
					return x.text
					
			elif not linearizability:
				if src_ip != info['my_ip']:
					# Send result to source
					url_src = "http://"+src_ip+"/execute"
					obj = {'action':action, 'key':key, 'value':value, 'src_ip':src_ip, 'return_id':info['my_id']}
					x = requests.get(url_src, params = obj)
				else:
					print("{} (key, value): ({}->{}, {}) successful from node {}".format(action,value[1],key,value[0],info['my_id']))
				
				if info['next_ip'] != info['my_ip'] and replication_factor != 1 and action != 'query':
					def replicate(**kwargs):
						# Thread function to replicate insert/update with artificial lag
						sleep(0.0001)
						y = requests.get(kwargs['url'], params = kwargs['obj'])
						
					url_next = "http://"+info['next_ip']+"/execute"
					obj = {'action':action, 'key':key, 'value':value, 'src_ip':src_ip, 'k':replication_factor, 'rm':info['my_id']}
					# Pass action with a separate thread to return asap
					thread = threading.Thread(target=replicate, kwargs={'url': url_next, 'obj': obj})
					thread.start()
				
				if src_ip != info['my_ip']:
					return x.text
				else:
					return "{} (key, value): ({}->{}, {}) successful from node {}".format(action,value[1],key,value[0],info['my_id'])
 
		else: # If key not in (last_id, 'my_id']
			if linearizability and action == 'query' and key_in_area(key, info['my_replica_id'], info['my_replica_mn']):
				# Node responds if it is last in replication chain and action is query
				value = items.get(key, ['No_key_found',value[1]])

				if src_ip != info['my_ip']:
					# Send result to source
					url_src = "http://"+src_ip+"/execute"
					obj = {'action':action, 'key':key, 'value':value, 'src_ip':src_ip, 'return_id':info['my_id']}
					x = requests.get(url_src, params = obj)
					return x.text
				else:
					print("{} (key, value): ({}->{}, {}) successful from node {}".format(action,value[1],key,value[0],info['my_id']))
					return "{} (key, value): ({}->{}, {}) successful from node {}".format(action,value[1],key,value[0],info['my_id'])
			else:
				# Send action to next node
				url_next = "http://"+info['next_ip']+"/execute"
				obj = {'action':action, 'key':key, 'value':value, 'src_ip':src_ip}
				#print ("Send (key, value): ({}->{}, {}) to next node with id {}".format(value[1],key,value[0],info['next_id']))
				x = requests.post(url_next, data = obj)
				return x.text
	

	if request.method == 'GET':
		action = request.args['action']
		key = int(request.args['key'])
		value = request.args.to_dict(flat=False)['value']
		src_ip = request.args['src_ip']

		if 'return_id' in request.args.keys():
			# If response is sent to the source
			return_id = request.args['return_id']
			print("{} (key, value): ({}->{}, {}) successful from node {}".format(action,value[1],key,value[0],return_id))
			return "{} (key, value): ({}->{}, {}) successful from node {}".format(action,value[1],key,value[0],return_id)
		else:
			# If action is passed to replica managers
			if action == 'insert':
				items[key]=value
			elif action == 'delete':
				value = items.pop(key, ['No_key_found',value[1]])
			elif action == 'query':
				value = items.get(key, ['No_key_found',value[1]])

			# Continue to next replica managers
			k = int(request.args['k']) - 1
			rm = int(request.args['rm'])

			if k == 1 or rm == info['next_id']:
				# Node is last replica manager
				if linearizability:
					url_src = "http://"+src_ip+"/execute"
					obj = {'action':action, 'key':key, 'value':value, 'src_ip':src_ip, 'return_id':info['my_id']}
					x = requests.get(url_src, params = obj)
					return x.text
			else:
				url_next = "http://"+info['next_ip']+"/execute"
				obj = {'action':action, 'key':key, 'value':value,'src_ip':src_ip, 'k':k, 'rm':rm}
				x = requests.get(url_next, params = obj)
				return x.text
	return "OK"
	

# Route for query '*'
@app.route('/star',methods = ['GET'])
def query():
	src_ip = request.args['src_ip']

	if info['my_ip'] != src_ip:
		url_src = "http://"+src_ip+"/star"
		data = items.copy()
		data['id'] = info['my_id']
		data['src_ip'] = src_ip
		#print "Send all my key, value pairs to source node"
		x = requests.get(url_src, params = data)
		a = x.json()
		if info['next_ip'] != src_ip:
			url_next = "http://"+info['next_ip']+"/star"
			y = requests.get(url_next, params = {'src_ip': src_ip})
			a.update(y.json())
		return a

	else:
		remote_id = request.args['id']

		if remote_id == 'None':
			print "Node", info['my_id']
			for key,value in items.items():
				print ("({}->{}, {})".format(value[1],key,value[0]))
			if len(items.items())==0:
				print "No key, value pairs found"
			if info['next_id'] != None:
				url_next = "http://"+info['next_ip']+"/star"
				x = requests.get(url_next, params = {'src_ip': src_ip})

			b = {info['my_id']:items}
			b.update(x.json())
			return b
		else:
			sent_items = request.args.to_dict(flat=False)
			sent_items.pop('id', 'No id found')
			sent_items.pop('src_ip', 'No ip found')

			print "Node", remote_id
			
			for key,value in sent_items.items():
				print ("({}->{}, {})".format(value[1],key,value[0]))
			
			if len(sent_items.items())==0:
				print "No key, value pairs found"
			return {remote_id:sent_items}
	return "OK"


# Route for Chord ring overlay
@app.route('/overlay',methods = ['POST'])
def overlay():
	id_list = request.form.to_dict(flat=False)['id_list']

	if str(info['my_id']) not in id_list:
		# Add node to overlay
		id_list.append(info['my_id'])
		url_next = "http://"+info['next_ip']+"/overlay"
		x = requests.post(url_next, data = {'id_list': id_list})
		return x.json()
	else: # Message has completed the ring
		id_list.remove('None')

		# Return topology
		print ("The topology of the chord is: {}".format(map(int, id_list)))
		return {'id_list':id_list}
	return "OK"


# Route for replica management during join/depart
@app.route('/replica',methods = ['POST', 'GET'])
def replica():
	if request.method == 'POST':
		k = int(request.form['k']) - 1
		k_next = int(request.form['k_next'])
		src_ip = request.form['src_ip']
		replica_id = int(request.form['replica_id'])
		join_ip = request.form['join_ip']
		
		if info['my_ip'] == src_ip and replica_id !=-1:
			info['my_replica_id'] = replica_id
			info['my_replica_mn'] = int(request.form['replica_mn'])
			#print "My replica id, mn is:", info['my_replica_id'], info['my_replica_mn'] 
			
			for key, value in items.items():
				if not key_in_area(key, info['my_replica_id'], info['my_id']):
					items.pop(key, 'No_key_found')

			if k_next != 0 and info['next_ip'] != join_ip:
				url_next = "http://"+info['next_ip']+"/replica"
				obj = {'k':replication_factor, 'src_ip':info['next_ip'], 'replica_id':-1, 'k_next':k_next-1, 'join_ip':join_ip}
				x = requests.post(url_next, data = obj)
		elif info['my_ip'] == src_ip and replica_id == -1 and info['prev_ip']!=info['my_ip']:
			url_prev = "http://"+info['prev_ip']+"/replica"
			obj = {'k': k, 'src_ip':src_ip, 'replica_id':-1, 'k_next':k_next, 'join_ip':join_ip}
			x = requests.post(url_prev, data = obj)

		elif info['prev_ip'] == src_ip or k == 0:
			sent = {}
			for key, value in items.items():
				if key_in_area(key, info['prev_id'], info['my_id'])==True:
					sent[key] = value
			url_src = "http://"+src_ip+"/replica"
			x = requests.get(url_src, params = sent)

			obj = {'k': k, 'src_ip':src_ip, 'replica_id':info['prev_id'], 'replica_mn':info['my_id'], 'k_next':k_next, 'join_ip':join_ip}
			x = requests.post(url_src, data = obj)

		else:
			sent = {}
			for key, value in items.items():
				if key_in_area(key, info['prev_id'], info['my_id'])==True:
					sent[key] = value
			url_src = "http://"+src_ip+"/replica"
			x = requests.get(url_src, params = sent)

			url_prev = "http://"+info['prev_ip']+"/replica"
			obj = {'k': k, 'src_ip':src_ip, 'replica_id':-1, 'k_next':k_next, 'join_ip':join_ip}
			x = requests.post(url_prev, data = obj)

	# Update (key, value) pairs of node
	if request.method == 'GET':
		for key, value in request.args.to_dict(flat=False).items():
			items.update({int(key):value})
	return "OK"


if __name__ == '__main__':
	app.run(host='192.168.1.5', port=5000, debug = False)
