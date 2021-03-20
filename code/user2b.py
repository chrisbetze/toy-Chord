from flask import Flask, render_template, request
import requests
import logging
import sys
import threading

app = Flask(__name__)
log = logging.getLogger('werkzeug')
log.disabled = True

items = {}
info = {'my_id':None, 'my_ip':'192.168.1.2:5001', 'prev_id':None, 'next_id':None, 'prev_ip':None, 'next_ip':None, 'my_replica_id':None}
linearizability = (sys.argv[1] == "lin")
replication_factor = int(sys.argv[2])

if replication_factor == 1:
	linearizability = False

def in_my_area(key, prev, my_id):
	a = False
	if prev == 'None':
		a = True
	elif prev < my_id:
		if key > prev and key <= my_id:
			a = True
	elif prev > my_id:
		if key > prev or key <= my_id:
			a = True	
	else:
		a = True
	return a


@app.route('/get',methods = ['POST'])
def get():
	info['my_id'] = int(request.form['id'])
		
	url ="http://192.168.1.1:5000/join"
	x = requests.post(url, data={'ip':info['my_ip'], 'id':info['my_id']})
	if replication_factor!=1:
		url_replica = "http://"+info['my_ip']+"/replica"
		x = requests.post(url_replica, data = {'k':replication_factor, 'k2':replication_factor, 'src_ip':info['my_ip'], 'replica_id':-1, 'join_ip':info['my_ip']})
	return "OK"


@app.route('/join',methods = ['POST', 'GET'])
def join():
	if request.method == 'POST':
		node_ip = request.form['ip']
		node_id = int(request.form['id'])
		if in_my_area(node_id, info['prev_id'], info['my_id']):
			url = 'http://'+node_ip+'/join'
			obj = {'prev_id':info['prev_id'], 'next_id':info['my_id'], 'prev_ip':info['prev_ip'], 'next_ip':info['my_ip']}
			for key, value in items.items():
				if in_my_area(int(key), info['prev_id'], node_id):
					items.pop(int(key))
					obj.update({int(key):value})
			x = requests.get(url, params = obj)
		else:
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
			items.update({int(key):value})
		url_prev = "http://"+info['prev_ip']+"/neig"
		url_next = "http://"+info['next_ip']+"/neig"
		x = requests.post(url_prev, data = {'next_id': info['my_id'], 'next_ip':info['my_ip']})
		y = requests.post(url_next, data = {'prev_id': info['my_id'], 'prev_ip':info['my_ip']})
	return "OK"


@app.route('/depart',methods = ['POST', 'GET'])
def depart():
	if request.method == 'POST':
		url_prev = "http://"+info['prev_ip']+"/neig"
		url_next = "http://"+info['next_ip']+"/neig"
		url_next_get = "http://"+info['next_ip']+"/depart"
		x = requests.post(url_prev, data = {'next_id': info['next_id'], 'next_ip':info['next_ip']})
		y = requests.post(url_next, data = {'prev_id': info['prev_id'], 'prev_ip':info['prev_ip']})
		w = requests.get(url_next_get, params = items)
		items.clear()
		print "I departed from the chord"
		
		if replication_factor != 1:
			url_next_replica = "http://"+info['next_ip']+"/replica"
			obj = {'k':replication_factor, 'k2':replication_factor, 'src_ip':info['next_ip'], 'replica_id':-1, 'join_ip':info['next_ip']}
			x = requests.post(url_next_replica, data = obj)
	if request.method == 'GET':
		for key, value in request.args.to_dict(flat=False).items():
			items.update({int(key):value})
		#print "I updated my key, value pairs"
	return "OK"


@app.route('/neig',methods = ['POST'])
def neighbor():
	if request.form.keys()[0] == 'next_id':
		info['next_id'] = int(request.form['next_id'])
		info['next_ip'] = request.form['next_ip']
	else:
		info['prev_id'] = int(request.form['prev_id'])
		info['prev_ip'] = request.form['prev_ip']
	print ("My new (prev, next) neighbors are: ({}, {})".format(info['prev_id'],info['next_id']))
	return "OK"


@app.route('/write',methods = ['POST', 'GET'])
def write():
	if request.method == 'POST':
		action = request.form['action']
		key = int(request.form['key'])
		value = request.form.to_dict(flat=False)['value']
		src_ip = request.form['src_ip']
		if action == 'query' and linearizability == False and replication_factor != 1:
			last_id = info['my_replica_id']
		else:
			last_id = info['prev_id']

		if in_my_area(key, last_id, info['my_id'])==True:
			if action == 'insert': 
				items[key] = value
				#print("Store (key, value): ({}->{}, {})".format(value[1],key,value[0]))
			elif action == 'delete':
				value = items.pop(key, ['No key found',value[1]])
			else:
				value = items.get(key, ['No key found',value[1]])

			if linearizability and replication_factor != 1:
				if info['next_ip'] != info['my_ip']:
					url_next = "http://"+info['next_ip']+"/write"
					obj = {'action':action, 'key':key, 'value':value, 'src_ip':src_ip, 'k':replication_factor, 'rm':info['my_id']}
					x = requests.get(url_next, params = obj)
					return x.text
			elif not linearizability:
				if src_ip != info['my_ip']:
					url_src = "http://"+src_ip+"/write"
					obj = {'action':action, 'key':key, 'value':value, 'src_ip':src_ip, 'return_id':info['my_id']}
					x = requests.get(url_src, params = obj)
				else:
					print("{} (key, value): ({}->{}, {}) successful from node {}".format(action,value[1],key,value[0],info['my_id']))
				
				if info['next_ip'] != info['my_ip'] and replication_factor!=1 and action != 'query':
					def replicate(**kwargs):
						from time import sleep
						sleep(0.0001)
						y = requests.get(kwargs['url'], params = kwargs['obj'])
						
					url_next = "http://"+info['next_ip']+"/write"
					obj = {'action':action, 'key':key, 'value':value, 'src_ip':src_ip, 'k':replication_factor, 'rm':info['my_id']}
					thread = threading.Thread(target=replicate, kwargs={'url': url_next, 'obj': obj})
					thread.start()
				
				if src_ip != info['my_ip']:
					return x.text
				else:
					return "{} (key, value): ({}->{}, {}) successful from node {}".format(action,value[1],key,value[0],info['my_id'])
 
		else:
			if linearizability and action == 'query' and in_my_area(key, info['my_replica_id'], info['my_replica_mn']) == True:
				value = items.get(key, ['No key found',value[1]])
				print("{} (key, value): ({}->{}, {}) successful from node {}".format(action,value[1],key,value[0],info['my_id']))
				return "{} (key, value): ({}->{}, {}) successful from node {}".format(action,value[1],key,value[0],info['my_id'])
			else:
				url_next = "http://"+info['next_ip']+"/write"
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
			return_id = request.args['return_id']
			print("{} (key, value): ({}->{}, {}) successful from node {}".format(action,value[1],key,value[0],return_id))
			return "{} (key, value): ({}->{}, {}) successful from node {}".format(action,value[1],key,value[0],return_id)
		else:
			if action == 'insert':
				items[key]=value
			elif action == 'delete':
				value = items.pop(key, ['No key found',value[1]])
			else:
				value = items.get(key, ['No key found',value[1]])
			k = int(request.args['k']) - 1
			rm = int(request.args['rm'])
			if k==1 or rm==info['next_id']:
				if linearizability == True:
					url_src = "http://"+src_ip+"/write"
					obj = {'action':action, 'key':key, 'value':value, 'src_ip':src_ip, 'return_id':info['my_id']}
					x = requests.get(url_src, params = obj)
					return x.text
			else:
				url_next = "http://"+info['next_ip']+"/write"
				obj = {'action':action, 'key':key, 'value':value,'src_ip':src_ip, 'k':k, 'rm':rm}
				x = requests.get(url_next, params = obj)
				return x.text
	return "OK"
	

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


@app.route('/overlay',methods = ['POST'])
def overlay():
	id_list = request.form.to_dict(flat=False)['id_list']
	if str(info['my_id']) not in id_list:
		id_list.append(info['my_id'])
		url_next = "http://"+info['next_ip']+"/overlay"
		x = requests.post(url_next, data = {'id_list': id_list})
		return x.text
	else:
		id_list.remove('None')
		print ("The topology of the chord is: {}".format(map(int, id_list)))
		return "The topology of the chord is: {}".format(map(int, id_list))
	return "OK"


@app.route('/replica',methods = ['POST', 'GET'])
def replica():
	if request.method == 'POST':
		k = int(request.form['k']) - 1
		k2 = int(request.form['k2'])
		src_ip = request.form['src_ip']
		replica_id = int(request.form['replica_id'])
		join_ip = request.form['join_ip']
		
		if info['my_ip'] == src_ip and replica_id !=-1:
			info['my_replica_id'] = replica_id
			info['my_replica_mn'] = int(request.form['replica_mn'])
			#print "My replica id, mn is:", info['my_replica_id'], info['my_replica_mn'] 
			
			for key, value in items.items():
				if in_my_area(key, info['my_replica_id'], info['my_id'])==False:
					items.pop(key, 'No key found')

			if k2!=0 and info['next_ip']!=join_ip:
				url_next = "http://"+info['next_ip']+"/replica"
				obj = {'k':replication_factor, 'src_ip':info['next_ip'], 'replica_id':-1, 'k2':k2-1, 'join_ip':join_ip}
				x = requests.post(url_next, data = obj)
		elif info['my_ip'] == src_ip and replica_id == -1 and info['prev_ip']!=info['my_ip']:
			url_prev = "http://"+info['prev_ip']+"/replica"
			obj = {'k': k, 'src_ip':src_ip, 'replica_id':-1, 'k2':k2, 'join_ip':join_ip}
			x = requests.post(url_prev, data = obj)

		elif info['prev_ip'] == src_ip or k==0:
			sent = {}
			for key, value in items.items():
				if in_my_area(key, info['prev_id'], info['my_id'])==True:
					sent[key] = value
			url_src = "http://"+src_ip+"/replica"
			x = requests.get(url_src, params = sent)

			obj = {'k': k, 'src_ip':src_ip, 'replica_id':info['prev_id'], 'replica_mn':info['my_id'], 'k2':k2, 'join_ip':join_ip}
			x = requests.post(url_src, data = obj)
		else:
			sent = {}
			for key, value in items.items():
				if in_my_area(key, info['prev_id'], info['my_id'])==True:
					sent[key] = value
			url_src = "http://"+src_ip+"/replica"
			x = requests.get(url_src, params = sent)

			url_prev = "http://"+info['prev_ip']+"/replica"
			obj = {'k': k, 'src_ip':src_ip, 'replica_id':-1, 'k2':k2, 'join_ip':join_ip}
			x = requests.post(url_prev, data = obj)
	if request.method == 'GET':
		for key, value in request.args.to_dict(flat=False).items():
			items.update({int(key):value})
	return "OK"


if __name__ == '__main__':
	app.run(host='192.168.1.2', port=5001, debug = False)
