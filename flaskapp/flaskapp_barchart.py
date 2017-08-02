from flask import Flask, request, render_template, session, redirect, url_for, make_response
import os, time

app = Flask(__name__)

@app.route('/', methods=['POST','GET'])
def home():
	return render_template('index_mapred.html')

@app.route('/input', methods=['POST','GET'])
def input():		
	if request.method == 'POST':
		uip1 = request.form['uip1']
		uip2 = request.form['uip2']
		#uip3 = request.form['uip3']
	string = uip1+"_"+uip2 #+"_"+uip3
	
	fname = "/home/ubuntu/user_output/uop.txt"
	last_modified_op = os.stat(fname).st_mtime
	
	uip = open("/home/ubuntu/user_input/uip.txt","w")
	uip.write(string)
	uip.close()
	
	while last_modified_op == os.stat(fname).st_mtime:
		time.sleep(2)

	uop = open("/home/ubuntu/user_output/uop.txt","r")
	exec_time = uop.readline()
	
	list = ''
	content = ''
	
	fname = "/home/ubuntu/user_output/uop.csv"
	with open(fname,"rb") as f:
		for line in f:
			content = content +'\n'+ line
	list += content
	list += "<br><br>Execution Time = " + str(exec_time)
	
	return '''<html><head><title>Output</title><link rel="stylesheet" href="static/stylesheets/style.css"></head><body>'''+list+'''</body></html>'''
		
if __name__ == '__main__':
	app.run(debug=True)