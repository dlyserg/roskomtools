#!/usr/bin/python3

from bottle import Bottle, run

app = Bottle()

@app.route('/')
def home_page():
    return "Hello World!"

if __name__ == '__main__':
	run(app, host = 'localhost', port = 8080)
