from django.http import JsonResponse
import json
import sqlite3
import subprocess

def nodes(request):
    conn = sqlite3.connect('file:../connections.db?mode=rw', uri=True)
    c = conn.cursor()
    c.execute("""SELECT * FROM chord;""")
    dbnodes = c.fetchall()
    mp=dict()
    for i in range(len(dbnodes)):
        mp[dbnodes[i][0]] = i
    
    nodes = [{"id": i, "name": dbnodes[i][0], 'fixed':False} for i in range(len(dbnodes))]
    links = [{'source': i, 'target': mp[dbnodes[i][1]]} for i in range(len(dbnodes))]
    data = {
            "name": "hello world",
            'nodes': nodes,
            'links': links,
        }
    
    return JsonResponse(data)
def node_data(request, key):
    conn = sqlite3.connect('file:../connections.db?mode=rw', uri=True)
    c = conn.cursor()
    c.execute("""SELECT * FROM chord;""")
    dbnodes = c.fetchone()
    data = {
        'value': subprocess.check_output(f"go run ../example/ret.go {dbnodes[0]} {key}", shell=True).decode("utf-8")
    }
    return JsonResponse(data)
def node_pdata(request, key, value):
    conn = sqlite3.connect('file:../connections.db?mode=rw', uri=True)

    c = conn.cursor()
    c.execute("""SELECT * FROM chord;""")
    dbnodes = c.fetchone()
    data = {
        'value': subprocess.check_output(f"go run ../example/save.go {dbnodes[0]} {key} {value}", shell=True).decode("utf-8")
    }
    return JsonResponse(data)