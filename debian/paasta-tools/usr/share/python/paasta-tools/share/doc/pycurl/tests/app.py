import time as _time
import bottle
try:
    import json
except ImportError:
    import simplejson as json

app = bottle.Bottle()
app.debug = True

@app.route('/success')
def ok():
    return 'success'

@app.route('/status/403')
def forbidden():
    return bottle.HTTPResponse('forbidden', 403)

@app.route('/status/404')
def not_found():
    return bottle.HTTPResponse('not found', 404)

@app.route('/postfields', method='post')
def postfields():
    return json.dumps(dict(bottle.request.forms))

# XXX file is not a bottle FileUpload instance, but FieldStorage?
def convert_file(key, file):
    return {
        'key': key,
        'name': file.name,
        'raw_filename': file.raw_filename,
        'headers': file.headers,
        'content_type': file.content_type,
        'content_length': file.content_length,
        'data': file.read(),
    }

def convert_file(key, file):
    return {
        'name': file.name,
        'filename': file.filename,
        'data': file.file.read(),
    }

@app.route('/files', method='post')
def files():
    files = [convert_file(key, bottle.request.files[key]) for key in bottle.request.files]
    return json.dumps(files)

def pause_writer():
    yield 'part1'
    _time.sleep(0.5)
    yield 'part2'

@app.route('/pause')
def pause():
    return pause_writer()
