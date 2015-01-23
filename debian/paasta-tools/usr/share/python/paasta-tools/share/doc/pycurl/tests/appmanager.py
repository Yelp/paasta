import sys, time, os

def noop(*args):
    pass

def setup(*specs):
    if os.environ.get('PYCURL_STANDALONE_APP') and os.environ['PYCURL_STANDALONE_APP'].lower() in ['1', 'yes', 'true']:
        return (noop, noop)
    else:
        return perform_setup(*specs)

def perform_setup(*specs):
    from . import runwsgi
    
    app_specs = []
    for spec in specs:
        app_module = __import__(spec[0], globals(), locals(), ['app'], 1)
        app = getattr(app_module, 'app')
        app_specs.append([app] + list(spec[1:]))
    
    return runwsgi.app_runner_setup(*app_specs)

quit = False

def sigterm_handler(*args):
    global quit
    quit = True

def run_standalone():
    import signal
    
    funcs = []
    
    signal.signal(signal.SIGTERM, sigterm_handler)
    
    funcs.append(setup(('app', 8380)))
    funcs.append(setup(('app', 8381)))
    funcs.append(setup(('app', 8382)))
    funcs.append(setup(('app', 8383, dict(ssl=True))))
    
    for setup_func, teardown_func in funcs:
        setup_func(sys.modules[__name__])
    
    sys.stdout.write("Running, use SIGTERM or SIGINT to stop\n")
    
    try:
        while not quit:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    
    for setup_func, teardown_func in funcs:
        teardown_func(sys.modules[__name__])

if __name__ == '__main__':
    run_standalone()
