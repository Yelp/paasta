def get_config():
    # Required keys (need defaults for some):
    #   docker_registry
    #   docker_image
    #   url
    #   user
    #   pass
    #   cluster
    #   executor
    # TODO read from a config file
    config = {
        'cluster': 'devc',
        'url': 'http://dev5-devc.dev.yelpcorp.com:5052',
        'user': 'admin',
        'pass': '***REMOVED***',
        'docker_registry': 'docker-dev.yelpcorp.com',
        'docker_options': ['-v', '/nail/etc/:/nail/etc/:ro'],
        'executor': '/usr/bin/deimos',
    }
    return config



def is_leader(marathon_config):
    return true
    #http://dev15-devc:5052/v1/debug/leaderUrl




