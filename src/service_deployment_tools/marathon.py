def get_config():
    # TODO read from a config file
    config = {
        'cluster': 'devc',
        'url': 'http://dev5-devc.dev.yelpcorp.com:5052',
        'user': 'admin',
        'pass': '***REMOVED***',
        'docker_registry': 'docker-dev.yelpcorp.com',
        'docker_options': ['-v', '/nail/etc/:/nail/etc/:ro'],
    }
    return config



def is_leader(marathon_config):
    
    #http://dev15-devc:5052/v1/debug/leaderUrl
    



