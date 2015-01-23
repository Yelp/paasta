import os, os.path, urllib, subprocess, shutil, re

python_versions = ['2.4.6', '2.5.6', '2.6.8', '2.7.5']
libcurl_versions = ['7.19.0', '7.32.0']

python_meta = {
    '2.5.6': {
        'patches': ['python25.patch'],
    },
}

root = os.path.abspath(os.path.dirname(__file__))

class in_dir:
    def __init__(self, dir):
        self.dir = dir
    
    def __enter__(self):
        self.oldwd = os.getcwd()
        os.chdir(self.dir)
    
    def __exit__(self, type, value, traceback):
        os.chdir(self.oldwd)

def fetch(url, archive):
    if not os.path.exists(archive):
        print "Fetching %s" % url
        io = urllib.urlopen(url)
        with open('.tmp.%s' % archive, 'w') as f:
            while True:
                chunk = io.read(65536)
                if len(chunk) == 0:
                    break
                f.write(chunk)
        os.rename('.tmp.%s' % archive, archive)

def build(archive, dir, prefix, meta=None):
    if not os.path.exists(dir):
        print "Building %s" % archive
        subprocess.check_call(['tar', 'xf', archive])
        with in_dir(dir):
            if meta and 'patches' in meta:
                for patch in meta['patches']:
                    patch_path = os.path.join(root, 'matrix', patch)
                    subprocess.check_call(['patch', '-p1', '-i', patch_path])
            subprocess.check_call(['./configure', '--prefix=%s' % prefix])
            subprocess.check_call(['make'])
            subprocess.check_call(['make', 'install'])

def patch_pycurl_for_24():
    # change relative imports to old syntax as python 2.4 does not
    # support relative imports
    for root, dirs, files in os.walk('tests'):
        for file in files:
            if file.endswith('.py'):
                path = os.path.join(root, file)
                with open(path, 'rb') as f:
                    contents = f.read()
                contents = re.compile(r'^(\s*)from \. import', re.M).sub(r'\1import', contents)
                contents = re.compile(r'^(\s*)from \.(\w+) import', re.M).sub(r'\1from \2 import', contents)
                with open(path, 'wb') as f:
                    f.write(contents)

def run_matrix():
    for python_version in python_versions:
        url = 'http://www.python.org/ftp/python/%s/Python-%s.tgz' % (python_version, python_version)
        archive = os.path.basename(url)
        fetch(url, archive)
        
        dir = archive.replace('.tgz', '')
        prefix = os.path.abspath('i/%s' % dir)
        build(archive, dir, prefix, meta=python_meta.get(python_version))

    for libcurl_version in libcurl_versions:
        url = 'http://curl.haxx.se/download/curl-%s.tar.gz' % libcurl_version
        archive = os.path.basename(url)
        fetch(url, archive)
        
        dir = archive.replace('.tar.gz', '')
        prefix = os.path.abspath('i/%s' % dir)
        build(archive, dir, prefix)

    fetch('https://raw.github.com/pypa/virtualenv/1.7/virtualenv.py', 'virtualenv-1.7.py')

    if not os.path.exists('venv'):
        os.mkdir('venv')

    for python_version in python_versions:
        python_version_pieces = map(int, python_version.split('.')[:2])
        for libcurl_version in libcurl_versions:
            python_prefix = os.path.abspath('i/Python-%s' % python_version)
            libcurl_prefix = os.path.abspath('i/curl-%s' % libcurl_version)
            venv = os.path.abspath('venv/Python-%s-curl-%s' % (python_version, libcurl_version))
            if os.path.exists(venv):
                shutil.rmtree(venv)
            if python_version_pieces >= [2, 5]:
                subprocess.check_call(['virtualenv', venv, '-p', '%s/bin/python' % python_prefix, '--no-site-packages'])
            else:
                subprocess.check_call(['python', 'virtualenv-1.7.py', venv, '-p', '%s/bin/python' % python_prefix, '--no-site-packages'])
            curl_config_path = os.path.join(libcurl_prefix, 'bin/curl-config')
            curl_lib_path = os.path.join(libcurl_prefix, 'lib')
            with in_dir('pycurl'):
                extra_patches = []
                extra_env = []
                if python_version_pieces >= [2, 6]:
                    deps_cmd = 'pip install -r requirements-dev.txt'
                elif python_version_pieces >= [2, 5]:
                    deps_cmd = 'pip install -r requirements-dev-2.5.txt'
                else:
                    deps_cmd = 'easy_install nose simplejson==2.1.0'
                    patch_pycurl_for_24()
                    extra_patches.append('(cd %s/lib/python2.4/site-packages/nose-* && patch -p1) <tests/matrix/nose-1.3.0-python24.patch' % venv)
                    extra_env.append('PYCURL_STANDALONE_APP=yes')
                extra_patches = ' && '.join(extra_patches)
                extra_env = ' '.join(extra_env)
                cmd = '''
                    make clean &&
                    . %(venv)s/bin/activate &&
                    %(deps_cmd)s && %(extra_patches)s
                    python -V &&
                    LD_LIBRARY_PATH=%(curl_lib_path)s PYCURL_CURL_CONFIG=%(curl_config_path)s %(extra_env)s make test
                ''' % dict(
                    venv=venv,
                    deps_cmd=deps_cmd,
                    extra_patches=extra_patches,
                    curl_lib_path=curl_lib_path,
                    curl_config_path=curl_config_path,
                    extra_env=extra_env
                )
                print(cmd)
                subprocess.check_call(cmd, shell=True)

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'patch-24':
        patch_pycurl_for_24()
    else:
        run_matrix()
