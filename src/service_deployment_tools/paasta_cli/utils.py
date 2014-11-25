import os
import glob


def load_method(module_name, method_name):
    """
    Return a function given a module and method name
    :param module_name: a string
    :param method_name: a string
    :return: a function
    """
    module = __import__(module_name, fromlist=[method_name])
    method = getattr(module, method_name)
    return method


def file_names_in_dir(directory):
    """
    Read and return the files names in the directory
    :return: a list of strings such as ['list','check'] that correspond to the
    files in the directory without their extensions
    """
    dir_path = os.path.dirname(os.path.abspath(directory.__file__))
    path = os.path.join(dir_path, '*.py')

    for file_name in glob.glob(path):
        basename = os.path.basename(file_name)
        root, _ = os.path.splitext(basename)
        if root == '__init__':
            continue
        yield root


def is_file_in_dir(file_name, path):
    """
    Recursively search path for file_name
    :param file_name: a string of a file name to find
    :param path: a string path
    :return: a boolean
    """
    for root, dirnames, filenames in os.walk(path):
        for filename in filenames:
            if filename == file_name:
                return True
    return False


def check_mark():
    return u'\u2713'.encode('utf-8')


def x_mark():
    return u'\u2717'.encode('utf-8')
