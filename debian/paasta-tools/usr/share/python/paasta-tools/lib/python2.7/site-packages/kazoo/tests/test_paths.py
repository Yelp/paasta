import sys
from unittest import TestCase

from kazoo.protocol import paths


if sys.version_info > (3, ):  # pragma: nocover
    def u(s):
        return s
else:  # pragma: nocover
    def u(s):
        return unicode(s, "unicode_escape")


class NormPathTestCase(TestCase):

    def test_normpath(self):
        self.assertEqual(paths.normpath('/a/b'), '/a/b')

    def test_normpath_empty(self):
        self.assertEqual(paths.normpath(''), '')

    def test_normpath_unicode(self):
        self.assertEqual(paths.normpath(u('/\xe4/b')), u('/\xe4/b'))

    def test_normpath_dots(self):
        self.assertEqual(paths.normpath('/a./b../c'), '/a./b../c')

    def test_normpath_slash(self):
        self.assertEqual(paths.normpath('/'), '/')

    def test_normpath_multiple_slashes(self):
        self.assertEqual(paths.normpath('//'), '/')
        self.assertEqual(paths.normpath('//a/b'), '/a/b')
        self.assertEqual(paths.normpath('/a//b//'), '/a/b')
        self.assertEqual(paths.normpath('//a////b///c/'), '/a/b/c')

    def test_normpath_relative(self):
        self.assertRaises(ValueError, paths.normpath, './a/b')
        self.assertRaises(ValueError, paths.normpath, '/a/../b')


class JoinTestCase(TestCase):

    def test_join(self):
        self.assertEqual(paths.join('/a'), '/a')
        self.assertEqual(paths.join('/a', 'b/'), '/a/b/')
        self.assertEqual(paths.join('/a', 'b', 'c'), '/a/b/c')

    def test_join_empty(self):
        self.assertEqual(paths.join(''), '')
        self.assertEqual(paths.join('', 'a', 'b'), 'a/b')
        self.assertEqual(paths.join('/a', '', 'b/', 'c'), '/a/b/c')

    def test_join_absolute(self):
        self.assertEqual(paths.join('/a/b', '/c'), '/c')


class IsAbsTestCase(TestCase):

    def test_isabs(self):
        self.assertTrue(paths.isabs('/'))
        self.assertTrue(paths.isabs('/a'))
        self.assertTrue(paths.isabs('/a//b/c'))
        self.assertTrue(paths.isabs('//a/b'))

    def test_isabs_false(self):
        self.assertFalse(paths.isabs(''))
        self.assertFalse(paths.isabs('a/'))
        self.assertFalse(paths.isabs('a/../'))


class BaseNameTestCase(TestCase):

    def test_basename(self):
        self.assertEquals(paths.basename(''), '')
        self.assertEquals(paths.basename('/'), '')
        self.assertEquals(paths.basename('//a'), 'a')
        self.assertEquals(paths.basename('//a/'), '')
        self.assertEquals(paths.basename('/a/b.//c..'), 'c..')


class PrefixRootTestCase(TestCase):

    def test_prefix_root(self):
        self.assertEquals(paths._prefix_root('/a/', 'b/c'), '/a/b/c')
        self.assertEquals(paths._prefix_root('/a/b', 'c/d'), '/a/b/c/d')
        self.assertEquals(paths._prefix_root('/a', '/b/c'), '/a/b/c')
        self.assertEquals(paths._prefix_root('/a', '//b/c.'), '/a/b/c.')


class NormRootTestCase(TestCase):

    def test_norm_root(self):
        self.assertEquals(paths._norm_root(''), '/')
        self.assertEquals(paths._norm_root('/'), '/')
        self.assertEquals(paths._norm_root('//a'), '/a')
        self.assertEquals(paths._norm_root('//a./b'), '/a./b')
