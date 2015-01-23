from unittest import TestCase


class ExceptionsTestCase(TestCase):

    def _get(self):
        from kazoo import exceptions
        return exceptions

    def test_backwards_alias(self):
        module = self._get()
        self.assertTrue(getattr(module, 'NoNodeException'))
        self.assertTrue(module.NoNodeException, module.NoNodeError)

    def test_exceptions_code(self):
        module = self._get()
        exc_8 = module.EXCEPTIONS[-8]
        self.assertTrue(isinstance(exc_8(), module.BadArgumentsError))

    def test_invalid_code(self):
        module = self._get()
        self.assertRaises(RuntimeError, module.EXCEPTIONS.__getitem__, 666)
