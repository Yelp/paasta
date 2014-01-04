from mock import Mock
import testify as T

import create_service


class SrvReaderWriterTestCase(T.TestCase):
    @T.setup
    def init_service(self):
        self.service = create_service.Service("fake_service")
        self.srw = self.service.io

    def test_append_raises_when_file_dne(self):
        self.srw._append()

class ValidateOptionsTestCase(T.TestCase):
    def test_enable_puppet_requires_puppet_root(self):
        parser = Mock()
        options = Mock()
        options.enable_puppet = True
        options.puppet_root = None
        with T.assert_raises(SystemExit):
            create_service.validate_options(parser, options)

    def test_enable_nagios_requires_nagios_root(self):
        parser = Mock()
        options = Mock()
        options.enable_nagios = True
        options.nagios_root = None
        with T.assert_raises(SystemExit):
            create_service.validate_options(parser, options)


if __name__ == "__main__":
    T.run()
