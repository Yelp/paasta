from mock import Mock
import testify as T

import create_service


class SrvReaderWriterTestCase(T.TestCase):
    """I bailed out of this test, but I'll leave this here for now as an
    example of how to interact with the Srv* classes."""
    @T.setup
    def init_service(self):
        paths = create_service.paths.SrvPathBuilder("fake_srvpathbuilder")
        self.srw = create_service.SrvReaderWriter(paths)

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
