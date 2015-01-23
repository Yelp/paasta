class MarathonError(Exception):
    pass


class MarathonHttpError(MarathonError):

    def __init__(self, response):
        """
        :param :class:`requests.Response` response: HTTP response
        """
        content = response.json()
        self.status_code = response.status_code
        self.error_message  = content['message']
        super(MarathonHttpError, self).__init__(self.__str__() )

    def __repr__(self):
        return 'MarathonHttpError: HTTP %s returned with message, "%s"' % \
               (self.status_code, self.error_message)

    def __str__(self):
        return self.__repr__()


class NotFoundError(MarathonHttpError):
    pass


class InternalServerError(MarathonHttpError):
    pass


class InvalidChoiceError(MarathonError):

    def __init__(self, param, value, options):
        super(InvalidChoiceError, self).__init__(
            'Invalid choice "{value}" for param "{param}". Must be one of {options}'.format(
                param=param, value=value, options=options
            )
        )