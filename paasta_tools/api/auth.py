import json

import hvac
from hvac.exceptions import Forbidden
from pyramid.authentication import CallbackAuthenticationPolicy
from pyramid.interfaces import IAuthenticationPolicy
from zope.interface import implementer


@implementer(IAuthenticationPolicy)
class VaultAuthAuthenticationPolicy(CallbackAuthenticationPolicy):
    """
    """

    def __init__(self, vault_url, vault_ca):
        self.debug = True
        self.vault_url = vault_url
        self.vault_ca = vault_ca

    def get_vault_client(self, token):
        self.client = hvac.Client(url=self.vault_url, token=token, verify=self.vault_ca)

    def unauthenticated_userid(self, request):
        """
        """
        return 'nobody'

    def remember(self, request, userid, **kw):
        """ A no-op. Vault authentication does not provide a protocol for
        remembering the user. Credentials are sent on every request.
        """
        return []

    def forget(self, request):
        return []

    def check(self, token, request):
        self.get_vault_client(token)
        try:
            token = self.client.lookup_token()
            self.client.renew_token()
            self.debug and self._log(json.dumps(token), 'check', request)
        except Forbidden:
            self.debug and self._log("Vault token expired/invalid", 'check', request)
            return None
        except Exception:
            self.debug and self._log("Failed to communicate with Vault", 'check', request)
            raise
        policies = token['data']['policies']
        username = token['data']['meta']['username']
        return policies + [username]

    def callback(self, username, request):
        # Username arg is ignored, we get it later from resolving the token
        token = request.headers.get('X-Paasta-Token')
        if token:
            return self.check(token, request)
