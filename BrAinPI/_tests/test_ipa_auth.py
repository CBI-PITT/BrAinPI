"""Standalone tests for BrAinPI's ipa_authenticate + DN helpers.

auth.py imports only flask/flask_login at module level, so these run without
importing the app (brain_api_main pulls diskcache and other heavy deps not in
the peace-flask env). ldap3 is mocked — no network is ever contacted.

Run from the BrAinPI repo root:
    python3 -m pytest BrAinPI/_tests/test_ipa_auth.py
"""

import os
import sys
from unittest import mock

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import auth


def test_ipa_host():
    assert auth._ipa_host('ipa.cbiserver.pitt.edu') == 'ipa.cbiserver.pitt.edu'
    assert auth._ipa_host('ldaps://ipa.cbiserver.pitt.edu:636') == 'ipa.cbiserver.pitt.edu'
    assert auth._ipa_host('ldap://ipa.cbiserver.pitt.edu') == 'ipa.cbiserver.pitt.edu'


def test_ipa_base_dn():
    assert auth._ipa_base_dn('ipa.cbiserver.pitt.edu') == 'dc=cbiserver,dc=pitt,dc=edu'
    assert auth._ipa_base_dn('ipa') == 'dc=ipa'


def test_ipa_authenticate_success(monkeypatch):
    server_cls = mock.MagicMock()
    monkeypatch.setattr('ldap3.Server', server_cls)
    monkeypatch.setattr('ldap3.Connection',
                        mock.MagicMock(return_value=mock.MagicMock()))
    assert auth.ipa_authenticate('test_user', 'secret') is True
    server_kwargs = server_cls.call_args.kwargs
    assert server_kwargs['port'] == 636
    assert server_kwargs['use_ssl'] is True
    assert 'tls' not in server_kwargs


def test_ipa_authenticate_bind_dn(monkeypatch):
    conn_cls = mock.MagicMock(return_value=mock.MagicMock())
    monkeypatch.setattr('ldap3.Server', mock.MagicMock())
    monkeypatch.setattr('ldap3.Connection', conn_cls)
    assert auth.ipa_authenticate('test_user', 'secret') is True
    assert conn_cls.call_args.kwargs['user'] == (
        'uid=test_user,cn=users,cn=accounts,dc=cbiserver,dc=pitt,dc=edu')


def test_ipa_authenticate_invalid_credentials_returns_false(monkeypatch):
    from ldap3.core.exceptions import LDAPInvalidCredentialsResult
    monkeypatch.setattr('ldap3.Server', mock.MagicMock())
    monkeypatch.setattr('ldap3.Connection',
                        mock.MagicMock(side_effect=LDAPInvalidCredentialsResult('bad')))
    assert auth.ipa_authenticate('test_user', 'wrong') is False


def test_ipa_authenticate_unknown_user_returns_false(monkeypatch):
    from ldap3.core.exceptions import LDAPBindError
    monkeypatch.setattr('ldap3.Server', mock.MagicMock())
    monkeypatch.setattr('ldap3.Connection',
                        mock.MagicMock(side_effect=LDAPBindError('no such entry')))
    assert auth.ipa_authenticate('nobody', 'secret') is False


def test_ipa_authenticate_unreachable_raises_connection_error(monkeypatch):
    from ldap3.core.exceptions import LDAPException
    monkeypatch.setattr('ldap3.Server', mock.MagicMock())
    monkeypatch.setattr('ldap3.Connection',
                        mock.MagicMock(side_effect=LDAPException('socket error')))
    with pytest.raises(ConnectionError):
        auth.ipa_authenticate('test_user', 'secret')


def test_ipa_authenticate_plain_ldap(monkeypatch):
    server_cls = mock.MagicMock()
    monkeypatch.setattr('ldap3.Server', server_cls)
    monkeypatch.setattr('ldap3.Connection',
                        mock.MagicMock(return_value=mock.MagicMock()))
    assert auth.ipa_authenticate('test_user', 'secret', use_tls=False) is True
    server_kwargs = server_cls.call_args.kwargs
    assert server_kwargs['port'] == 389
    assert server_kwargs['use_ssl'] is False


def test_ipa_authenticate_ca_file_builds_verifying_tls(monkeypatch, tmp_path):
    ca = tmp_path / 'ipa-ca.crt'
    ca.write_text('CERT')
    server_cls = mock.MagicMock()
    monkeypatch.setattr('ldap3.Server', server_cls)
    monkeypatch.setattr('ldap3.Connection',
                        mock.MagicMock(return_value=mock.MagicMock()))
    assert auth.ipa_authenticate('test_user', 'secret', ca_file=str(ca)) is True
    server_kwargs = server_cls.call_args.kwargs
    assert 'tls' in server_kwargs
    assert server_kwargs['tls'].validate.name == 'CERT_REQUIRED'
    assert server_kwargs['tls'].ca_certs_file == str(ca)


def test_ipa_authenticate_missing_ca_file_warns_and_proceeds(monkeypatch, capsys):
    server_cls = mock.MagicMock()
    tls_cls = mock.MagicMock()
    monkeypatch.setattr('ldap3.Server', server_cls)
    monkeypatch.setattr('ldap3.Tls', tls_cls)
    monkeypatch.setattr('ldap3.Connection',
                        mock.MagicMock(return_value=mock.MagicMock()))
    assert auth.ipa_authenticate('test_user', 'secret',
                                 ca_file='/nonexistent/ipa-ca.crt') is True
    assert 'does not exist' in capsys.readouterr().out
    assert tls_cls.call_args.kwargs['validate'].name == 'CERT_REQUIRED'
    assert tls_cls.call_args.kwargs['ca_certs_file'] == '/nonexistent/ipa-ca.crt'
