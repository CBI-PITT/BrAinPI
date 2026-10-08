# -*- coding: utf-8 -*-
"""Flask-Login setup with optional FreeIPA and Windows-domain LDAP authentication."""

'''
Windows domain auth:
    https://soshace.com/integrate-ldap-authentication-with-flask/
    https://ldap3.readthedocs.io/en/latest/connection.html

Flask login:
    https://www.digitalocean.com/community/tutorials/how-to-add-authentication-to-your-app-with-flask-login
    https://blog.miguelgrinberg.com/post/the-flask-mega-tutorial-part-v-user-logins

Flask limiter:
    https://flask-limiter.readthedocs.io/en/latest/
'''

from flask import (render_template, 
                   request, 
                   flash, 
                   redirect, 
                   url_for,
                   abort)

import os
import traceback

from flask_login import (LoginManager, 
                         login_user, 
                         UserMixin, 
                         current_user,
                         login_required,
                         logout_user)

def user_info():
    """
    Retrieve information about the currently logged-in user.

    Returns:
        dict: A dictionary containing the following keys:
              - 'is_authenticated': Whether the user is authenticated (bool).
              - 'id': The user's ID if authenticated, otherwise None.
    """
    return {'is_authenticated':current_user.is_authenticated, 'id':current_user.id if current_user.is_authenticated else None}

class User(UserMixin):
    """Minimal Flask-Login user identified by a username."""

    def __init__(self,username):
        self.id = username

def setup_auth(app):
    """
    Set up user authentication and session management for the Flask application.

    Configures:
    - Secure session cookies.
    - Login manager for managing user sessions.
    - Rate limiter to prevent brute force login attempts.
    - Routes for login, logout, and profile pages.

    Args:
        app (Flask): The Flask application instance.

    Returns:
        tuple: A tuple containing the modified Flask app and the LoginManager instance.
    """
    ## This import must remain here else circular import error
    from brain_api_main import settings
    from flask_limiter import Limiter
    from flask_limiter.util import get_remote_address
    
    app.config['SESSION_COOKIE_SECURE'] = True
    
    ## KEY FOR TESTING ONLY ##
    app.secret_key = settings.get('auth','secret_key')
    
    ############################################################
    # Configure login manager
    ############################################################
    login_manager = LoginManager(app)
    login_manager.login_view = 'login'
    # login_manager.init_app(app)
    
    @login_manager.user_loader
    def load_user(user_id):
        """Reconstruct a session user from its serialized identifier."""
        return User(user_id)
    
    
    ##########################################################
    # Configure login rate limiter
    ##########################################################
    
    # limiter = Limiter(app, key_func=get_remote_address)
    ##TODO Rate limiter needs to be in place where all workers can access
    #https://flask-limiter.readthedocs.io/en/stable/configuration.html#RATELIMIT_STORAGE_URI
    limiter = Limiter(get_remote_address,app=app,storage_uri="memory://")
    
    @app.errorhandler(429)
    def ratelimit_handler(e):
        """Redirect rate-limited login attempts back to the login page."""
        flash("Login ratelimit exceeded %s" % e.description)
        return redirect(url_for('login'))
    
    
    ##########################################################
    # Configure login routes
    ##########################################################


    @app.route('/login')
    def login():
        """Render the login form or redirect an authenticated user."""
        if current_user.is_authenticated:
            flash('''
                  You are already signed in as user {}.
                  If this is not you, please logout
                  '''.format(current_user.id))
            return redirect(url_for('profile'))
        return render_template('login.html',
                               user=user_info(),
                               app_name=settings.get('app','name'),
                               page_name='Login',
                               gtag=settings.get('GA4','gtag'))
    
    

    @app.route('/login', methods=['POST'])
    @limiter.limit(settings.get('auth','login_limit'))
    def login_post():
        """Authenticate submitted credentials and create a login session."""
        
        remote_ip = request.remote_addr #<--Potential to log attempts and restrict number of tries
        username = request.form.get('username')
        password = request.form.get('password')
        ## Check user against domain server
        user = False # Default to False for security
        if 'auth' in settings and not settings.getboolean('auth','bypass_auth'):
            # FreeIPA is attempted first; rejected credentials and connection
            # failures fall back to the configured Windows-domain server.
            ipa_enabled = settings.getboolean('auth', 'ipa_auth', fallback=False)
            if ipa_enabled:
                ipa_server = (settings.get('auth', 'ipa_server', fallback='') or '').strip()
                ipa_server = ipa_server or 'ipa.cbiserver.pitt.edu'
                ipa_tls = settings.getboolean('auth', 'ipa_use_tls', fallback=True)
                ipa_ca = (settings.get('auth', 'ipa_ca_file', fallback='') or '').strip() or None
                try:
                    user = ipa_authenticate(username, password,
                                            server=ipa_server,
                                            use_tls=ipa_tls,
                                            ca_file=ipa_ca)
                except ConnectionError:
                    app.logger.warning('FreeIPA connection failed; trying domain authentication',
                                       exc_info=True)
                    user = False

            if user is not True:
                # With allow_no_value=True, valueless INI keys return None.
                domain_server = (settings.get('auth', 'domain_server', fallback='') or '').strip()
                domain_port = (settings.get('auth', 'domain_port', fallback='') or '').strip()
                domain_name = (settings.get('auth', 'domain_name', fallback='') or '').strip()

                # IPA-only deployments must not need AD settings. If IPA failed,
                # reject the login rather than attempting an invalid LDAP URL.
                if not all((domain_server, domain_port, domain_name)):
                    if ipa_enabled:
                        flash('Your credentials could not be verified. Please try again.')
                    else:
                        app.logger.warning('LDAP login requested, but LDAP is not configured')
                        flash('Authentication is not configured for this deployment.')
                    return redirect(url_for('login'))

                user = domain_auth(username,
                                   password,
                                   domain_server=r"ldap://{}:{}".format(
                                       domain_server, domain_port),
                                   domain=domain_name)
            
            if user == False:
                flash('''Your credentials are not valid''')
                return redirect(url_for('login'))
            if user is None:
                flash('''An error occured during login: please try again.
                      If the error persists, please report the problem''')
                return redirect(url_for('login'))
        else:
            user = True
    
        if user == False:
            flash('Please check your login details and try again.')
            return redirect(url_for('login')) # if the user doesn't exist or password is wrong, reload the page
    
        # if the above check passes, then we know the user has the right credentials
        # Keep the authenticated identity across browser restarts until the
        # remember cookie expires or the user explicitly logs out.
        login_user(load_user(username), remember=True)
        print('Got to here')
        return redirect(url_for('browse_fs'))
    
    
    
    
    # @app.route('/signup')
    # def signup():
    #     return render_template('signup.html')
    
    
    
    @app.route('/profile')
    @login_required
    def profile():
        """Render the profile page for the authenticated user."""
        return render_template('profile.html',
                               user=user_info(),
                               app_name=settings.get('app','name'),
                               page_name='Profile',
                               gtag=settings.get('GA4','gtag'))
    
    
    
    @app.route('/logout')
    def logout():
        """Clear the current login session and return to the home page."""
        logout_user()
        return redirect(url_for('home'))
    
    return app,login_manager


def setup_NO_auth(app):
    """
    Set up a Flask application with disabled authentication.

    This function disables all login routes and returns 404 errors for any login-related requests.

    Args:
        app (Flask): The Flask application instance.

    Returns:
        tuple: A tuple containing the modified Flask app and the LoginManager instance.
    """
    ## This import must remain here else circular import error
    from brain_api_main import settings
    from flask_limiter import Limiter
    from flask_limiter.util import get_remote_address

    app.config['SESSION_COOKIE_SECURE'] = True

    ############################################################
    # Configure login manager
    ############################################################
    login_manager = LoginManager(app)
    login_manager.login_view = 'login'

    # login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(user_id):
        """Reject session restoration when authentication is disabled."""
        abort(404)

    ##########################################################
    # Configure login rate limiter
    ##########################################################

    # limiter = Limiter(app, key_func=get_remote_address)
    ##TODO Rate limiter needs to be in place where all workers can access
    # https://flask-limiter.readthedocs.io/en/stable/configuration.html#RATELIMIT_STORAGE_URI
    limiter = Limiter(get_remote_address, app=app, storage_uri="memory://")

    @app.errorhandler(429)
    def ratelimit_handler(e):
        """Hide the login rate-limit route when authentication is disabled."""
        flash("Login ratelimit exceeded %s" % e.description)
        return abort(404)

    ##########################################################
    # Abort all login routes
    ##########################################################


    @app.route('/login')
    def login():
        """Return 404 because authentication routes are disabled."""
        abort(404)

    @app.route('/login', methods=['POST'])
    # @limiter.limit(settings.get('auth', 'login_limit'))
    def login_post():
        """Return 404 because credential submission is disabled."""
        abort(404)

    @app.route('/profile')
    # @login_required
    def profile():
        """Return 404 because profiles are disabled."""
        abort(404)

    @app.route('/logout')
    def logout():
        """Return 404 because logout is disabled."""
        return abort(404)

    return app, login_manager


def domain_auth(user_name,password,domain_server=r"ldap://localhost:389",domain="mydomain"):
    """
    Attempts a simple verification of user account on windows domain server

    Args:
        user_name (str): The username to authenticate.
        password (str): The user's password.
        domain_server (str, optional): The domain server's address (LDAP URI). Defaults to 'ldap://localhost:389'.
        domain (str, optional): The domain name. Defaults to "mydomain".

    Returns:
        bool or None:
            - True if authentication succeeds.
            - False if authentication fails.
            - None if an error occurs during the connection.
    """
 
    from ldap3 import Server, Connection, ALL, NTLM
    
    
    user = domain + "\\" + user_name
    
    server = Server(domain_server, get_info=ALL)
     
    try:
        conn = Connection(server, user=user, password=password, authentication=NTLM)
        
        if conn.bind():
            print('Authentication successful as user: {}'.format(user_name))
            conn.unbind()
            # if conn.closed == True:
            #     return True
            return True
        else:
            print('Authentication Failed')
            return False
    except:
        print('An error occured while connecting to the domain server')
        traceback.print_exc()
        return None


def _ipa_host(server_string):
    '''
    Extract the bare hostname from a server string, e.g.
    'ldaps://ipa.cbiserver.pitt.edu:636' -> 'ipa.cbiserver.pitt.edu'
    '''
    return server_string.replace("ldaps://", "").replace("ldap://", "").split("/")[0].split(":")[0]


def _ipa_base_dn(host):
    '''
    Derive the FreeIPA base DN from the server hostname, e.g.
    'ipa.cbiserver.pitt.edu' -> 'dc=cbiserver,dc=pitt,dc=edu'
    '''
    parts = host.split(".")
    domain = ".".join(parts[1:]) if len(parts) > 1 else parts[0]
    return ",".join("dc=" + label for label in domain.split("."))


def ipa_authenticate(user_name, password, server="ipa.cbiserver.pitt.edu", use_tls=True, ca_file=None):
    '''
    Attempts to authenticate a user against FreeIPA over LDAP.
    The bind itself performs the authentication.

    ca_file is an optional path to the FreeIPA CA bundle (e.g. /etc/ipa/ca.crt)
    used to verify the LDAPS certificate; the system trust store usually does
    NOT contain the FreeIPA CA, so without it TLS verification fails.

    Return True if auth succeeded
    Return False if auth was rejected (invalid credentials or unknown user)
    Raise ConnectionError if the server is unreachable or the LDAP session fails
    '''

    from ldap3 import Server, Connection, ALL
    from ldap3.core.exceptions import (LDAPBindError,
                                       LDAPException,
                                       LDAPInvalidCredentialsResult)

    host = _ipa_host(server)
    bind_dn = "uid={},cn=users,cn=accounts,{}".format(user_name, _ipa_base_dn(host))
    if ca_file and not os.path.exists(ca_file):
        print('[ipa-auth] WARNING: ca_file {} does not exist on this host'.format(ca_file))
    try:
        if ca_file:
            import ssl
            from ldap3 import Tls
            ldap_server = Server(host,
                                 port=636 if use_tls else 389,
                                 use_ssl=use_tls,
                                 tls=Tls(validate=ssl.CERT_REQUIRED, ca_certs_file=ca_file),
                                 get_info=ALL)
        else:
            ldap_server = Server(host, port=636 if use_tls else 389, use_ssl=use_tls, get_info=ALL)
        print('[ipa-auth] binding as {} to {}:{}'.format(bind_dn, host, 636 if use_tls else 389))

        conn = Connection(ldap_server,
                          user=bind_dn,
                          password=password,
                          auto_bind=True,
                          raise_exceptions=True)
    except (LDAPInvalidCredentialsResult, LDAPBindError):
        print('[ipa-auth] FreeIPA rejected credentials for {}'.format(bind_dn))
        return False
    except LDAPException as exc:
        print('[ipa-auth] LDAP session to {} failed: {}'.format(host, exc))
        traceback.print_exc()
        raise ConnectionError('LDAP connection to {} failed: {}'.format(host, exc)) from exc

    try:
        conn.unbind()
    except LDAPException:
        pass
    return True
