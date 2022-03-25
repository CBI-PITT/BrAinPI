# -*- coding: utf-8 -*-
"""
Created on Wed Mar 23 20:44:29 2022

@author: alpha
"""

'''
An attempt at setting up windows domain authentication for CBI users
https://soshace.com/integrate-ldap-authentication-with-flask/
https://ldap3.readthedocs.io/en/latest/connection.html


https://www.digitalocean.com/community/tutorials/how-to-add-authentication-to-your-app-with-flask-login

'''

from flask import render_template

def setup_auth(app):

    @app.route('/login')
    def login():
        return render_template('login.html')
    
    @app.route('/signup')
    def signup():
        return render_template('signup.html')
    
    @app.route('/profile')
    def profile():
        return render_template('profile.html')
    
    @app.route('/logout')
    def logout():
        return 'Logout'
    
    return app









def domain_auth(user_name,password,domain_server=r"ldap://cbilab.pitt.edu:389",domain="cbilab"):
    '''
    Attempts a simple verification of user account on windows domain server
    Return True if auth succeeded
    Return False if auth was rejected
    Return None if an error occured
    '''
 
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
        return None
     
