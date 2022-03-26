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

from flask import render_template, request, flash, redirect, url_for
from flask_login import LoginManager, login_user, UserMixin
import time

class User(UserMixin):
    def __init__(self,username):
        self.id = username.encode()

def setup_auth(app):
    
    app.secret_key = 'this is my secret key'
    
    login_manager = LoginManager()
    login_manager.login_view = 'app.login'
    login_manager.init_app(app)
    
    @login_manager.user_loader
    def load_user(user_id):
        return user_id.encode()

    @app.route('/login')
    def login():
        return render_template('login.html')
    
    @app.route('/login', methods=['POST'])
    def login_post():
        
        remote_ip = request.remote_addr #<--Potential to log attempts and restrict number of tries
        username = request.form.get('username')
        password = request.form.get('password')
        remember = True if request.form.get('remember') else False
        
        ## Check user against domain server
        user = False # Default to False for security
        user = domain_auth(username,password) # check if the user actually exists
        if user != True:
            user = False
        
        user = True ####  TESTING ONLY  ####
    
        if user == False:
            flash('Please check your login details and try again.')
            return redirect(url_for('login')) # if the user doesn't exist or password is wrong, reload the page
    
        # if the above check passes, then we know the user has the right credentials
        login_user(username, remember=remember)  
        # login_user(User(username), remember=remember)  
        return redirect(url_for('profile'))
    
    @app.route('/signup')
    def signup():
        return render_template('signup.html')
    
    @app.route('/profile')
    def profile():
        return render_template('profile.html')
    
    @app.route('/logout')
    def logout():
        return 'Logout'
    
    return app,login_manager









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
     
