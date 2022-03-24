# -*- coding: utf-8 -*-
"""
Created on Wed Mar 23 20:44:29 2022

@author: alpha
"""

'''
An attempt at setting up windows domain authentication for CBI users
https://soshace.com/integrate-ldap-authentication-with-flask/
https://ldap3.readthedocs.io/en/latest/connection.html
'''

from ldap3 import Server, Connection, ALL, NTLM

def domain_auth(user_name,password, domain_server=r"ldap://cbilab.pitt.edu:389",domain="cbilab"):
 
    # user
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
    except:
        print('An error occured while connecting to the domain server')
     