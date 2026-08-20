"""RSA/JWT helpers and Flask routes for time-limited BrAinPI URLs."""

# pip install pyjwt
import base64
from datetime import datetime, timedelta
import jwt
from jwt.exceptions import ExpiredSignatureError, InvalidSignatureError
import urllib.request

from flask import (
	abort,
    render_template,
    request,
    flash,
    url_for,
    redirect,
    send_file,
    jsonify
    )

import rsa
from rsa import encrypt, decrypt, newkeys
from base64 import urlsafe_b64encode, urlsafe_b64decode
import os

def generate_key_pairs_save_to_disk(location: str, bit: int = 512):
	"""
    Generate RSA public and private keys, then save them to disk.

    This function creates a new RSA key pair with the specified bit length, and writes the keys 
    to files in the provided directory. The public key is saved in PEM format under 'public_key.pem'
    and the private key is saved under 'private_key.key'.

    Parameters:
        location (str): The directory path where the key files will be saved.
        bit (int, optional): The bit length for the RSA keys. Defaults to 512.
    """
	publicKey, privateKey = rsa.newkeys(bit)
	print('Generating Public and Private RSA Keys')
	if not os.path.exists(location):
		os.makedirs(location)
	with open(os.path.join(location,'public_key.pem'),'wb') as f:
		tmp = publicKey.save_pkcs1()
		f.write(tmp)
		print(tmp)
	with open(os.path.join(location, 'private_key.key'), 'wb') as f:
		tmp = privateKey.save_pkcs1()
		f.write(tmp)
		print(tmp)

def generate_key_pairs_if_not_exist(location: str, bit=512):
	"""
    Generate RSA key pairs if they do not already exist in the given location.

    This function checks if the RSA public and private key files exist in the specified directory.
    If either file is missing, it generates a new key pair and saves them to disk.

    Parameters:
        location (str): The directory where the key files should be located.
        bit (int, optional): The bit length for the RSA keys if they need to be generated. Defaults to 512.
    """
	if not os.path.exists(os.path.join(location,'public_key.pem')) and \
			not os.path.exists(os.path.join(location, 'private_key.key')):
		generate_key_pairs_save_to_disk(location, bit = bit)

def get_rsa_key_pairs(location: str, bit: int = 512):
	"""
    Retrieve RSA public and private key pairs from disk.

    This function ensures that RSA key pairs exist at the provided location 
    (generating them if necessary), then loads and returns the keys.

    Parameters:
        location (str): The directory where the key files are located.
        bit (int, optional): The bit length used for generating keys if they do not exist. Defaults to 512.

    Returns:
        tuple: A tuple (publicKey, privateKey) containing the RSA public and private keys.
    """
	generate_key_pairs_if_not_exist(location, bit = bit)
	with open(os.path.join(location,'public_key.pem'),'rb') as f:
		publicKey = f.read()
		publicKey = rsa.PublicKey.load_pkcs1(publicKey)
	with open(os.path.join(location, 'private_key.key'), 'rb') as f:
		privateKey = f.read()
		privateKey = rsa.PrivateKey.load_pkcs1(privateKey)
	return publicKey, privateKey


# def rsa_encrypt_string_to_url_friendly(before: str, public_key):
# 	encMessage = encrypt(before.encode(), public_key)
# 	return urlsafe_b64encode(encMessage)



def rsa_encrypt_string_to_url_friendly(before: str, public_key):
	"""
    Encrypt a string using RSA and convert the result to a URL-friendly format.

    The encryption is performed in chunks (of 5 characters per chunk) to avoid potential 
    limitations on the RSA encryption size. The resulting encrypted chunks are concatenated 
    and then encoded using URL-safe base64 encoding.

    Parameters:
        before (str): The plaintext string to be encrypted.
        public_key: The RSA public key used for encryption.

    Returns:
        bytes: A URL-safe base64 encoded bytes object representing the encrypted message.
    """
	result = []
	for n in range(0, len(before), 5):
		part = before[n:n + 5]
		result.append(rsa.encrypt(part.encode(), public_key))
	print(f'{result=}')
	# print(len(result), len(result[0]))
	encMessage = b''.join(result)
	print(f'{encMessage=}')
	# encMessage = encrypt(before.encode(), public_key)
	return urlsafe_b64encode(encMessage)

# def rsa_decrypt_from_url_friendly_string(encMessage: str, private_key):
# 	bstring = urlsafe_b64decode(encMessage)
# 	return decrypt(bstring, private_key).decode()

def rsa_decrypt_from_url_friendly_string(encMessage: str, private_key):
	"""
    Decrypt a URL-friendly encoded string that was encrypted using RSA.

    The function decodes the URL-safe base64 encoded string to bytes, 
    then decrypts the message in fixed-size chunks (5 bytes per chunk) and concatenates the results.

    Parameters:
        encMessage (str): The URL-safe base64 encoded encrypted string.
        private_key: The RSA private key used for decryption.

    Returns:
        str: The decrypted plaintext string.
    """
	bstring = urlsafe_b64decode(encMessage)
	result = []
	for n in range(0, len(bstring), 5):
		part = bstring[n:n + 5]
		result.append(rsa.decrypt(part, private_key).decode())
	result = ''.join(result)
	return result


from flask_cors import cross_origin

def get_token(url: str, days: int, SECRET_KEY: str = None):
	"""
    Generate a JWT token with an expiration date based on the provided URL and number of days.

    The token payload contains the URL and an expiration timestamp computed from the current date 
    plus the specified number of days. The token is signed using the provided secret key.

    Parameters:
        url (str): The URL to include in the token payload.
        days (int): The number of days until the token expires.
        SECRET_KEY (str, optional): The secret key used for signing the token.
                                    Must not be None.

    Returns:
        str: The encoded JWT token.

    Raises:
        AssertionError: If SECRET_KEY is None.
    """
	assert SECRET_KEY is not None, 'A secret key must be defined in settings.ini ["auth"] for this to be valid'
	a = datetime.now().date() + timedelta(days=days)
	exp = datetime(a.year ,a.month ,a.day)
	exp = exp.timestamp()
	exp = int(exp)
	encoded_data = jwt.encode(payload={"url": url, "exp": exp} ,key=SECRET_KEY ,algorithm="HS256")
	return encoded_data


def decode_token(token: str, SECRET_KEY: str = None):
	"""
    Decode a JWT token and return its payload.

    This function decodes the token using the provided secret key and handles any exceptions 
    related to token expiration or invalid signature.

    Parameters:
        token (str): The JWT token to decode.
        SECRET_KEY (str, optional): The secret key used for decoding the token.
                                    Must not be None.

    Returns:
        dict or str: The token payload if decoding is successful; otherwise, returns 'expired' 
                     if the token has expired or 'invalid_signature' for an invalid token.

    Raises:
        AssertionError: If SECRET_KEY is None.
    """
	assert SECRET_KEY is not None, 'A secret key must be defined in settings.ini ["auth"] for this to be valid'
	try:
		decoded_data = jwt.decode(jwt=token ,key=SECRET_KEY ,algorithms=["HS256"])
	except ExpiredSignatureError:
		decoded_data = 'expired'
	except InvalidSignatureError: # Wrong SECRET_KEY
		decoded_data = 'invalid_signature'
	return decoded_data










def rsa_encrypt_string(before: str, public_key):
	"""
    Encrypt a string using RSA and encode the result with base85 encoding.

    The encryption is performed in chunks (5 characters per chunk) and the resulting 
    encrypted data is concatenated and encoded using base85 to produce a URL-friendly string.

    Parameters:
        before (str): The plaintext string to encrypt.
        public_key: The RSA public key used for encryption.

    Returns:
        str: The base85 encoded string representing the encrypted message.
    """
	result = []
	for n in range(0, len(before), 5):
		part = before[n:n + 5]
		result.append(rsa.encrypt(part.encode(), public_key))
	# print(len(result), len(result[0]))
	encMessage = b''.join(result)
	# encMessage = encrypt(before.encode(), public_key)
	return base64.b85encode(encMessage).decode()

def rsa_decrypt_string(encMessage: str, private_key):
	"""
    Decrypt a base85 encoded string that was encrypted using RSA.

    The function decodes the encrypted string from base85, then decrypts it in fixed-size chunks
    (5 bytes per chunk) and concatenates the resulting pieces to reconstruct the original plaintext.

    Parameters:
        encMessage (str): The base85 encoded encrypted string.
        private_key: The RSA private key used for decryption.

    Returns:
        str: The decrypted plaintext string.
    """
	bstring = base64.b85decode(encMessage).decode()
	result = []
	for n in range(0, len(bstring), 5):
		part = bstring[n:n + 5]
		result.append(rsa.decrypt(part, private_key).decode())
	result = ''.join(result)
	return result




def setup_tokenized_endpoint(app, config):
	"""
    Set up Flask endpoints for tokenized URL operations.

    This function registers two routes on the Flask app:
      - '/token/<token>/' (with an optional appended path) to decode and handle a tokenized URL request.
      - '/get_token/' to generate and return a token for a given URL and expiration days.
      
    The endpoint for '/token/<token>/' decodes the token, rewrites the incoming request path based 
    on the decoded URL, and forwards the request to the appropriate endpoint. If the token is expired 
    or invalid, an appropriate message is returned.

    Parameters:
        app: The Flask application instance.
        config: A configuration object with settings including:
                - 'auth' settings with a 'secret_key' for signing tokens.
                - 'app' settings with a base URL under 'url'.
                - Optional 'rsa' settings with a non-empty 'key_location' and
                  a 'bit' length. RSA initialization is skipped when the
                  section, option, or location value is absent.
    
    Returns:
        The Flask application instance with the new routes registered.
    """
	from flask import request
	import rsa

	SECRET_KEY = config.settings.get('auth' ,'secret_key') # Used for token signing
	APP_URL = config.settings.get('app' ,'url')
	if APP_URL[-1] != '/':
		APP_URL += '/'

	# RSA is an optional, currently inactive encryption layer around the signed
	# JWT. Keep the key support available, but do not create/read key files unless
	# an explicit, non-empty key directory is configured.
	RSA_PUBLIC = None
	RSA_PRIVATE = None
	RSA_KEY_LOCATION = config.settings.get('rsa', 'key_location', fallback=None)
	if RSA_KEY_LOCATION:
		RSA_KEY_LOCATION = RSA_KEY_LOCATION.strip()
	if RSA_KEY_LOCATION:
		RSA_KEY_BITS = config.settings.getint('rsa', 'bit', fallback=2048)
		RSA_PUBLIC, RSA_PRIVATE = get_rsa_key_pairs(
			RSA_KEY_LOCATION,
			bit=RSA_KEY_BITS,
		)

	@app.route('/token/<token>/' + '<path:req_path>')
	@app.route('/token/<token>/', defaults={'req_path': ''})
	# @app.route('/token/<token>/', methods=['GET'])
	@cross_origin(allow_headers=['Content-Type'])
	def use_token(token, req_path, request=request):
		"""
        Decode the provided token and route the request based on the embedded URL.

        This endpoint decodes the JWT token. If valid, it reconstructs the new request URL by appending
        the provided path to the decoded URL. It then adjusts the request object and forwards the request 
        to the corresponding endpoint. If the token is expired or has an invalid signature, an appropriate 
        error message is returned.

        Parameters (from URL and request):
            token (str): The JWT token provided in the URL.
            req_path (str): Additional path segments following the token.

        Returns:
            The response from the forwarded request or an error message if the token is not valid.
        """
		# Encrypt token
		# token = dict(rsa_decrypt_from_url_friendly_string(token, RSA_PRIVATE))
		decoded_token = decode_token(token, SECRET_KEY)
		print(f'{decoded_token=}')
		if isinstance(decoded_token, dict):
			new_req_url = f"{decoded_token['url']}/{req_path}"
			new_req_path = f"/{new_req_url.replace(APP_URL ,'')}"
			new_req_endpoint = new_req_path.split('/')[1]
			old_headers = request.headers
		elif decoded_token == 'expired':
			return ('Token Expired')
		elif decoded_token == 'invalid_signature':
			return('You are being naughty and trying to pass an invalid token')
		else:
			abort(404)

		# rewrite_url = f'{APP_URL}{url}/{req_path}'
		# rewrite_url = f'{APP_URL}{url}/{req_path}'
		# rewrite_url = f'{new_req_url}/{req_path}'
		# print(f'{rewrite_url=}')
		# return redirect(rewrite_url)
		# with urllib.request.urlopen(rewrite_url) as resp:
		# 	return resp.read()
		# if endpoint == 'ng':
		# 	from neuroGlancer import neuro_glancer_entry
		# 	print('TRYING TO RD')
		# 	return neuro_glancer_entry(req_path)

		# Rewrite request obj to pass to appropriate endpoint
			class fake_request_obj:
				"""Request-like object used to dispatch a decoded token internally."""
				url = new_req_url
			path = new_req_path
			headers = old_headers

		fro = fake_request_obj()

		if new_req_endpoint == 'ng':
			neuro_glancer_entry = app.view_functions['neuro_glancer_entry']
			return neuro_glancer_entry(new_req_path, request=fro)
		else:
			return "Humm, seems like it's not working"

		# return f'{decoded_token=} AND {request.args=} AND {new_req_url=} AND {new_req_path=}, AND {request.path=} AND {request.url=}'



	@app.route('/get_token/', methods=['GET'])
	# @cross_origin(allow_headers=['Content-Type'])
	def get_token_endpoint():
		"""
        Generate a JWT token for the specified URL with an expiration period.

        This endpoint reads the 'url' and optional 'days' query parameters, generates a JWT token 
        with the expiration date calculated from the current date plus the specified number of days, 
        and returns the token.

        Query Parameters:
            url (str): The target URL to include in the token payload.
            days (int, optional): Number of days until the token expires. Defaults to 30 if not provided.

        Returns:
            str: A string representation of the generated token along with the request arguments.
        """
		url = request.args['url']
		if 'days' in request.args:
			days = int(request.args['days'])
		else:
			days = 30

		url
		token = get_token(url=url, days=30, SECRET_KEY=SECRET_KEY)

		# Encrypt token
		# token = rsa_encrypt_string_to_url_friendly(str(token), RSA_PUBLIC)

		print(f'{token=}')
		print(f'{request=}')

		return f'{token=} AND {request.args=}'







	# from werkzeug.routing import RequestRedirect  # , MethodNotAllowed, NotFound

	# def get_view_function(url, method='GET'):
	# 	"""Match a url and return the view and arguments
    #     it will be called with, or None if there is no view.
    #     """
	#
	# 	adapter = app.url_map.bind('localhost')
	# 	print(f'{adapter=}')
	#
	# 	try:
	# 		match = adapter.match(url, method=method)
	# 		print(f'{match=}')
	# 	except RequestRedirect as e:
	# 		# recursively match redirects
	# 		return get_view_function(e.new_url, method)
	# 	# except (MethodNotAllowed, NotFound):
	# 	#     # no match
	# 	#     return None
	#
	# 	try:
	# 		# return the view function and arguments
	# 		return app.view_functions[match[0]], match[1]
	# 	except KeyError:
	# 		# no view is associated with the endpoint
	# 		print('KeyError')
	# 		return None

	return app


# SECRET_KEY = 'asdlkj3t90845hwefkpq35g8947h5oiqwrbnqwc q3roq385y58p93r93rnvq-r3-gghmq3rn3 v-35539yu5h'
# test_url = r'http://136.142.29.160:5001/ng/world/BrainA.ims'
# exp_days = 1
# token = get_token(test_url, exp_days, SECRET_KEY=SECRET_KEY)
# out = decode_token(token, SECRET_KEY=SECRET_KEY)
# print(out)
