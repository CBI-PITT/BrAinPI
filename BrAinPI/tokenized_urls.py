
# pip install pyjwt
from datetime import datetime, timedelta
import jwt
from jwt.exceptions import ExpiredSignatureError, InvalidSignatureError
import urllib.request

from flask import (
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
	if not os.path.exists(os.path.join(location,'public_key.pem')) and \
			not os.path.exists(os.path.join(location, 'private_key.key')):
		generate_key_pairs_save_to_disk(location, bit = bit)

def get_rsa_key_pairs(location: str, bit: int = 512):
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
	bstring = urlsafe_b64decode(encMessage)
	result = []
	for n in range(0, len(bstring), 5):
		part = bstring[n:n + 5]
		result.append(rsa.decrypt(part, private_key).decode())
	result = ''.join(result)
	return result


from flask_cors import cross_origin

def get_token(url: str, days: int, SECRET_KEY: str = None):
	assert SECRET_KEY is not None, 'A secret key must be defined in settings.ini ["auth"] for this to be valid'
	a = datetime.now().date() + timedelta(days=days)
	exp = datetime(a.year ,a.month ,a.day)
	exp = exp.timestamp()
	exp = int(exp)
	encoded_data = jwt.encode(payload={"url": url, "exp": exp} ,key=SECRET_KEY ,algorithm="HS256")
	return encoded_data


def decode_token(token: str, SECRET_KEY: str = None):
	assert SECRET_KEY is not None, 'A secret key must be defined in settings.ini ["auth"] for this to be valid'
	try:
		decoded_data = jwt.decode(jwt=token ,key=SECRET_KEY ,algorithms=["HS256"])
	except ExpiredSignatureError:
		decoded_data = 'expired'
	except InvalidSignatureError: # Wrong SECRET_KEY
		decoded_data = 'invalid_signature'
	return decoded_data










def rsa_encrypt_string(before: str, public_key):
	result = []
	for n in range(0, len(before), 5):
		part = before[n:n + 5]
		result.append(rsa.encrypt(part.encode(), public_key))
	# print(len(result), len(result[0]))
	encMessage = b''.join(result)
	# encMessage = encrypt(before.encode(), public_key)
	return base64.b85encode(encMessage).decode()

def rsa_decrypt_string(encMessage: str, private_key):
	bstring = base64.b85decode(encMessage).decode()
	result = []
	for n in range(0, len(bstring), 5):
		part = bstring[n:n + 5]
		result.append(rsa.decrypt(part, private_key).decode())
	result = ''.join(result)
	return result




def setup_tokenized_endpoint(app, config):
	from flask import request
	import rsa

	SECRET_KEY = config.settings.get('auth' ,'secret_key') # Used for token signing
	APP_URL = config.settings.get('app' ,'url')
	if APP_URL[-1] is not '/':
		APP_URL += '/'

	RSA_KEY_LOCATION = config.settings.get('rsa', 'key_location')  # Used for token signing
	RSA_KEY_BITS = config.settings.getint('rsa', 'bit')  # Used for token signing

	if RSA_KEY_LOCATION is None:
		RSA_KEY_LOCATION = os.curdir

	print('MAKING KEYS')
	RSA_PUBLIC, RSA_PRIVATE = get_rsa_key_pairs(RSA_KEY_LOCATION, bit=RSA_KEY_BITS)

	@app.route('/token/<token>/' + '<path:req_path>')
	@app.route('/token/<token>/', defaults={'req_path': ''})
	# @app.route('/token/<token>/', methods=['GET'])
	@cross_origin(allow_headers=['Content-Type'])
	def use_token(token, req_path, request=request):

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
