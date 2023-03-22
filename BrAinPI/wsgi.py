#from gevent import monkey
#monkey.patch_all()
#from gevent.pywsgi import WSGIServer

# from BrAinPI import app

# if __name__ == "__main__":
#     app.run()

#gunicorn -b 0.0.0.0:5000 --chdir /CBI_FastStore/cbiPythonTools/bil_api/BrAinPI wsgi:app -w 24 --threads=2


from BrAinPI import app

if __name__ == "__main__":
    app.run()

# gunicorn --worker-class gevent -b 0.0.0.0:5000 --chdir /CBI_FastStore/cbiPythonTools/bil_api/BrAinPI wsgi:app -w 24 --threads=2

#gunicorn --worker-class gevent -b 0.0.0.0:5000 --chdir /CBI_FastStore/cbiPythonTools/bil_api/BrAinPI wsgi:app -w 24 --threads=2 --worker-connections=2 --reuse-port

#https://eng.lyft.com/gevent-part-3-performance-e64303fa102b