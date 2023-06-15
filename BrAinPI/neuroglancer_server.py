import neuroglancer
import urllib.request

# Project specific imports
import utils
def get_server():
    settings = utils.get_config('settings.ini')
    ip = settings.get('neuroglancer','local_ip')
    port = settings.getint('neuroglancer','local_port')
    token = 'base'

    # When Multiple processes are running, we only want 1 instance of the neuroglancer server
    # This will test for whether the server is running and if:
    #   False: Return server
    #   True: Return None
    code = None
    try:
        ng_server_url = f'http://{ip}:{port}/v/{token}/'
        with urllib.request.urlopen(ng_server_url) as r:
            code = r.getcode()
    except:
        pass

    if code == 200:
        return
    else:
        try:
            neuroglancer.set_server_bind_address(bind_address=ip, bind_port=port)
            viewer = neuroglancer.UnsynchronizedViewer(token='base')
            # viewer = neuroglancer.Viewer()
            print(viewer)
            return viewer
        except OSError:
            del viewer
            return
        except:
            del viewer
            return

if __name__ == '__main__':
        viewer = get_server()
