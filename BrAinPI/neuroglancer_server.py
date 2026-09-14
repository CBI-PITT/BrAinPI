"""Start the optional standalone Neuroglancer frontend server."""

import neuroglancer
import urllib.request
import threading
from logger_tools import logger
# Project specific imports
import config_tools


def get_server():
    """
    Launch the Neuroglancer server.
    """
    settings = config_tools.get_config('settings.ini')
    ip = settings.get('neuroglancer','local_ip')
    port = settings.getint('neuroglancer','local_port')
    token = 'base'

    # When Multiple processes are running, we only want 1 instance of the neuroglancer server
    # This will test for whether the server is running and if:
    #   False: Return server
    #   True: Return None
    code = None
    probe_ip = "127.0.0.1" if ip in ("0.0.0.0", "::") else ip
    try:
        ng_server_url = f'http://{probe_ip}:{port}/v/{token}/'
        with urllib.request.urlopen(ng_server_url, timeout=2) as r:
            code = r.getcode()
    except Exception:
        pass

    if code == 200:
        return
    else:
        try:
            neuroglancer.set_server_bind_address(bind_address=ip, bind_port=port)
            viewer = neuroglancer.UnsynchronizedViewer(token=token)
            logger.success(f"Neuroglancer frontend listening at {viewer}")
            return viewer
        except Exception as exc:
            logger.error(f"Unable to start Neuroglancer frontend: {exc}")
            return None

def keep_alive():
    """
    Keep the standalone process alive on Linux, macOS, and Windows.
    """
    threading.Event().wait()

if __name__ == '__main__':
    viewer = get_server()
    if viewer:
        keep_alive()
