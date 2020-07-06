import mantistable.settings

def websocket_url(request):
    if mantistable.settings.DEBUG:
        return {
            'websocket_url': "'ws://' + window.location.hostname + ':5001/'"
        }

    return {
        'websocket_url': "'ws://' + window.location.host + '/'"
    }
 
