import json
import shutil
import os
import inspect

import cdsapi

_SAVE_REQUESTS_AND_RESULTS = True


class _SessionMock:
    def close(self):
        pass


class CDSClientMock:

    """A simple mock of the cdsapi.Client class

    This mock class uses predefined requests from on-disk JSON files. When
    the retrieve method is called, it checks if the request matches one of
    these known requests, and if so returns the associated result (also read
    from disk). If the request cannot be matched, an exception is raised.

    This class implements a minimal subset of the functionality of the
    actual cdsapi.Client class, just sufficient to allow the unit tests to run.

    The url and key arguments are not perfectly mocked: in the actual CDS API
    client, the environment variables are read as part of the default
    argument definitions. This is hard to test, since default arguments are
    only evaluated once, when the method is defined. In the mock,
    the parameters have None as a default, which is replaced (if possible) in
    the method body with an environment variable read at runtime.
    """

    def __init__(self, url=None, key=None):
        self.session = _SessionMock()

        if url is None:
            url = os.environ.get('CDSAPI_URL')
        if key is None:
            key = os.environ.get('CDSAPI_KEY')
        dotrc = os.environ.get('CDSAPI_RC', os.path.expanduser('~/.cdsapirc'))
        if url is None or key is None:
            if os.path.exists(dotrc):
                config = cdsapi.api.read_config(dotrc)
                if key is None:
                    key = config.get('key')
                if url is None:
                    url = config.get('url')
        if url is None or key is None:
            raise Exception(f'Missing/incomplete configuration file: {dotrc}')

        self.url = url
        self.key = key

        resource_path = os.path.join(os.path.dirname(__file__),
                                     'mock_results')
        # request_map is a list because dicts can't be hashed in Python, and
        # it's not worth introducing a dependency on frozendict just for this.
        self.request_map = []
        for d in os.listdir(resource_path):
            dir_path = os.path.join(resource_path, d)
            with open(os.path.join(dir_path, 'request.json'), 'r') as fh:
                request = json.load(fh)
            self.request_map.append((request, os.path.join(dir_path, 'result')))

    def _get_result(self, request):
        for canned_request, canned_result in self.request_map:
            if request == canned_request:
                return canned_result
        raise KeyError('Request not recognized')

    def retrieve(self, dataset_name, params, file_path):
        params_with_name = {**dict(_dataset_name=dataset_name),
                            **params}
        shutil.copy2(self._get_result(params_with_name), file_path)


def get_cds_client(dirname=None):
    if _SAVE_REQUESTS_AND_RESULTS:
        # Wrap the real client and save the requests and responses
        # for future mocking.
        dirname = inspect.currentframe().f_back.f_code.co_name
        resource_path = os.path.join(os.path.dirname(__file__),
                                     'mock_results')
        path = os.path.join(resource_path, dirname)

        class CDSClientWrapper:

            def __init__(self, url=None, key=None):
                self.session = _SessionMock()
                self.real_client = cdsapi.Client()

            def retrieve(self, dataset_name, params, file_path):
                params_with_name = {**dict(_dataset_name=dataset_name),
                                    **params}
                with open(os.path.join(path, 'request.json'), 'w') as fh:
                    json.dump(params_with_name, fh)
                self.real_client.retrieve(dataset_name, params, file_path)
                shutil.copy2(file_path, os.path.join(path, 'result'))

        return CDSClientWrapper
    else:
        # Use pre-generated response data for known requests.
        return CDSClientMock
