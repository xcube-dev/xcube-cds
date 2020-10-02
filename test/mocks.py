import json
import shutil
import os


class CDSClientMock:

    """A simple mock of the cdsapi.Client class

    This mock class uses predefined requests from on-disk JSON files. When
    the retrieve method is called, it checks if the request matches one of
    these known requests, and if so returns the associated result (also read
    from disk). If the request cannot be matched, an exception is raised.

    This class implements a minimal subset of the functionality of the
    actual cdsapi.Client class, just sufficient to allow the unit tests to run.
    """

    def __init__(self, url=None, key=None):
        class Session:
            def close(self):
                pass
        self.session = Session()

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
