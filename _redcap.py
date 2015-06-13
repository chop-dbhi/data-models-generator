from redcap import Project


def patch():
    # Patches _call_api to bypass https://github.com/sburns/PyCap/issues/54
    # since it is not relevant in this context.

    __call_api = Project._call_api

    def _call_api(self, payload, typpe, **kwargs):
        if typpe == 'exp_event':
            return [{'error': 'monkey patched'}]

        return __call_api(self, payload, typpe, **kwargs)

    Project._call_api = _call_api
