import pytest
from wire_rpc import App
from wire_rpc.auth.cookie import CookieSessionAuth


async def test_partial_auth_startup_rolls_back_already_started_dependency_once():
    events=[]
    class Validator:
        async def startup(self):
            events.append('validator started')
        async def shutdown(self):
            events.append('validator stopped')
    class Sessions:
        async def startup(self):
            raise RuntimeError('store unavailable')
        async def shutdown(self):
            events.append('unstarted store stopped')
    auth=CookieSessionAuth(Validator(),Sessions())
    with pytest.raises(RuntimeError):
        await auth.startup()
    await auth.shutdown()
    assert events == ['validator started','validator stopped'], 'partial startup must roll back successful dependencies once without shutting down an uninitialized store'


async def test_failed_application_hook_still_releases_started_transport_dependencies():
    events=[]
    class Transport:
        async def startup(self):
            events.append('start')
        async def shutdown(self):
            events.append('stop')
    app=App(Transport())
    @app.on_startup
    async def start():
        raise RuntimeError('application startup failed')
    with pytest.raises(RuntimeError):
        await app._run()
    assert events == ['start','stop'], 'failed application startup must not leak resources opened by the transport authentication layer'
    assert not app._running, 'a failed startup must release the application lifecycle state'
