import tyvrana_core


def test_package_imports() -> None:
    assert tyvrana_core.__name__ == "tyvrana_core"


def test_public_api_exports() -> None:
    for name in tyvrana_core.__all__:
        assert getattr(tyvrana_core, name) is not None
    assert isinstance(tyvrana_core.AdapterServer, type)
    assert isinstance(tyvrana_core.CoreConfig, type)
    assert issubclass(tyvrana_core.RemoteOperationError, tyvrana_core.CoreError)
