from pytest_mock import MockFixture
from streamlit_helper import setup, setup_streamlit
from streamlit_helper.setup import statsd


def test_login(mocker: MockFixture) -> None:
    mocker.patch.object(setup, "initialize")
    inc = mocker.patch.object(statsd, "increment")

    @setup_streamlit()
    def test() -> None:
        pass

    inc.assert_not_called()
    test()
    inc.assert_called_once()
    called_tags = inc.call_args[-1]["tags"]

    assert "function_name:test" in called_tags
    assert "module:test_setup" in called_tags
    assert "working_dir:packages/streamlit-helper" in called_tags
