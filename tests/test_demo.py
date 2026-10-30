import pytest

from consensus.demo import main


@pytest.mark.parametrize("args", [[], ["--step"], ["--size", "5", "--seed", "7"]])
def test_demo_runs_every_phase(args: list[str], capsys: pytest.CaptureFixture[str]) -> None:
    main(args)
    out = capsys.readouterr().out
    assert "crashed" in out
    assert "restarts from disk" in out
