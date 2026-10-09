"""`z4s ... | head -1` 처럼 출력을 읽는 쪽이 먼저 닫혀도 오류나 traceback 을 내지 않는다.

BrokenPipeError 를 일반 OSError 로 감싸면 `Error: [Errno 32] Broken pipe` 와 interpreter
종료 시 flush 의 `Exception ignored ... BrokenPipeError` 가 stderr 에 남는다.
"""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from zeta4s.cli import main as cli_main

ROOT = Path(__file__).resolve().parents[1]


def _env(cli_home: Path) -> dict[str, str]:
    env = dict(os.environ)
    env["ZETA4S_CLI_HOME"] = str(cli_home)
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(ROOT / "src"), env.get("PYTHONPATH")]))
    return env


class CliBrokenPipeContractTest(unittest.TestCase):
    def test_closed_stdout_exits_quietly(self) -> None:
        with TemporaryDirectory() as tmp:
            cwd = Path(tmp)
            env = _env(cwd / "home")
            for args in (["work", "init"], ["project", "init", "hello"]):
                subprocess.run(
                    [sys.executable, "-m", "zeta4s.cli.main", *args],
                    cwd=cwd,
                    env=env,
                    capture_output=True,
                    check=True,
                )
            (cwd / "zeta4s-work" / "projects" / "hello" / "jobs" / "hello.yml").write_text(
                "job_id: hello\nsteps:\n  - step_id: start\n    type: noop\n",
                encoding="utf-8",
            )

            for args in (["project", "graph", "hello"], ["--help"]):
                with self.subTest(args=args):
                    read_fd, write_fd = os.pipe()
                    # 읽는 쪽을 먼저 닫아 child 의 첫 write 가 EPIPE 를 받게 한다.
                    os.close(read_fd)
                    try:
                        proc = subprocess.Popen(
                            [sys.executable, "-m", "zeta4s.cli.main", *args],
                            cwd=cwd,
                            env=env,
                            stdout=write_fd,
                            stderr=subprocess.PIPE,
                            text=True,
                        )
                    finally:
                        os.close(write_fd)
                    _, stderr = proc.communicate(timeout=60)

                    self.assertEqual(proc.returncode, 1, stderr)
                    self.assertNotIn("Broken pipe", stderr)
                    self.assertNotIn("BrokenPipeError", stderr)
                    self.assertNotIn("Traceback", stderr)
                    self.assertNotIn("Error:", stderr)

    def test_catch_does_not_wrap_broken_pipe_as_command_error(self) -> None:
        def broken() -> int:
            raise BrokenPipeError(32, "Broken pipe")

        with self.assertRaises(BrokenPipeError):
            cli_main._catch(broken)

    def test_main_returns_one_when_final_flush_hits_broken_pipe(self) -> None:
        class _BrokenStdout:
            def flush(self) -> None:
                raise BrokenPipeError(32, "Broken pipe")

        with (
            patch.object(cli_main, "_run_cli", return_value=0),
            patch.object(cli_main, "_silence_stdout") as silence,
            patch.object(sys, "stdout", _BrokenStdout()),
        ):
            code = cli_main.main([])

        self.assertEqual(code, 1)
        silence.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
