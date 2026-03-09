import io
import os
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from bot import console


class HumanConsoleTests(unittest.TestCase):
    def tearDown(self) -> None:
        console._reset_rate_limit_state()

    def test_say_respects_human_console_flag(self) -> None:
        stream = io.StringIO()
        with patch.dict(os.environ, {"HUMAN_CONSOLE": "false"}, clear=False):
            with redirect_stdout(stream):
                console.say("Connected to market data stream for {symbol}.", symbol="BTCUSDT")

        self.assertEqual(stream.getvalue(), "")

    def test_rate_limiter_suppresses_repeated_messages(self) -> None:
        stream = io.StringIO()
        with patch.dict(os.environ, {"HUMAN_CONSOLE": "true"}, clear=False):
            with redirect_stdout(stream):
                console.say("Skipping: market conditions not suitable right now (reason={reason}).", reason="spread")
                console.say("Skipping: market conditions not suitable right now (reason={reason}).", reason="spread")

        lines = [line for line in stream.getvalue().splitlines() if line]
        self.assertEqual(len(lines), 1)
        self.assertIn("[HUMAN]", lines[0])


if __name__ == "__main__":
    unittest.main()
