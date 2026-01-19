"""Tests for BuildSession context manager."""

import pandas as pd
import pytest

from migrator_studio import BuildSession
from migrator_studio._tracking import get_active_session, is_build_mode


class TestBuildSession:
    """Tests for BuildSession."""

    def test_session_activates_build_mode(self):
        """Session should activate build mode when entered."""
        assert not is_build_mode()

        with BuildSession(sample=10) as session:
            assert is_build_mode()
            assert get_active_session() is not None

        assert not is_build_mode()
        assert get_active_session() is None

    def test_session_tracks_sample_size(self):
        """Session should track the sample size."""
        with BuildSession(sample=5) as session:
            assert session.sample == 5

    def test_history_starts_empty(self):
        """History should start empty."""
        with BuildSession() as session:
            assert session.history == []
            assert session.last_operation is None

    def test_summary_returns_dataframe(self):
        """Summary should return a DataFrame."""
        with BuildSession() as session:
            summary = session.summary()
            assert isinstance(summary, pd.DataFrame)
            assert list(summary.columns) == [
                "operation", "rows_before", "rows_after", "change", "duration_ms"
            ]

    def test_nested_sessions_not_recommended(self):
        """Nested sessions overwrite the outer session context."""
        with BuildSession(sample=10) as outer:
            assert get_active_session().sample_size == 10

            with BuildSession(sample=5) as inner:
                # Inner session takes over
                assert get_active_session().sample_size == 5

            # After inner exits, session is None (not restored to outer)
            assert get_active_session() is None

    def test_session_without_context_manager(self):
        """Session can be used without context manager for Marimo cells."""
        session = BuildSession(sample=10)

        # Before entering
        assert not is_build_mode()

        # Manually enter
        session.__enter__()
        assert is_build_mode()

        # Manually exit
        session.__exit__(None, None, None)
        assert not is_build_mode()
