import sys
from unittest.mock import MagicMock

sys.modules["streamlit"] = MagicMock()
sys.modules["streamlit_authenticator"] = MagicMock()