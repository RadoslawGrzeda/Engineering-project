import io
import os
from unittest.mock import MagicMock, patch

import pytest

from minio_client import MinioClient


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    """MinioClient z zamockowanym połączeniem Minio."""
    with patch("minio_client.Minio") as mock_class:
        mock_instance = MagicMock()
        mock_class.return_value = mock_instance
        c = MinioClient()
        c._mock = mock_instance  # skrót do mocka w testach
        yield c


# ---------------------------------------------------------------------------
# upload_file
# ---------------------------------------------------------------------------

class TestUploadFile:
    def test_sukces_zwraca_wiadomosc_sukcesu(self, client):
        data = io.BytesIO(b"col1,col2\n1,2")
        result = client.upload_file("user", "produkt", "file.csv", data, 13, "application/csv")
        assert "Successfully" in result
        assert "file.csv" in result
        assert "produkt" in result

    def test_sukces_wywoluje_put_object(self, client):
        data = io.BytesIO(b"test")
        client.upload_file("user", "produkt", "file.csv", data, 4, "application/csv")
        client._mock.put_object.assert_called_once_with(
            "produkt", "file.csv", data, 4, "application/csv"
        )

    def test_wyjatek_zwraca_wiadomosc_bledu(self, client):
        client._mock.put_object.side_effect = Exception("connection refused")
        data = io.BytesIO(b"test")
        result = client.upload_file("user", "produkt", "file.csv", data, 4, "application/csv")
        assert "Error" in result
        assert "file.csv" in result

    def test_wyjatek_nie_propaguje_sie(self, client):
        client._mock.put_object.side_effect = Exception("timeout")
        data = io.BytesIO(b"test")
        # nie powinien rzucać wyjątku na zewnątrz
        result = client.upload_file("user", "produkt", "file.csv", data, 4, "application/csv")
        assert result is not None

    def test_przekazuje_poprawna_nazwe_bucketa(self, client):
        data = io.BytesIO(b"x")
        client.upload_file("user", "sklep", "plik.csv", data, 1, "application/csv")
        args = client._mock.put_object.call_args[0]
        assert args[0] == "sklep"

    def test_przekazuje_poprawna_nazwe_pliku(self, client):
        data = io.BytesIO(b"x")
        client.upload_file("user", "produkt", "20240220_chief.csv", data, 1, "application/csv")
        args = client._mock.put_object.call_args[0]
        assert args[1] == "20240220_chief.csv"


# ---------------------------------------------------------------------------
# make_bucket
# ---------------------------------------------------------------------------

class TestMakeBucket:
    def test_sukces_zwraca_true(self, client):
        result = client.make_bucket("user", "produkt")
        assert result is True

    def test_sukces_wywoluje_make_bucket(self, client):
        client.make_bucket("user", "produkt")
        client._mock.make_bucket.assert_called_once_with("produkt")

    def test_wyjatek_zwraca_false(self, client):
        client._mock.make_bucket.side_effect = Exception("bucket already exists")
        result = client.make_bucket("user", "produkt")
        assert result is False

    def test_wyjatek_nie_propaguje_sie(self, client):
        client._mock.make_bucket.side_effect = Exception("error")
        result = client.make_bucket("user", "produkt")
        assert result is not None

    def test_ignoruje_client_name_uzywa_bucket_name(self, client):
        client.make_bucket("jakis_user", "docelowy_bucket")
        client._mock.make_bucket.assert_called_once_with("docelowy_bucket")


# ---------------------------------------------------------------------------
# bucket_exists
# ---------------------------------------------------------------------------

class TestBucketExists:
    def test_zwraca_true_gdy_bucket_istnieje(self, client):
        client._mock.bucket_exists.return_value = True
        assert client.bucket_exists("produkt") is True

    def test_zwraca_false_gdy_brak_bucketa(self, client):
        client._mock.bucket_exists.return_value = False
        assert client.bucket_exists("produkt") is False

    def test_wywoluje_z_poprawna_nazwa(self, client):
        client.bucket_exists("sklep")
        client._mock.bucket_exists.assert_called_once_with("sklep")

    def test_rozne_buckety_rozne_wyniki(self, client):
        client._mock.bucket_exists.side_effect = lambda name: name == "produkt"
        assert client.bucket_exists("produkt") is True
        assert client.bucket_exists("sklep") is False