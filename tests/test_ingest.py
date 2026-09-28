from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from riesgo_contratos.config import Settings
from riesgo_contratos.ingest import (
    COLUMNS,
    fetch_month,
    month_range,
    normalize_row,
    partition_path,
    to_frame,
    write_atomic,
)


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self) -> None:
        pass

    def json(self):
        return self._payload


class FakeSession:
    """Sirve páginas predefinidas y registra los parámetros recibidos."""

    def __init__(self, pages):
        self.pages = list(pages)
        self.calls: list[dict[str, str]] = []

    def get(self, url, *, params, timeout):
        self.calls.append(params)
        return FakeResponse(self.pages.pop(0))


def test_month_range_cruza_anio():
    months = list(month_range(date(2024, 11, 1), date(2025, 1, 1)))
    assert months == [
        (date(2024, 11, 1), date(2024, 12, 1)),
        (date(2024, 12, 1), date(2025, 1, 1)),
        (date(2025, 1, 1), date(2025, 2, 1)),
    ]


def test_month_range_rechaza_rango_invertido():
    with pytest.raises(ValueError):
        list(month_range(date(2025, 2, 1), date(2025, 1, 1)))


def test_normalize_row_completa_columnas_y_convierte_a_texto():
    row = normalize_row({"id_contrato": "CO1", "valor_del_contrato": 1000, "extra": "x"})
    assert set(row) == set(COLUMNS)
    assert row["valor_del_contrato"] == "1000"
    assert row["proveedor_adjudicado"] is None


def test_fetch_month_pagina_hasta_pagina_incompleta():
    settings = Settings(page_size=2, _env_file=None)
    session = FakeSession([
        [{"id_contrato": "a"}, {"id_contrato": "b"}],
        [{"id_contrato": "c"}],
    ])
    rows = fetch_month(session, settings, date(2025, 1, 1), date(2025, 2, 1))

    assert [r["id_contrato"] for r in rows] == ["a", "b", "c"]
    assert [c["$offset"] for c in session.calls] == ["0", "2"]
    assert all(c["$order"] == ":id" for c in session.calls)
    assert "fecha_de_firma < '2025-02-01T00:00:00'" in session.calls[0]["$where"]


def test_escritura_y_lectura_parquet(tmp_path: Path):
    import polars as pl

    df = to_frame([normalize_row({"id_contrato": "a"})], datetime.now(UTC))
    path = partition_path(tmp_path, date(2025, 3, 1))
    write_atomic(df, path)

    assert path.name == "mes=03.parquet" and path.parent.name == "anio=2025"
    back = pl.read_parquet(path)
    assert back.height == 1 and back["id_contrato"][0] == "a"


def test_to_frame_vacio_mantiene_esquema():
    df = to_frame([], datetime.now(UTC))
    assert df.height == 0
    assert set(COLUMNS) <= set(df.columns)
