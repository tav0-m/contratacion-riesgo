"""Ingesta de contratos SECOP II (API Socrata de datos.gov.co) a Parquet crudo.

Capa "raw": los valores se guardan como texto, tal como los entrega la API. El tipado y la
limpieza ocurren en una etapa posterior, para poder reprocesar sin volver a descargar.

Particionado: data/raw/secop_contratos/anio=YYYY/mes=MM.parquet (por fecha de firma).

Uso:
    python -m riesgo_contratos.ingest --desde 2025-01 --hasta 2025-03
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Protocol

import polars as pl
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import Settings

logger = logging.getLogger(__name__)

# Subconjunto explícito de columnas. Se excluyen a propósito datos sensibles que no aportan
# al problema: cuentas bancarias, documentos de supervisores/ordenadores y del representante legal.
COLUMNS: tuple[str, ...] = (
    "id_contrato",
    "proceso_de_compra",
    "referencia_del_contrato",
    "nombre_entidad",
    "nit_entidad",
    "codigo_entidad",
    "departamento",
    "ciudad",
    "orden",
    "sector",
    "rama",
    "entidad_centralizada",
    "estado_contrato",
    "codigo_de_categoria_principal",
    "descripcion_del_proceso",
    "objeto_del_contrato",
    "tipo_de_contrato",
    "modalidad_de_contratacion",
    "justificacion_modalidad_de",
    "fecha_de_firma",
    "fecha_de_inicio_del_contrato",
    "fecha_de_fin_del_contrato",
    "duraci_n_del_contrato",
    "dias_adicionados",
    "el_contrato_puede_ser_prorrogado",
    "tipodocproveedor",
    "documento_proveedor",
    "codigo_proveedor",
    "proveedor_adjudicado",
    "es_grupo",
    "es_pyme",
    "habilita_pago_adelantado",
    "origen_de_los_recursos",
    "destino_gasto",
    "valor_del_contrato",
    "valor_de_pago_adelantado",
    "valor_facturado",
    "valor_pagado",
    "valor_pendiente_de_pago",
    "espostconflicto",
    "ultima_actualizacion",
)


class HttpSession(Protocol):
    def get(self, url: str, *, params: dict[str, str], timeout: float) -> Any: ...


def build_session(settings: Settings) -> requests.Session:
    retry = Retry(
        total=settings.max_retries,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers["Accept"] = "application/json"
    if settings.app_token:
        session.headers["X-App-Token"] = settings.app_token
    return session


def parse_month(value: str) -> date:
    try:
        year, month = value.split("-")
        return date(int(year), int(month), 1)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Mes inválido '{value}', se espera YYYY-MM") from exc


def next_month(d: date) -> date:
    return date(d.year + d.month // 12, d.month % 12 + 1, 1)


def month_range(start: date, end: date) -> Iterator[tuple[date, date]]:
    """Genera (inicio, inicio_del_mes_siguiente) para cada mes entre start y end, inclusive."""
    if start > end:
        raise ValueError(f"'desde' ({start:%Y-%m}) es posterior a 'hasta' ({end:%Y-%m})")
    current = start.replace(day=1)
    while current <= end:
        following = next_month(current)
        yield current, following
        current = following


def month_filter(month_start: date, month_end: date) -> str:
    return (
        f"fecha_de_firma >= '{month_start.isoformat()}T00:00:00' "
        f"AND fecha_de_firma < '{month_end.isoformat()}T00:00:00'"
    )


def fetch_month(
    session: HttpSession, settings: Settings, month_start: date, month_end: date
) -> list[dict[str, str | None]]:
    """Descarga todas las filas de un mes paginando por offset con orden estable (:id)."""
    rows: list[dict[str, str | None]] = []
    offset = 0
    while True:
        params = {
            "$select": ",".join(COLUMNS),
            "$where": month_filter(month_start, month_end),
            "$order": ":id",
            "$limit": str(settings.page_size),
            "$offset": str(offset),
        }
        response = session.get(settings.endpoint, params=params, timeout=settings.request_timeout_s)
        response.raise_for_status()
        page = response.json()
        rows.extend(normalize_row(r) for r in page)
        logger.debug("%s offset=%d filas=%d", f"{month_start:%Y-%m}", offset, len(page))
        if len(page) < settings.page_size:
            return rows
        offset += settings.page_size


def normalize_row(row: dict[str, Any]) -> dict[str, str | None]:
    """Garantiza todas las columnas y todo como texto (la capa raw no tipa)."""
    return {c: None if row.get(c) is None else str(row[c]) for c in COLUMNS}


def count_month(session: HttpSession, settings: Settings, month_start: date, month_end: date) -> int:
    params = {"$select": "count(*) AS n", "$where": month_filter(month_start, month_end)}
    response = session.get(settings.endpoint, params=params, timeout=settings.request_timeout_s)
    response.raise_for_status()
    return int(response.json()[0]["n"])


def to_frame(rows: list[dict[str, str | None]], ingested_at: datetime) -> pl.DataFrame:
    schema = {c: pl.String for c in COLUMNS}
    return pl.DataFrame(rows, schema=schema).with_columns(
        pl.lit(ingested_at).alias("_ingested_at")
    )


def partition_path(raw_dir: Path, month_start: date) -> Path:
    return raw_dir / f"anio={month_start.year}" / f"mes={month_start.month:02d}.parquet"


def write_atomic(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    df.write_parquet(tmp, compression="zstd")
    tmp.replace(path)


def ingest(settings: Settings, start: date, end: date, *, force: bool = False) -> int:
    """Descarga los meses pedidos. Devuelve el número de meses cuyo conteo no cuadró con la API."""
    session = build_session(settings)
    mismatches = 0
    for month_start, month_end in month_range(start, end):
        label = f"{month_start:%Y-%m}"
        path = partition_path(settings.raw_dir, month_start)
        if path.exists() and not force:
            logger.info("%s ya existe, se omite (usa --force para re-descargar)", label)
            continue

        rows = fetch_month(session, settings, month_start, month_end)
        expected = count_month(session, settings, month_start, month_end)
        df = to_frame(rows, datetime.now(UTC))
        write_atomic(df, path)

        if df.height != expected:
            mismatches += 1
            logger.warning(
                "%s descargadas=%d esperadas=%d (la fuente pudo actualizarse durante la descarga)",
                label, df.height, expected,
            )
        logger.info("%s -> %s (%d filas)", label, path, df.height)
    return mismatches


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingesta SECOP II -> Parquet crudo")
    parser.add_argument("--desde", type=parse_month, required=True, help="YYYY-MM")
    parser.add_argument("--hasta", type=parse_month, required=True, help="YYYY-MM")
    parser.add_argument("--force", action="store_true", help="Re-descargar meses existentes")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        mismatches = ingest(Settings(), args.desde, args.hasta, force=args.force)
    except (requests.RequestException, ValueError) as exc:
        logger.error("Ingesta fallida: %s", exc)
        return 1
    return 2 if mismatches else 0


if __name__ == "__main__":
    sys.exit(main())
