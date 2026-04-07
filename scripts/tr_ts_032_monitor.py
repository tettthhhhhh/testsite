#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import sys
import textwrap
from datetime import date, datetime, timedelta, timezone
from html import unescape
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


API_BASE = os.environ.get("SS_API_BASE", "https://ss-api.ru").rstrip("/")
STATE_PATH = Path(os.environ.get("STATE_PATH", ".state/tr_ts_032_seen.json"))
EMAIL_BODY_PATH = Path(os.environ.get("EMAIL_BODY_PATH", ".state/tr_ts_032_email.txt"))
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "7"))
PAGE_SIZE = max(10, int(os.environ.get("PAGE_SIZE", "100")))
TR_TS_032_ID = 5


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def read_json(url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    if params:
        query = urlencode(params)
        url = f"{url}?{query}"
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "tr-ts-032-monitor/1.0",
        },
    )
    try:
        with urlopen(request, timeout=30) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            payload = response.read().decode(charset)
            return json.loads(payload)
    except HTTPError as exc:
        raise RuntimeError(f"HTTP {exc.code} for {url}") from exc
    except URLError as exc:
        raise RuntimeError(f"Network error for {url}: {exc.reason}") from exc


def load_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {"bootstrapped": False, "seen_source_indexes": []}
    return json.loads(STATE_PATH.read_text(encoding="utf-8"))


def save_state(state: dict[str, Any]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(
        json.dumps(state, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def recent_from_date() -> str:
    return (date.today() - timedelta(days=LOOKBACK_DAYS)).isoformat()


def iter_recent_documents() -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    page = 1
    pages = 1
    while page <= pages:
        response = read_json(
            f"{API_BASE}/documents",
            {
                "date_beginning_from": recent_from_date(),
                "page": page,
                "size": PAGE_SIZE,
            },
        )
        pages = int(response.get("pages") or 1)
        documents.extend(response.get("items") or [])
        page += 1
    return documents


def fetch_document_detail(source_index: int) -> dict[str, Any]:
    return read_json(f"{API_BASE}/documents/{source_index}")


def clean_text(value: Any, fallback: str = "—") -> str:
    if value is None:
        return fallback
    if isinstance(value, str):
        value = unescape(value).strip()
        if not value:
            return fallback
        return " ".join(value.split())
    return str(value)


def first_nonempty(*values: Any, fallback: str = "—") -> str:
    for value in values:
        cleaned = clean_text(value, fallback="")
        if cleaned:
            return cleaned
    return fallback


def matches_tr_ts_032(detail: dict[str, Any]) -> bool:
    regulations = {str(value) for value in (detail.get("idTechnicalReglaments") or [])}
    if str(TR_TS_032_ID) in regulations:
        return True
    for group in detail.get("productGroups") or []:
        if str(group.get("idTechReg")) == str(TR_TS_032_ID):
            return True
    haystacks = [
        json.dumps(detail.get("product") or {}, ensure_ascii=False),
        json.dumps(detail.get("productGroups") or [], ensure_ascii=False),
    ]
    text = " ".join(haystacks).lower()
    return "тр тс 032/2013" in text or "032/2013" in text


def shorten(value: str, limit: int = 220) -> str:
    value = clean_text(value)
    if len(value) <= limit:
        return value
    return value[: limit - 1].rstrip() + "…"


def summarize_document(detail: dict[str, Any], summary: dict[str, Any]) -> dict[str, str]:
    applicant = detail.get("applicant") or {}
    manufacturer = detail.get("manufacturer") or {}
    product = detail.get("product") or {}
    identifications = product.get("identifications") or []
    first_identification = identifications[0] if identifications else {}
    status = detail.get("status") or {}

    source_index = detail.get("idCertificate") or summary.get("source_index")
    reg_number = first_nonempty(detail.get("number"), summary.get("reg_number"))
    date_beginning = first_nonempty(detail.get("dateBeginning"), summary.get("date_beginning"))

    return {
        "source_index": str(source_index),
        "reg_number": reg_number,
        "date_beginning": date_beginning,
        "status": first_nonempty(status.get("status_name"), summary.get("document_status", {}).get("name")),
        "applicant": shorten(first_nonempty(applicant.get("shortName"), applicant.get("fullName"))),
        "manufacturer": shorten(first_nonempty(manufacturer.get("shortName"), manufacturer.get("fullName"))),
        "product": shorten(first_nonempty(product.get("fullName"), first_identification.get("name")), limit=500),
        "detail_url": f"{API_BASE}/documents/{source_index}",
    }


def write_github_output(name: str, value: str) -> None:
    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        return
    with open(output_path, "a", encoding="utf-8") as handle:
        handle.write(f"{name}<<EOF\n{value}\nEOF\n")


def set_outputs(send_email: bool, subject: str = "", body_file: str = "", note: str = "") -> None:
    write_github_output("send_email", "true" if send_email else "false")
    write_github_output("email_subject", subject)
    write_github_output("email_body_file", body_file)
    write_github_output("note", note)


def write_email(subject: str, body: str) -> str:
    EMAIL_BODY_PATH.parent.mkdir(parents=True, exist_ok=True)
    EMAIL_BODY_PATH.write_text(body.rstrip() + "\n", encoding="utf-8")
    set_outputs(
        send_email=True,
        subject=subject,
        body_file=str(EMAIL_BODY_PATH.resolve()),
        note="Prepared email notification.",
    )
    return subject


def build_email_body(items: list[dict[str, str]]) -> str:
    header = f"Новые сертификаты/декларации по ТР ТС 032: {len(items)}"
    blocks = [header, ""]

    for item in items:
        blocks.extend(
            [
                f"Номер: {item['reg_number']}",
                f"Дата: {item['date_beginning']}",
                f"Статус: {item['status']}",
                f"Заявитель: {item['applicant']}",
                f"Производитель: {item['manufacturer']}",
                f"Продукция: {item['product']}",
                f"Source index: {item['source_index']}",
                f"API: {item['detail_url']}",
                "",
            ]
        )

    return "\n".join(blocks).rstrip()


def bootstrap_state(state: dict[str, Any], details: list[dict[str, Any]]) -> None:
    state["bootstrapped"] = True
    state["bootstrapped_at"] = now_utc_iso()
    state["seen_source_indexes"] = sorted({int(item["source_index"]) for item in details})
    save_state(state)
    set_outputs(send_email=False, note="Bootstrap only. No email sent.")
    print(
        f"Bootstrap complete. Saved {len(state['seen_source_indexes'])} known TR TS 032 documents without sending email."
    )


def main() -> int:
    send_test = "--send-test" in sys.argv

    if send_test:
        subject = "Тест: монитор TR TS 032"
        body = textwrap.dedent(
            f"""
            Это тестовое письмо от монитора ТР ТС 032.

            Время формирования: {now_utc_iso()}
            Адрес API: {API_BASE}
            """
        ).strip()
        write_email(subject, body)
        print("Prepared test email.")
        return 0

    state = load_state()
    seen = {int(value) for value in state.get("seen_source_indexes", [])}

    documents = iter_recent_documents()
    documents_to_check = [doc for doc in documents if int(doc.get("source_index")) not in seen]
    if not state.get("bootstrapped"):
        documents_to_check = documents

    matched_details: list[dict[str, str]] = []
    all_current_matches: list[dict[str, str]] = []

    for summary in documents_to_check:
        source_index = int(summary["source_index"])
        detail = fetch_document_detail(source_index)
        if not matches_tr_ts_032(detail):
            continue
        normalized = summarize_document(detail, summary)
        all_current_matches.append(normalized)
        if source_index not in seen:
            matched_details.append(normalized)

    if not state.get("bootstrapped"):
        bootstrap_state(state, all_current_matches)
        return 0

    if not matched_details:
        set_outputs(send_email=False, note="No new TR TS 032 documents found.")
        print("No new TR TS 032 documents found.")
        return 0

    matched_details.sort(key=lambda item: (item["date_beginning"], item["source_index"]))
    subject = f"TR TS 032: {len(matched_details)} новых документов"
    body = build_email_body(matched_details)
    write_email(subject, body)

    updated_seen = seen | {int(item["source_index"]) for item in matched_details}
    state["seen_source_indexes"] = sorted(updated_seen)
    state["last_notified_at"] = now_utc_iso()
    state["last_notified_count"] = len(matched_details)
    save_state(state)

    print(f"Prepared email for {len(matched_details)} new TR TS 032 documents.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
