#!/usr/bin/env python3
"""Generate Moroccan Darija/French/English code-switching transcript JSONL."""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError as exc:  # pragma: no cover - startup dependency check
    raise SystemExit("Install PyYAML first: pip install pyyaml") from exc

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover - optional progress dependency
    tqdm = None


ARABIC_RE = re.compile(r"[\u0600-\u06FF]")
LATIN_RE = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿ]")
EMAIL_RE = re.compile(r"\b\S+@\S+\.\S+\b")
LONG_NUMBER_RE = re.compile(r"(?:\+?\d[\s\-_.]*){6,}")
DOTENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
EMOJI_RE = re.compile(
    "[\U0001F300-\U0001FAFF\U00002700-\U000027BF\U00002600-\U000026FF]",
    flags=re.UNICODE,
)

FRENCH_TERMS = [
    "rendez-vous",
    "client",
    "projet",
    "réunion",
    "équipe",
    "dossier",
    "problème",
    "service",
    "facture",
    "paiement",
    "livraison",
    "commande",
    "retard",
    "assurance",
    "banque",
    "agence",
    "médecin",
    "pharmacie",
    "urgence",
    "résultat",
    "analyse",
    "document",
    "inscription",
    "attestation",
    "stage",
    "entretien",
    "formation",
    "emploi",
    "responsable",
    "directeur",
    "contrat",
    "salaire",
    "congé",
    "demande",
    "message",
    "numéro",
    "adresse",
    "email",
    "application",
    "connexion",
    "mot de passe",
    "réseau",
    "forfait",
    "recharge",
    "solde",
    "abonnement",
    "confirmation",
    "annulation",
    "réservation",
    "hôtel",
    "taxi",
    "train",
    "billet",
    "bagage",
    "réclamation",
    "remboursement",
    "devis",
    "prix",
    "disponibilité",
]

ENGLISH_TERMS = [
    "meeting",
    "call",
    "email",
    "password",
    "login",
    "account",
    "app",
    "update",
    "bug",
    "issue",
    "backend",
    "frontend",
    "deploy",
    "deadline",
    "task",
    "ticket",
    "support",
    "server",
    "database",
    "dashboard",
    "link",
    "file",
    "report",
    "online",
    "offline",
    "feedback",
    "reminder",
    "schedule",
    "team",
    "manager",
    "business",
    "startup",
    "interview",
    "job",
    "remote",
    "payment",
    "order",
    "delivery",
    "tracking",
    "refund",
    "booking",
    "check-in",
    "checkout",
]

MSA_MARKERS = {
    "لقد",
    "سوف",
    "أيضا",
    "ذلك",
    "تلك",
    "حيث",
    "إذ",
    "إذن",
    "يرجى",
    "المرجو",
    "الخاصة",
    "بشكل",
    "مسبق",
    "لاحقا",
    "المزيد",
    "المعلومات",
    "الخدمات",
    "العميل",
    "المستخدم",
    "يريد",
    "يجب",
    "يمكنك",
    "لدينا",
    "هناك",
}

DOMAIN_ALIASES = {
    "customer_support_calls": "phone_call_support",
    "phone_call_support": "phone_call_support",
    "appointment_booking": "appointment_booking",
    "delivery_order_tracking": "delivery_order_tracking",
    "bank_insurance_calls": "bank_insurance_calls",
    "university_admin_calls": "university_admin_calls",
    "healthcare_appointment_calls": "healthcare_appointment_calls",
    "job_interview_calls": "job_interview_calls",
    "technical_support": "technical_support",
    "project_team_calls": "project_team_calls",
    "ecommerce_returns_refunds": "ecommerce_returns_refunds",
    "travel_hotel_taxi_calls": "travel_hotel_taxi_calls",
    "family_friend_casual_calls": "family_friend_casual_calls",
}

MIX_DESCRIPTIONS = {
    "darija_french": "Darija + French only. Include at least one French word or phrase in Latin script.",
    "darija_english": "Darija + English only. Include at least one English word or phrase in Latin script.",
    "darija_french_english": "Darija + French + English. Include at least one French term and one English term in Latin script.",
    "pure_darija": "Pure Moroccan Darija in Arabic script only. Do not include French or English.",
}


@dataclass(frozen=True)
class QualityResult:
    ok: bool
    reason: str = ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/generation.yaml"))
    parser.add_argument("--num-texts", type=int, default=None, help="Override text_generation.num_texts.")
    parser.add_argument("--batch-size", type=int, default=None, help="Override text_generation.batch_size.")
    return parser.parse_args()


def load_config(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def load_dotenv(path: Path, override: bool = True) -> None:
    if not path.exists():
        return

    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :].strip()
            if "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            if not DOTENV_KEY_RE.match(key):
                continue
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                value = value[1:-1]
            if override or key not in os.environ:
                os.environ[key] = value


def resolve_path(path_value: str | os.PathLike[str], base_dir: Path) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return base_dir / path


def normalize_spaces(text: str) -> str:
    text = text.replace("\u00a0", " ").replace("’", "'")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_for_dedup(text: str) -> str:
    text = normalize_spaces(text).lower()
    text = re.sub(r"[،,.!?؟;:]+", "", text)
    return text


def word_tokens(text: str) -> list[str]:
    return re.findall(r"[\wÀ-ÖØ-öø-ÿ'\-]+", text, flags=re.UNICODE)


def arabic_tokens(text: str) -> list[str]:
    return re.findall(r"[\u0600-\u06FF]+", text)


def latin_terms_present(text: str, terms: list[str]) -> set[str]:
    lowered = text.lower().replace("’", "'")
    found: set[str] = set()
    for term in terms:
        term_lower = term.lower().replace("’", "'")
        if re.search(rf"(?<![A-Za-zÀ-ÖØ-öø-ÿ]){re.escape(term_lower)}(?![A-Za-zÀ-ÖØ-öø-ÿ])", lowered):
            found.add(term)
    return found


def has_repeated_phrase(text: str) -> bool:
    tokens = [token.lower() for token in word_tokens(text)]
    for size in (2, 3, 4):
        if len(tokens) < size * 2:
            continue
        for index in range(len(tokens) - (size * 2) + 1):
            if tokens[index : index + size] == tokens[index + size : index + (size * 2)]:
                return True
    for index in range(len(tokens) - 2):
        if tokens[index] == tokens[index + 1] == tokens[index + 2]:
            return True
    return False


def infer_language_mix(text: str, requested_mix: str) -> list[str]:
    mix = ["darija"]
    french = latin_terms_present(text, FRENCH_TERMS)
    english = latin_terms_present(text, ENGLISH_TERMS)
    if french or requested_mix in {"darija_french", "darija_french_english"} and LATIN_RE.search(text):
        mix.append("french")
    if english or requested_mix in {"darija_english", "darija_french_english"} and LATIN_RE.search(text):
        mix.append("english")
    return list(dict.fromkeys(mix))


def quality_check(text: str, requested_mix: str, config: dict[str, Any]) -> QualityResult:
    text = normalize_spaces(text)
    if not text:
        return QualityResult(False, "empty")
    if EMAIL_RE.search(text) or LONG_NUMBER_RE.search(text):
        return QualityResult(False, "private_or_numeric_data")
    if EMOJI_RE.search(text):
        return QualityResult(False, "emoji")
    if not ARABIC_RE.search(text):
        return QualityResult(False, "no_arabic_darija")

    tokens = word_tokens(text)
    min_words = int(config.get("min_words", 3))
    max_words = int(config.get("max_words", 25))
    if len(tokens) < min_words:
        return QualityResult(False, "too_short")
    if len(tokens) > max_words:
        return QualityResult(False, "too_long")

    latin_present = bool(LATIN_RE.search(text))
    french_present = bool(latin_terms_present(text, FRENCH_TERMS))
    english_present = bool(latin_terms_present(text, ENGLISH_TERMS))

    if requested_mix == "pure_darija" and latin_present:
        return QualityResult(False, "pure_darija_has_latin")
    if requested_mix != "pure_darija" and not latin_present:
        return QualityResult(False, "mixed_sample_without_latin")
    if requested_mix == "darija_french" and not french_present:
        return QualityResult(False, "missing_french")
    if requested_mix == "darija_french" and english_present:
        return QualityResult(False, "wrong_mix_contains_english")
    if requested_mix == "darija_english" and not english_present:
        return QualityResult(False, "missing_english")
    if requested_mix == "darija_english" and french_present:
        return QualityResult(False, "wrong_mix_contains_french")
    if requested_mix == "darija_french_english" and not (french_present and english_present):
        return QualityResult(False, "missing_french_or_english")

    ar_tokens = arabic_tokens(text)
    if ar_tokens:
        msa_count = sum(1 for token in ar_tokens if token in MSA_MARKERS)
        if msa_count >= 2 and msa_count / max(len(ar_tokens), 1) > 0.18:
            return QualityResult(False, "msa_heavy")

    if has_repeated_phrase(text):
        return QualityResult(False, "repeated_phrase")

    return QualityResult(True)


def load_existing_rows(output_path: Path) -> tuple[list[dict[str, Any]], set[str]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    if not output_path.exists():
        return rows, seen
    with output_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            text_key = normalize_for_dedup(str(row.get("text", "")))
            if text_key:
                seen.add(text_key)
            rows.append(row)
    return rows, seen


def clean_jsonl_response(content: str) -> str:
    content = content.strip()
    if content.startswith("```"):
        content = re.sub(r"^```(?:jsonl|json)?\s*", "", content)
        content = re.sub(r"\s*```$", "", content)
    return content.strip()


def parse_jsonl_response(content: str) -> list[dict[str, Any]]:
    cleaned = clean_jsonl_response(content)
    rows: list[dict[str, Any]] = []
    for line in cleaned.splitlines():
        line = line.strip().rstrip(",")
        if not line:
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            rows.append(value)
    if rows:
        return rows

    # Fallback for models that return a JSON array despite the instruction.
    try:
        value = json.loads(cleaned)
    except json.JSONDecodeError:
        return []
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def create_openai_client(text_config: dict[str, Any]):
    try:
        from openai import OpenAI
    except ImportError as exc:  # pragma: no cover - startup dependency check
        raise SystemExit("Install the OpenAI client first: pip install openai") from exc

    api_key_env = text_config.get("api_key_env", "LIGHTNING_API_KEY")
    api_key = os.environ.get(api_key_env) or text_config.get("api_key")
    if not api_key:
        raise SystemExit(
            f"Missing API key. Add `{api_key_env}=...` to `synthetic_cs_dataset/.env` "
            f"or export `{api_key_env}` in your environment."
        )
    return OpenAI(
        base_url=text_config.get("api_base_url"),
        api_key=api_key,
        timeout=float(text_config.get("request_timeout_seconds", 120)),
    )


def build_prompt(
    prompt_template: str,
    n: int,
    first_numeric_id: int,
    domains: list[str],
    mix_name: str,
    max_words: int,
) -> str:
    first_id = f"cs_{first_numeric_id:06d}"
    last_id = f"cs_{first_numeric_id + n - 1:06d}"
    return prompt_template.format(
        n=n,
        first_id=first_id,
        last_id=last_id,
        domains=", ".join(domains),
        mix_description=MIX_DESCRIPTIONS[mix_name],
        french_terms=", ".join(FRENCH_TERMS),
        english_terms=", ".join(ENGLISH_TERMS),
        max_words=max_words,
    )


def request_batch(client: Any, text_config: dict[str, Any], prompt: str) -> str:
    request: dict[str, Any] = {
        "model": text_config["model"],
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": prompt}],
            }
        ],
        "temperature": float(text_config.get("temperature", 0.9)),
        "top_p": float(text_config.get("top_p", 0.95)),
    }
    if text_config.get("max_tokens"):
        request["max_tokens"] = int(text_config["max_tokens"])
    completion = client.chat.completions.create(**request)
    return completion.choices[0].message.content or ""


def friendly_llm_error(exc: Exception, text_config: dict[str, Any]) -> str:
    message = str(exc)
    model = text_config.get("model", "<missing model>")
    api_key_env = text_config.get("api_key_env", "LIGHTNING_API_KEY")
    if "insufficient_balance" in message or "Error code: 402" in message:
        return (
            "Lightning returned 402 insufficient_balance for the configured LLM request. "
            f"The pipeline is using model `{model}` and API key env `{api_key_env}`. "
            "If a small manual request works with a different model, set `text_generation.model` "
            "in the config to that model and restart `viewer_server.py` after updating `.env` "
            f"or the `{api_key_env}` environment value."
        )
    return f"LLM request failed for model `{model}`: {message}"


def target_counts(total: int, distribution: dict[str, float]) -> dict[str, int]:
    raw = {key: total * float(value) for key, value in distribution.items()}
    counts = {key: int(value) for key, value in raw.items()}
    remainder = total - sum(counts.values())
    fractions = sorted(raw.items(), key=lambda item: item[1] - int(item[1]), reverse=True)
    for key, _value in fractions[:remainder]:
        counts[key] += 1
    return counts


def mix_key(row: dict[str, Any]) -> str:
    mix = set(row.get("language_mix") or [])
    if mix == {"darija"}:
        return "pure_darija"
    if {"darija", "french", "english"}.issubset(mix):
        return "darija_french_english"
    if {"darija", "english"}.issubset(mix):
        return "darija_english"
    return "darija_french"


def choose_next_mix(current_counts: Counter[str], desired_counts: dict[str, int]) -> str:
    deficits = {
        key: desired_counts[key] - current_counts.get(key, 0)
        for key in desired_counts
    }
    return max(deficits, key=lambda key: (deficits[key], desired_counts[key]))


def sanitize_row(
    row: dict[str, Any],
    numeric_id: int,
    requested_mix: str,
    domains: list[str],
) -> dict[str, Any] | None:
    text = normalize_spaces(str(row.get("text", "")))
    if not text:
        return None

    domain = str(row.get("domain") or "").strip()
    domain_key = re.sub(r"[^a-z0-9]+", "_", domain.lower()).strip("_")
    if domain_key in DOMAIN_ALIASES:
        domain = DOMAIN_ALIASES[domain_key]
    elif domain_key in domains:
        domain = domain_key
    if domain not in domains:
        domain = random.choice(domains)

    language_mix = infer_language_mix(text, requested_mix)
    contains_code_switch = any(language in language_mix for language in ("french", "english"))
    return {
        "id": f"cs_{numeric_id:06d}",
        "domain": domain,
        "text": text,
        "language_mix": language_mix,
        "contains_code_switch": contains_code_switch,
    }


def append_rows(output_path: Path, rows: list[dict[str, Any]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_report(output_path: Path, report: dict[str, Any]) -> None:
    report_path = output_path.with_name("text_generation_report.json")
    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)


def main() -> None:
    args = parse_args()
    project_dir = Path(__file__).resolve().parent.parent
    load_dotenv(project_dir / ".env")
    config_path = args.config.resolve()
    base_dir = config_path.parent.parent
    config = load_config(config_path)
    seed = int(config.get("project", {}).get("seed", 42))
    random.seed(seed)

    text_config = config["text_generation"]
    if args.num_texts is not None:
        text_config["num_texts"] = args.num_texts
    if args.batch_size is not None:
        text_config["batch_size"] = args.batch_size

    output_path = resolve_path(text_config["output_path"], base_dir)
    prompt_path = resolve_path(text_config["prompt_path"], base_dir)
    prompt_template = prompt_path.read_text(encoding="utf-8")
    domains = list(text_config["domains"])
    total_target = int(text_config["num_texts"])
    batch_size = int(text_config.get("batch_size", 40))
    max_words = int(text_config.get("max_words", 25))
    desired_counts = target_counts(total_target, text_config["language_mix_distribution"])

    rows: list[dict[str, Any]]
    seen_texts: set[str]
    if bool(text_config.get("resume", True)):
        rows, seen_texts = load_existing_rows(output_path)
    else:
        rows, seen_texts = [], set()
        if output_path.exists():
            output_path.unlink()

    current_counts = Counter(mix_key(row) for row in rows)
    next_numeric_id = len(rows) + 1
    reject_counts: Counter[str] = Counter()
    client = create_openai_client(text_config)
    progress = tqdm(total=total_target, initial=len(rows), desc="Accepted texts") if tqdm else None

    try:
        while len(rows) < total_target:
            requested_mix = choose_next_mix(current_counts, desired_counts)
            remaining_total = total_target - len(rows)
            remaining_for_mix = max(desired_counts[requested_mix] - current_counts[requested_mix], 1)
            request_n = min(batch_size, remaining_total, remaining_for_mix)
            prompt = build_prompt(prompt_template, request_n, next_numeric_id, domains, requested_mix, max_words)

            accepted_batch: list[dict[str, Any]] = []
            max_retries = int(text_config.get("max_retries_per_batch", 5))
            for attempt in range(1, max_retries + 1):
                try:
                    content = request_batch(client, text_config, prompt)
                except Exception as exc:
                    reject_counts["api_error"] += 1
                    if attempt >= max_retries:
                        raise RuntimeError(friendly_llm_error(exc, text_config)) from exc
                    time.sleep(min(2 ** attempt, 20))
                    continue
                candidates = parse_jsonl_response(content)
                for candidate in candidates:
                    if len(rows) + len(accepted_batch) >= total_target:
                        break
                    sanitized = sanitize_row(candidate, next_numeric_id + len(accepted_batch), requested_mix, domains)
                    if sanitized is None:
                        reject_counts["invalid_row"] += 1
                        continue
                    quality = quality_check(sanitized["text"], requested_mix, text_config)
                    if not quality.ok:
                        reject_counts[quality.reason] += 1
                        continue
                    text_key = normalize_for_dedup(sanitized["text"])
                    if text_key in seen_texts:
                        reject_counts["duplicate_text"] += 1
                        continue
                    seen_texts.add(text_key)
                    accepted_batch.append(sanitized)

                if accepted_batch:
                    break
                sleep_seconds = min(2 ** attempt, 20)
                time.sleep(sleep_seconds)

            if not accepted_batch:
                raise RuntimeError(
                    f"No acceptable rows returned for mix {requested_mix}. Rejections: {dict(reject_counts)}"
                )

            append_rows(output_path, accepted_batch)
            rows.extend(accepted_batch)
            for row in accepted_batch:
                current_counts[mix_key(row)] += 1
            next_numeric_id = len(rows) + 1
            if progress:
                progress.update(len(accepted_batch))
            else:
                print(f"Accepted {len(rows)}/{total_target}", file=sys.stderr)
    finally:
        if progress:
            progress.close()

    write_report(
        output_path,
        {
            "output_path": str(output_path),
            "target": total_target,
            "accepted": len(rows),
            "counts_by_mix": dict(current_counts),
            "reject_counts": dict(reject_counts),
        },
    )
    print(f"Wrote {len(rows)} rows to {output_path}")


if __name__ == "__main__":
    main()
