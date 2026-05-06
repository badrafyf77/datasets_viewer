#!/usr/bin/env python3
"""ASR text normalizer for Darija + French/English code-switching.

Transcription style:
- Darija stays in Arabic script.
- Clear French/English words stay in Latin script.
- French accents are kept by default: réunion, problème, téléphone.
- Latin internal apostrophes and hyphens are kept: l'application, didn't,
  rendez-vous.
- Noise, emojis, punctuation overload, Arabic diacritics, tatweel, and social
  letter stretching are removed.
- Darija particles are preserved; this normalizer does not translate Darija
  into Modern Standard Arabic.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any


ARABIC_DIACRITICS_RE = re.compile(
    "["
    "\u0610-\u061a"
    "\u064b-\u065f"
    "\u0670"
    "\u06d6-\u06ed"
    "]"
)
ZERO_WIDTH_RE = re.compile("[\u200b-\u200f\ufeff]")
WHITESPACE_RE = re.compile(r"\s+")
TAG_RE = re.compile(r"<[^>]+>|\[[^\]]+\]")
KNOWN_NOISE_PARENS_RE = re.compile(
    r"\((?:[^)]*(?:music|noise|applause|laugh|laughter|silence|inaudible|"
    r"موسيقى|ضجيج|تصفيق|ضحك|صمت)[^)]*)\)",
    flags=re.IGNORECASE,
)
SPEAKER_LAUGH_RE = re.compile(r"\bspeaker\s+(?:laughs?|laughing)\b", flags=re.IGNORECASE)
LATIN_LAUGH_RE = re.compile(r"\b(?:ha|he|h+a+h+a+){2,}\b", flags=re.IGNORECASE)
ARABIC_LAUGH_RE = re.compile(r"\b[هةخ]{3,}\b")
ARABIC_VOWEL_REPEAT_RE = re.compile(r"([اويى])\1+")
ARABIC_LONG_REPEAT_RE = re.compile(r"([\u0600-\u06ff])\1{2,}")
ARABIC_FINAL_REPEAT_RE = re.compile(r"([\u0600-\u06ff])\1+\b")

CHAR_REPLACEMENTS = str.maketrans(
    {
        "\u2018": "'",
        "\u2019": "'",
        "\u201a": "'",
        "\u201b": "'",
        "\u2032": "'",
        "`": "'",
        "\u00b4": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u201e": '"',
        "\u201f": '"',
        "\u2033": '"',
        "\u2010": "-",
        "\u2011": "-",
        "\u2012": "-",
        "\u2013": "-",
        "\u2014": "-",
        "\u2212": "-",
        "\u00a0": " ",
        "\u202f": " ",
        "\u2009": " ",
    }
)

ARABIC_LETTER_REPLACEMENTS = str.maketrans(
    {
        "\u0622": "\u0627",
        "\u0623": "\u0627",
        "\u0625": "\u0627",
        "\u0671": "\u0627",
        "\u0640": "",
    }
)

COMMON_DARIJA_WORDS = {
    # Question words and particles.
    "اشنو": "شنو",
    "شنوا": "شنو",
    "شنوة": "شنو",
    "شنويا": "شنو",
    "اشحال": "شحال",
    "شحالل": "شحال",
    "اشكون": "شكون",
    "واشي": "واش",
    "واششي": "واش",
    "باشي": "باش",
    "باشان": "باش",
    "بش": "باش",
    "حيث": "حيت",
    "حيتاش": "حيت",
    "حيتاشي": "حيت",
    "حيتش": "حيت",
    "حيتشي": "حيت",
    "حيتا": "حيت",
    "علاخاطر": "حيت",
    "علاخطر": "حيت",
    "لاخاطر": "حيت",
    "لخاطر": "حيت",
    "اوا": "وا",
    "وخا": "واخا",
    "يلاه": "يلا",
    "يالاه": "يلا",
    "يالا": "يلا",
    "ايلا": "الا",
    "فاشش": "فاش",
    "مليي": "ملي",
    "منينن": "منين",
    "مني": "منين",
    "حتىا": "حتى",
    "ولكنن": "ولكن",
    "راهه": "راه",
    "رااه": "راه",
    "راهوا": "راه",

    # Time words.
    "دبا": "دابا",
    "داب": "دابا",
    "ليوم": "اليوم",
    "اليومم": "اليوم",
    "غداا": "غدا",
    "لبارح": "البارح",
    "البارحح": "البارح",
    "الصباحح": "الصباح",
    "لعشية": "العشية",
    "العشيةة": "العشية",
    "دقيقةة": "دقيقة",
    "دقيقه": "دقيقة",
    "دقايقق": "دقايق",
    "سيمانةة": "سيمانة",
    "سيمانه": "سيمانة",
    "سبوع": "اسبوع",
    "سبوعع": "اسبوع",
    "شهرر": "شهر",
    "عامم": "عام",

    # Pronouns, demonstratives, and possessives.
    "هادا": "هاد",
    "هدا": "هاد",
    "هاذ": "هاد",
    "هدي": "هادي",
    "هذي": "هادي",
    "هادشيي": "هادشي",
    "داكشيي": "داكشي",
    "هناا": "هنا",
    "تمما": "تما",
    "تمة": "تما",
    "عندييا": "عندي",
    "عنديي": "عندي",
    "عندكك": "عندك",
    "عندكمم": "عندكم",
    "دياليي": "ديالي",
    "ديالكك": "ديالك",
    "ديالكمم": "ديالكم",
    "ديالوو": "ديالو",
    "ديالهاا": "ديالها",

    # Common verbs and aspect spellings.
    "بغيتى": "بغيت",
    "بغيتا": "بغيت",
    "بغيتوو": "بغيتو",
    "بغيناا": "بغينا",
    "بغيتت": "بغيت",
    "بغيتكك": "بغيتك",
    "بغا": "بغى",
    "يبغا": "يبغى",
    "تبغا": "تبغى",
    "نبغا": "نبغى",
    "كنبغي": "كانبغي",
    "كيبغي": "كايبغي",
    "كتبغي": "كاتبغي",
    "كيبغيو": "كايبغيو",
    "كتبغيو": "كاتبغيو",
    "غاديي": "غادي",
    "غاادي": "غادي",
    "غادا": "غادية",
    "غاديه": "غادية",
    "غنمشي": "غانمشي",
    "غنمشيو": "غانمشيو",
    "غنمشييو": "غانمشيو",
    "نمشيي": "نمشي",
    "نمشيوو": "نمشيو",
    "نمشييو": "نمشيو",
    "كنمشي": "كانمشي",
    "تنمشي": "كانمشي",
    "كاندير": "كندير",
    "كانديرو": "كنديرو",
    "كيدير": "كايدير",
    "كتدير": "كاتدير",
    "كيديرو": "كايديرو",
    "كتديرو": "كاتديرو",
    "كنقرا": "كانقرا",
    "تنقرا": "كانقرا",
    "كيقرا": "كايقرا",
    "كتقرا": "كاتقرا",
    "كيقراو": "كايقراو",
    "كناكل": "كاناكل",
    "كياكل": "كايكل",
    "كتاكل": "كاتاكل",
    "كنشرب": "كانشرب",
    "كيشرب": "كايشرب",
    "كتشرب": "كاتشرب",
    "جيتى": "جيتي",
    "جيتوو": "جيتو",
    "جيناا": "جينا",

    # Negation variants.
    "ماشيش": "ماشي",
    "ماشيي": "ماشي",
    "ماكينش": "ماكاينش",
    "مكاينش": "ماكاينش",
    "ماكيناش": "ماكايناش",
    "مكايناش": "ماكايناش",
    "مبقاش": "مابقاش",
    "مبقاتش": "مابقاتش",
    "مبغيتش": "مابغيتش",
    "مقدرتش": "ماقدرتش",
    "معرفتش": "ماعرفتش",

    # Common adjectives and nouns.
    "امزيان": "مزيان",
    "مزيانن": "مزيان",
    "مزيانةة": "مزيانة",
    "مزيانه": "مزيانة",
    "زوينن": "زوين",
    "زوينةة": "زوينة",
    "زوينه": "زوينة",
    "خايبب": "خايب",
    "خايبةة": "خايبة",
    "خايبه": "خايبة",
    "واعرر": "واعر",
    "بزافف": "بزاف",
    "بزااف": "بزاف",
    "بززاف": "بزاف",
    "شويةة": "شوية",
    "شويا": "شوية",
    "شوييا": "شوية",
    "شويّة": "شوية",
    "صافيى": "صافي",
    "صافييا": "صافي",
    "صافييه": "صافي",
    "دغييا": "دغيا",
    "دغية": "دغيا",
    "بزربةة": "بزربة",
    "بزربا": "بزربة",
    "خدامم": "خدام",
    "خدامةة": "خدامة",
    "خدامه": "خدامة",
    "خدامينن": "خدامين",
    "خدمةة": "خدمة",
    "خدمه": "خدمة",
    "بلاصةة": "بلاصة",
    "بلاصا": "بلاصة",
    "بلاصه": "بلاصة",
    "دريي": "دري",
    "دريةة": "درية",
    "دريه": "درية",
    "ولدد": "ولد",
    "بنتت": "بنت",
    "دارر": "دار",
    "الدارر": "الدار",
    "مشروعع": "مشروع",

    # Social formulae and frequent personal words.
    "خوياا": "خويا",
    "خوييا": "خويا",
    "اختيي": "اختي",
    "صاحبيي": "صاحبي",
    "صحابيى": "صحابي",
    "صحابيي": "صحابي",
    "عافاكك": "عافاك",
    "عافاكوم": "عافاكم",
    "شكراا": "شكرا",
    "مرسيي": "مرسي",
    "بسلامةة": "بسلامة",
    "بسلامه": "بسلامة",
    "اللهه": "الله",
    "انشاءالله": "ان شاء الله",
    "انشاالله": "ان شاء الله",
    "انشالله": "ان شاء الله",
    "ماشاءالله": "ما شاء الله",
    "مشاءالله": "ما شاء الله",
    "الحمدلله": "الحمد لله",
    "سمحلي": "سمح ليا",
    "سمحليا": "سمح ليا",
    "سمحوليا": "سمحو ليا",

    # High-confidence French/English code-switch words often written in Arabic.
    "رنديفو": "rendez-vous",
    "رونديفو": "rendez-vous",
    "بروجيت": "projet",
    "بروجي": "projet",
    "كليون": "client",
    "كلون": "client",
    "تيليفون": "téléphone",
    "تليفون": "téléphone",
    "تلفون": "téléphone",
    "ريونيون": "réunion",
    "ابليكاسيون": "application",
    "ابليكيشن": "application",
    "كونط": "compte",
}


def squeeze_spaces(text: str) -> str:
    return WHITESPACE_RE.sub(" ", text).strip()


def replace_spoken_symbols(text: str, lang: str | None = None) -> str:
    lang = (lang or "").lower()
    if "%" in text:
        replacement = " pour cent " if lang.startswith("fr") else " percent "
        text = text.replace("%", replacement)
    if "&" in text:
        replacement = " et " if lang.startswith("fr") else " and "
        text = text.replace("&", replacement)
    return text


def strip_latin_accents_text(text: str) -> str:
    decomposed = unicodedata.normalize("NFKD", text)
    without_marks = "".join(ch for ch in decomposed if unicodedata.category(ch) != "Mn")
    return unicodedata.normalize("NFKC", without_marks)


def is_latin_alnum(char: str) -> bool:
    if not char.isalnum():
        return False
    return "LATIN" in unicodedata.name(char, "")


def is_internal_latin_joiner(text: str, index: int) -> bool:
    if text[index] not in {"'", "-"}:
        return False
    if index == 0 or index == len(text) - 1:
        return False
    return is_latin_alnum(text[index - 1]) and is_latin_alnum(text[index + 1])


def remove_non_speech_text(text: str) -> str:
    text = TAG_RE.sub(" ", text)
    text = KNOWN_NOISE_PARENS_RE.sub(" ", text)
    text = SPEAKER_LAUGH_RE.sub(" ", text)
    text = LATIN_LAUGH_RE.sub(" ", text)
    text = ARABIC_LAUGH_RE.sub(" ", text)
    return text


def normalize_ta_marbuta(text: str, style: str) -> str:
    if style == "keep":
        return text
    if style == "h":
        return re.sub(r"ة(?=$|[\s.,;:!?؟،؛\]\)\}])", "ه", text)
    raise ValueError("ta_marbuta_style must be 'keep' or 'h'")


def normalize_arabic_repeats(text: str) -> str:
    text = ARABIC_VOWEL_REPEAT_RE.sub(r"\1", text)
    text = ARABIC_LONG_REPEAT_RE.sub(r"\1", text)
    text = ARABIC_FINAL_REPEAT_RE.sub(r"\1", text)
    return text


def normalize_common_darija_words(text: str) -> str:
    tokens = text.split(" ")
    return " ".join(COMMON_DARIJA_WORDS.get(token, token) for token in tokens)


def remove_punctuation_keep_latin_joiners(text: str) -> str:
    chars: list[str] = []
    for index, char in enumerate(text):
        category = unicodedata.category(char)
        if category[0] in {"L", "N", "M"}:
            chars.append(char)
        elif char.isspace():
            chars.append(" ")
        elif is_internal_latin_joiner(text, index):
            chars.append(char)
        elif category[0] in {"P", "S"}:
            chars.append(" ")
        else:
            chars.append(" ")
    return squeeze_spaces("".join(chars))


def normalize_asr_text(
    text: Any,
    lang: str | None = None,
    *,
    strip_latin_accents: bool = False,
    ta_marbuta_style: str = "keep",
) -> str:
    if text is None:
        return ""
    text = unicodedata.normalize("NFKC", str(text))
    text = text.translate(CHAR_REPLACEMENTS)
    text = ZERO_WIDTH_RE.sub("", text)
    text = remove_non_speech_text(text)
    text = replace_spoken_symbols(text, lang)
    text = text.casefold()
    text = text.translate(ARABIC_LETTER_REPLACEMENTS)
    text = normalize_ta_marbuta(text, ta_marbuta_style)
    text = ARABIC_DIACRITICS_RE.sub("", text)
    text = normalize_arabic_repeats(text)
    text = remove_punctuation_keep_latin_joiners(text)
    text = normalize_common_darija_words(text)
    if strip_latin_accents:
        text = strip_latin_accents_text(text)
    return squeeze_spaces(text)


def normalize_batch(batch: dict[str, Any], text_field: str = "text", lang_field: str = "lang") -> dict[str, Any]:
    batch["normalized_text"] = normalize_asr_text(batch.get(text_field, ""), batch.get(lang_field))
    return batch


if __name__ == "__main__":
    examples = [
        ("Peu de temps après, j'entendis les graviers crisser.", "fr"),
        ("I didn't think so either.", "en"),
        ("السلام، اليوم عندي rendez-vous مع le client باش نهضرو على l’avancement ديال projet، ومن بعد غادي نديرو une réunion.", "ary-fr"),
        ("سلاااام خــــويا", "ary"),
        ("[موسيقى] السلام عليكم hahaha شنو خبارك", "ary"),
    ]
    for value, lang in examples:
        print(normalize_asr_text(value, lang))
