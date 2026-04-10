# -*- coding: utf-8 -*-
"""
Stage-dropout analyzer.

For each funnel stage N (1-9), identifies calls that stopped at that stage
(stage_reached == N) and asks GPT-4o to analyze what went wrong
and what improvements could help.

Usage:
    python analyze_stages.py [--max-samples N] [--output-file path]

Requires OPENAI_API_KEY environment variable.
"""

import sys
import io
# Force UTF-8 output on Windows
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

import argparse
import json
import os
import time
from typing import Optional

import openai

from generate_data import (
    STAGE_NAMES,
    compute_funnel,
    generate_dataset,
)
import fetch_data

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """Вы — аналитик качества обслуживания в колл-центре.
Вы анализируете транскрипции звонков в IVR-системе (интерактивный голосовой ответчик / робот)
на русском или казахском языке.

Ваша задача — на основе предоставленных транскрипций определить:
1. Конкретные причины, по которым клиенты не прошли данный этап воронки.
2. Паттерны поведения клиентов (растерянность, недовольство, технические проблемы и т.д.).
3. Практические рекомендации по улучшению.

Будьте конкретны, опирайтесь на текст транскрипций. Отвечайте на русском языке."""


def build_user_prompt(
    stage_n: int,
    stage_name: str,
    next_stage_name: str,
    dropout_count: int,
    total_reached: int,
    transcriptions: list[str],
) -> str:
    dropout_pct = round(100 * dropout_count / total_reached, 1) if total_reached else 0
    samples_text = "\n\n".join(
        f"--- Транскрипция {i+1} ---\n{t}" for i, t in enumerate(transcriptions)
    )

    return f"""## Анализ оттока на этапе {stage_n}: «{stage_name}»

Из клиентов, достигших этапа {stage_n} («{stage_name}»),
**{dropout_count} из {total_reached} ({dropout_pct}%)** не перешли на следующий этап
(«{next_stage_name}»).

Ниже приведены транскрипции звонков клиентов, которые застряли на этом этапе:

{samples_text}

---

Пожалуйста, проведите анализ и ответьте по следующему плану:

### 1. Выявленные причины оттока
Перечислите конкретные причины, опираясь на транскрипции.

### 2. Поведенческие паттерны клиентов
Какое поведение или реакции клиентов характерны для этого этапа?

### 3. Системные/технические проблемы (если есть)
Были ли технические сбои, неудобный интерфейс, непонятные подсказки?

### 4. Рекомендации по улучшению
Конкретные предложения: что изменить в скриптах, логике, верификации, тайм-аутах и т.д.

### 5. Ожидаемый эффект
Насколько можно снизить отток на этом этапе при реализации рекомендаций?"""


# ---------------------------------------------------------------------------
# Core analysis
# ---------------------------------------------------------------------------

def analyze_stage(
    client_: openai.OpenAI,
    stage_n: int,
    stage_name: str,
    next_stage_name: str,
    dropout_records: list[dict],
    reached_count: int,
    max_samples: int = 5,
) -> str:
    """Send dropout transcriptions to GPT-4o and return analysis text."""

    # Sample up to max_samples transcriptions
    sample = dropout_records[:max_samples]
    transcriptions = [r["transcription"] for r in sample]

    user_prompt = build_user_prompt(
        stage_n=stage_n,
        stage_name=stage_name,
        next_stage_name=next_stage_name,
        dropout_count=len(dropout_records),
        total_reached=reached_count,
        transcriptions=transcriptions,
    )

    response = client_.chat.completions.create(
        model="gpt-4o",
        max_tokens=2048,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
    )

    return response.choices[0].message.content or ""


def run_full_analysis(
    records: list[dict],
    max_samples: int = 5,
    output_file: Optional[str] = None,
) -> dict:
    """Run analysis for every stage with dropouts. Returns dict of results."""

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        sys.exit("ERROR: OPENAI_API_KEY environment variable not set.")

    client_ = openai.OpenAI(api_key=api_key)
    funnel = compute_funnel(records)

    results = {}

    for stage_n in range(1, 10):  # stages 1..9 (10 is success, no dropout)
        stage_name = STAGE_NAMES[stage_n]
        next_stage_name = STAGE_NAMES[stage_n + 1]

        dropout_records = [r for r in records if r["stage_reached"] == stage_n]
        reached_count = funnel[stage_n]["count"]

        if not dropout_records:
            print(f"  [Этап {stage_n}] Нет оттока — пропускаем.")
            continue

        dropout_pct = round(100 * len(dropout_records) / reached_count, 1) if reached_count else 0
        print(
            f"\n{'='*70}\n"
            f"Анализ этапа {stage_n}: «{stage_name}»\n"
            f"  Достигли: {reached_count}   |   Отсеялись: {len(dropout_records)} ({dropout_pct}%)\n"
            f"  Анализирую {min(len(dropout_records), max_samples)} транскрипций...",
            flush=True,
        )

        try:
            analysis = analyze_stage(
                client_=client_,
                stage_n=stage_n,
                stage_name=stage_name,
                next_stage_name=next_stage_name,
                dropout_records=dropout_records,
                reached_count=reached_count,
                max_samples=max_samples,
            )
        except openai.RateLimitError:
            print("  Превышен лимит запросов — ожидаем 30 секунд...")
            time.sleep(30)
            analysis = analyze_stage(
                client_=client_,
                stage_n=stage_n,
                stage_name=stage_name,
                next_stage_name=next_stage_name,
                dropout_records=dropout_records,
                reached_count=reached_count,
                max_samples=max_samples,
            )
        except Exception as e:
            analysis = f"[Ошибка анализа: {e}]"

        results[stage_n] = {
            "stage_name": stage_name,
            "next_stage_name": next_stage_name,
            "reached_count": reached_count,
            "dropout_count": len(dropout_records),
            "dropout_pct": dropout_pct,
            "analysis": analysis,
        }

        print(f"\n--- РЕЗУЛЬТАТ АНАЛИЗА ---\n{analysis}")

    # Save results
    if output_file:
        os.makedirs(os.path.dirname(output_file) if os.path.dirname(output_file) else ".", exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"\n\nРезультаты сохранены → {output_file}")

    return results


def print_summary(results: dict, funnel: dict) -> None:
    """Print a concise summary table."""
    print("\n" + "=" * 70)
    print("ИТОГОВАЯ СВОДКА ВОРОНКИ И РЕКОМЕНДАЦИЙ")
    print("=" * 70)

    for stage_n in range(1, 11):
        name = STAGE_NAMES[stage_n]
        count = funnel[stage_n]["count"]
        pct = funnel[stage_n]["pct_of_total"]
        print(f"  Этап {stage_n:2d} | {pct:5.1f}% | {count:4d} вызовов | {name}")

    print("\n" + "-" * 70)
    print("КЛЮЧЕВЫЕ ПРОБЛЕМЫ ПО ЭТАПАМ (краткое резюме):")
    print("-" * 70)

    for stage_n, data in sorted(results.items()):
        print(f"\n★ Этап {stage_n} ({data['stage_name']}) — отток {data['dropout_pct']}%:")
        # Print first 3 lines of analysis as a teaser
        lines = [l for l in data["analysis"].split("\n") if l.strip()]
        for line in lines[:4]:
            print(f"   {line}")
        if len(lines) > 4:
            print("   ...")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Анализ оттока на этапах воронки роботизации")
    parser.add_argument(
        "--max-samples",
        type=int,
        default=4,
        help="Максимальное кол-во транскрипций на этап (default: 4)",
    )
    parser.add_argument(
        "--output-file",
        type=str,
        default="output/analysis_results.json",
        help="Файл для сохранения результатов (default: output/analysis_results.json)",
    )
    parser.add_argument(
        "--source",
        type=str,
        choices=["json", "postgres"],
        default="json",
        help="Источник данных: 'json' (файл) или 'postgres' (БД Grafana). (default: json)",
    )
    parser.add_argument(
        "--data-file",
        type=str,
        default="data/synthetic_calls.json",
        help="JSON-файл с данными звонков, используется при --source json (default: data/synthetic_calls.json)",
    )
    args = parser.parse_args()

    # Load data from the chosen source
    if args.source == "postgres":
        print("Загружаем данные из PostgreSQL (см. fetch_data.py для настройки)...")
        records = fetch_data.fetch_records()
        print(f"Загружено {len(records)} звонков из БД.")
    elif os.path.exists(args.data_file):
        print(f"Загружаем данные из {args.data_file}...")
        with open(args.data_file, encoding="utf-8") as f:
            records = json.load(f)
    else:
        print("Файл данных не найден — генерируем синтетические данные...")
        records = generate_dataset(300)
        os.makedirs(os.path.dirname(args.data_file) if os.path.dirname(args.data_file) else ".", exist_ok=True)
        with open(args.data_file, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
        print(f"Сохранено → {args.data_file}")

    funnel = compute_funnel(records)
    print(f"\nЗагружено {len(records)} звонков.")

    print("\n=== ВОРОНКА РОБОТИЗАЦИИ ===")
    for stage_n, name in STAGE_NAMES.items():
        d = funnel[stage_n]
        print(f"  Этап {stage_n:2d}: {name:<45s} | {d['count']:4d} | {d['pct_of_total']:5.1f}%")

    print(f"\nНачинаем анализ (макс. {args.max_samples} транскрипций на этап)...")
    results = run_full_analysis(
        records=records,
        max_samples=args.max_samples,
        output_file=args.output_file,
    )

    print_summary(results, funnel)
