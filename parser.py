#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
all_pribors_crawler.py

Запуск примеры:
    python all_pribors_crawler.py --rn 91851-24 --headless True --out data.csv
    python all_pribors_crawler.py --rn 85773-22 --date 2025-09-17
    python all_pribors_crawler.py --rn 85773-22 --date-range 2025-09-01:2025-09-17
"""

import argparse
import csv
import time
import re
import logging
import os
import json
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timedelta
from selenium.webdriver.chrome.service import Service
#from webdriver_manager.chrome import ChromeDriverManager 
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    StaleElementReferenceException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from utils.filter_dupls import cleanup_incomplete_pages

# --------------------
# Логгирование
# --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("crawler.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --------------------
# Константы (можно менять)
# --------------------
PAGE_BUTTONS_XPATH = "//*[@class='page-link shadow-none']"
CARD_BUTTON_XPATH = "//*[@class='btn btn-primary btn-sm dropdown-toggle']"
OPENED_CARD_XPATH = "//*[@class='border rounded mb-2 p-2 border-warning shadow']"
TOTAL_COUNT_XPATH = "//*[@class='text-muted text-end']/strong"

DEFAULT_WAIT = 2
CLICK_RETRIES = 3
DELAY_AFTER_CLICK = 2
DELAY_BETWEEN_PAGES = 2
MAX_ATTEMPTS_PER_PAGE = 3  # даём по 3 попытки на страницу
CARDS_PER_PAGE = 20  # ожидаемое количество карточек на «полной» странице

# Название поля в карточке, которое указывает дату поверки (как в CSV)
DATE_FIELD_NAME = "Дата поверки"


def safe_int(s):
    """Преобразует строку в int, убирая все нецифровые символы."""
    try:
        return int(re.sub(r'\D', '', str(s)))
    except Exception:
        return None


class AllPriborsCrawler:
    """
    Краулер с поддержкой восстановления прогресса.
    Формат progress JSON (верхний уровень — rn):
    {
      "<rn>": {
         "dates": {
            "ALL" или "YYYY-MM-DD" или "YYYY-MM-DD:YYYY-MM-DD": {
               "last_page": <int>,
               "collected": <int>,
               "page_stats": {
                   "1": {"cards_collected": 20},
                   "2": {"cards_collected": 19},
                   ...
               }
            }
         },
         "updated_at": "..."
      },
      ...
    }
    """

    def __init__(self, rn: str, headless: bool = True, out: str = "output.csv",
                 date: Optional[str] = None, date_range: Optional[str] = None):
        self.rn = rn
        self.base_url = f"https://all-pribors.ru/verification-results?rn={rn}"
        self.out = out
        self.headless = headless
        self.date = date
        self.date_range = date_range

        self.driver = None
        self.wait = None

        # CSV in-memory
        self.records: List[Dict[str, Any]] = []
        self.fieldnames: List[str] = []
        self.collected = 0

        # progress structures
        self.progress_all: Dict[str, Any] = {}
        self.progress: Dict[str, Any] = {"dates": {}, "updated_at": None}

        # was csv present on start? (флаг восстановления)
        self.csv_existed_on_start = os.path.exists(self.out)

        # load progress and CSV if present
        self._load_progress_and_csv_if_present()

        # open CSV for rewriting (we keep records in memory and rewrite fully to keep header sync)
        self.csvfile = open(self.out, "w", newline="", encoding="utf-8")
        if self.fieldnames:
            try:
                writer = csv.DictWriter(self.csvfile, fieldnames=self.fieldnames)
                writer.writeheader()
                if self.records:
                    writer.writerows(self.records)
                    self.csvfile.flush()
            except Exception as e:
                logger.error(f"[CSV] Ошибка при инициализации CSV: {e}")

    # --------------------
    # Progress helpers
    # --------------------
    def _progress_path(self) -> str:
        base, _ = os.path.splitext(self.out)
        return base + ".progress.json"

    def _load_progress_and_csv_if_present(self):
        """Загружает прогресс (весь файл) и CSV-строки для текущего rn (если есть)."""
        ppath = self._progress_path()
        if os.path.exists(ppath):
            try:
                with open(ppath, "r", encoding="utf-8") as pf:
                    self.progress_all = json.load(pf) or {}
            except Exception as e:
                logger.error(f"[PROGRESS] Не удалось прочитать {ppath}: {e}. Продолжаем без прогресса.")
                self.progress_all = {}

        # если для текущего rn есть запись — используем её
        if self.rn in self.progress_all:
            self.progress = self.progress_all[self.rn]
            logger.info(f"[PROGRESS] Загружен прогресс для rn={self.rn}")
        else:
            # инициализируем структуру под rn
            self.progress = {"dates": {}, "updated_at": None}

        # загрузка CSV (если есть) — для реконструкции page_stats
        if os.path.exists(self.out):
            try:
                with open(self.out, newline='', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    fieldnames = reader.fieldnames or []
                    rows_for_rn = []
                    for row in reader:
                        if row.get("rn") == self.rn:
                            rows_for_rn.append(row)
                    if rows_for_rn:
                        logger.info(f"[CSV] Найдены существующие записи для rn={self.rn}: {len(rows_for_rn)} строк")
                        rows_for_rn = cleanup_incomplete_pages(rows_for_rn)
                        self.records = rows_for_rn
                        self.fieldnames = fieldnames
                        self.collected = len(rows_for_rn)
                        # rebuild page_stats from CSV (CSV — источник истины при восстановлении)
                        self._rebuild_page_stats_from_records()
                    else:
                        logger.info(f"[CSV] Файл {self.out} есть, но записей для rn={self.rn} не найдено.")
            except Exception as e:
                logger.error(f"[CSV] Ошибка чтения {self.out}: {e}. Продолжаем без предварительной загрузки CSV.")
        else:
            self.records = []
            self.fieldnames = []

    def _save_progress_all(self):
        """Сохраняет весь progress_all в JSON-файл (atomic)."""
        path = self._progress_path()
        try:
            self.progress["updated_at"] = datetime.now().isoformat()
            # put current progress under rn
            self.progress_all[self.rn] = self.progress
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.progress_all, f, ensure_ascii=False, indent=2)
            os.replace(tmp, path)
            logger.debug(f"[PROGRESS] Сохранён {path}")
        except Exception as e:
            logger.error(f"[PROGRESS] Ошибка при сохранении прогресса: {e}")

    def _parse_date_range(self) -> Optional[Tuple[datetime.date, datetime.date]]:
        """Парсит self.date_range в кортеж (start,end) если задан, иначе None."""
        if not self.date_range:
            return None
        try:
            parts = self.date_range.split(":")
            if len(parts) != 2:
                return None
            start = datetime.strptime(parts[0], "%Y-%m-%d").date()
            end = datetime.strptime(parts[1], "%Y-%m-%d").date()
            return (start, end)
        except Exception:
            return None

    def _get_date_key_for_mode(self, date: Optional[str]) -> str:
        """
        Возвращает ключ прогресса:
         - если передан диапазон дат при запуске -> используем саму строку date_range как ключ
         - если передана одна дата -> используем её
         - если ничего -> 'ALL'
        """
        if self.date_range:
            return self.date_range
        if self.date:
            return self.date
        return "ALL"

    def _get_progress_for_date_key(self, date_key: str) -> Dict[str, Any]:
        d = self.progress.setdefault("dates", {}).setdefault(date_key, {"last_page": 0, "collected": 0, "page_stats": {}})
        if "page_stats" not in d or not isinstance(d.get("page_stats"), dict):
            d["page_stats"] = {}
        return d

    def _rebuild_page_stats_from_records(self):
        """
        Реконструирует page_stats из self.records.
        Важно: если при запуске указан date_range — все записи с Датой поверки внутри диапазона
        попадают в один ключ = строка date_range. Если указана одна дата — учитываем только записи с этой датой.
        Если дата не указана — все записи относятся к 'ALL'.
        """
        logger.info("[PROGRESS] Реконструкция page_stats из CSV...")
        # prepare date_range bounds if present
        range_bounds = self._parse_date_range()

        # temporary aggregation: date_key -> page_str -> count
        agg: Dict[str, Dict[str, int]] = {}

        for rec in self.records:
            # page normalization
            page_val = rec.get("page")
            try:
                p = int(page_val) if page_val not in (None, "") else None
            except Exception:
                p = None
            if p is None:
                # если нет данных о странице — пропускаем при расчёте page_stats
                continue

            # determine record's date (as date object) if possible
            rec_date_val = rec.get(DATE_FIELD_NAME)
            rec_date = None
            if rec_date_val:
                for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
                    try:
                        rec_date = datetime.strptime(rec_date_val.strip(), fmt).date()
                        break
                    except Exception:
                        rec_date = None

            # decide which date_key this rec belongs to
            if self.date_range and range_bounds:
                start, end = range_bounds
                if rec_date and start <= rec_date <= end:
                    date_key = self.date_range
                else:
                    # запись вне диапазона — не относим её к текущему date_range
                    # NOTE: мы НЕ удаляем такие записи из CSV, просто не включаем в агрегат для date_range
                    continue
            elif self.date:
                # single date mode: include only recs matching the date
                try:
                    target = datetime.strptime(self.date, "%Y-%m-%d").date()
                    if rec_date and rec_date == target:
                        date_key = self.date
                    else:
                        # не та дата — пропускаем
                        continue
                except Exception:
                    continue
            else:
                # ALL mode: все записи относим в ALL
                date_key = "ALL"

            agg.setdefault(date_key, {}).setdefault(str(p), 0)
            agg[date_key][str(p)] += 1

        # merge agg into self.progress
        for date_key, pages_map in agg.items():
            pd = self._get_progress_for_date_key(date_key)
            pd["page_stats"] = {}
            for p_str, cnt in pages_map.items():
                pd["page_stats"][p_str] = {"cards_collected": int(cnt)}
            # recompute collected and last_page
            try:
                pd["collected"] = sum(int(v.get("cards_collected", 0)) for v in pd["page_stats"].values())
                pd["last_page"] = max(int(k) for k in pd["page_stats"].keys()) if pd["page_stats"] else pd.get("last_page", 0)
            except Exception:
                pass

        # save progress
        self._save_progress_all()
        logger.info("[PROGRESS] Реконструкция завершена.")

    # --------------------
    # Driver lifecycle
    # --------------------
    def _init_driver(self):
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        prefs = {"profile.managed_default_content_settings.images": 2}
        chrome_options.add_experimental_option("prefs", prefs)
        self.driver = webdriver.Chrome(options=chrome_options)
        #self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        self.wait = WebDriverWait(self.driver, DEFAULT_WAIT)
        logger.info("[DRIVER] Chrome инициализирован")

    def _restart_driver(self):
        try:
            if self.driver:
                self.driver.quit()
        except Exception:
            pass
        time.sleep(0.3)
        self._init_driver()

    # --------------------
    # Date helpers
    # --------------------
    def _generate_date_list(self) -> List[Optional[str]]:
        """
        Возвращает список дат ('YYYY-MM-DD') для обхода.
        ВАЖНО: если задан date_range — мы всё ещё возвращаем список отдельных дат (для обхода сайта),
        но ключ прогресса будет равен строке date_range (см. _get_date_key_for_mode).
        """
        if self.date and self.date_range:
            logger.warning("[DATE] И date, и date-range заданы одновременно — беру date_range приоритетно.")
        if self.date_range:
            try:
                parts = self.date_range.split(":")
                if len(parts) != 2:
                    raise ValueError("date-range должен быть в формате YYYY-MM-DD:YYYY-MM-DD")
                start = datetime.strptime(parts[0], "%Y-%m-%d").date()
                end = datetime.strptime(parts[1], "%Y-%m-%d").date()
                if end < start:
                    raise ValueError("end_date < start_date в date-range")
                days = (end - start).days
                return [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days + 1)]
            except Exception as e:
                logger.error(f"[DATE] Ошибка парсинга date-range: {e}")
                return []
        if self.date:
            try:
                datetime.strptime(self.date, "%Y-%m-%d")
                return [self.date]
            except Exception:
                logger.error("[DATE] Неверный формат date. Ожидается YYYY-MM-DD")
                return []
        return [None]

    # --------------------
    # URL + навигация
    # --------------------
    def _build_url_for(self, page_num: int, date: Optional[str]) -> str:
        params = f"rn={self.rn}"
        if date:
            params += f"&date={date}"
        base = f"https://all-pribors.ru/verification-results?{params}"
        if page_num == 1:
            return base
        return f"{base}&page={page_num}"

    def _navigate_to_page(self, page_num: int, date: Optional[str], page_load_timeout: int = 30):
        url = self._build_url_for(page_num, date)
        logger.info(f"[NAV] Открываю: {url}")
        if not self.driver:
            self._init_driver()
        try:
            self.driver.set_page_load_timeout(page_load_timeout)
            self.driver.get(url)
        except TimeoutException:
            logger.error(f"[NAV] Таймаут загрузки: {url}. Рестарт драйвера.")
            try:
                self._restart_driver()
                self.driver.set_page_load_timeout(page_load_timeout)
                self.driver.get(url)
            except TimeoutException:
                logger.error("[NAV] Повторный таймаут после рестарта. Жду 5 минут.")
                time.sleep(300)
                try:
                    self._restart_driver()
                    self.driver.set_page_load_timeout(page_load_timeout)
                    self.driver.get(url)
                except Exception as e:
                    logger.error(f"[NAV] Не удалось загрузить страницу после 2 рестартов: {e}")
                    raise
            except Exception as e:
                logger.error(f"[NAV] Ошибка после рестарта: {e}")
                raise
        except WebDriverException as e:
            logger.error(f"[NAV] WebDriverException: {e}")
            raise
        time.sleep(1)

    # --------------------
    # Scraping helpers
    # --------------------
    def get_total_found(self) -> int:
        try:
            el = self.wait.until(EC.presence_of_element_located((By.XPATH, TOTAL_COUNT_XPATH)))
            return safe_int(el.text.strip())
        except TimeoutException:
            return 0

    def _get_numeric_pages_from_page(self) -> List[int]:
        try:
            els = self.driver.find_elements(By.XPATH, PAGE_BUTTONS_XPATH)
            nums = []
            for e in els:
                txt = e.text.strip()
                if txt.isdigit():
                    nums.append(int(txt))
            if not nums:
                return [1]
            max_page = max(nums)
            return list(range(1, max_page + 1))
        except Exception:
            return [1]

    def _click_button_with_retry(self, button_element):
        for attempt in range(1, CLICK_RETRIES + 1):
            try:
                self.driver.execute_script(
                    "arguments[0].scrollIntoView({block:'center', inline:'nearest'});", button_element
                )
                time.sleep(0.5)
                button_element.click()
                return True
            except (ElementClickInterceptedException, StaleElementReferenceException, WebDriverException) as e:
                logger.debug(f"[CLICK] Попытка {attempt} не удалась: {e}")
                time.sleep(0.8 * attempt)
                try:
                    buttons = self.driver.find_elements(By.XPATH, CARD_BUTTON_XPATH)
                    if buttons:
                        button_element = buttons[0]
                except Exception:
                    pass
        return False

    def _extract_from_opened_card(self, card_element):
        record = {"rn": self.rn}
        try:
            rows = card_element.find_elements(By.XPATH, ".//tr")
        except Exception:
            rows = []
        for row in rows:
            try:
                th = row.find_element(By.TAG_NAME, "th").text.strip().replace("\n", "")
                td = row.find_element(By.TAG_NAME, "td").text.strip().replace("\n", "")
                if th:
                    record[th] = td
            except Exception:
                continue
        return record

    # --------------------
    # CSV helpers
    # --------------------
    def _write_record(self, record: dict):
        # update headers
        new_keys = [k for k in record.keys() if k not in self.fieldnames]
        if new_keys:
            self.fieldnames.extend(new_keys)
            logger.info(f"[CSV] Новые поля: {new_keys}")
        if "page" not in self.fieldnames:
            self.fieldnames.append("page")

        self.records.append(record)

        try:
            self.csvfile.seek(0)
            self.csvfile.truncate()
            writer = csv.DictWriter(self.csvfile, fieldnames=self.fieldnames)
            writer.writeheader()
            for r in self.records:
                writer.writerow(r)
            self.csvfile.flush()
        except Exception as e:
            logger.error(f"[CSV] Ошибка записи: {e}")
        self.collected = len(self.records)
        logger.info(f"[CSV] Сохранено (в памяти): {self.collected}")

    def _count_records_for_date_and_page(self, date_key: str, page_num: int) -> int:
        cnt = 0
        for rec in self.records:
            # normalize page
            page_val = rec.get("page")
            try:
                p = int(page_val) if page_val not in (None, "") else None
            except Exception:
                p = None
            if p != page_num:
                continue
            # determine rec_date_key according to startup mode (date_range/date/ALL)
            rec_date_val = rec.get(DATE_FIELD_NAME)
            rec_date = None
            if rec_date_val:
                for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
                    try:
                        rec_date = datetime.strptime(rec_date_val.strip(), fmt).date()
                        break
                    except Exception:
                        rec_date = None
            # mapping to date_key
            if self.date_range:
                bounds = self._parse_date_range()
                if bounds and rec_date and bounds[0] <= rec_date <= bounds[1]:
                    rec_date_key = self.date_range
                else:
                    # not in range -> skip
                    continue
            elif self.date:
                try:
                    target = datetime.strptime(self.date, "%Y-%m-%d").date()
                    rec_date_key = self.date if (rec_date and rec_date == target) else None
                except Exception:
                    rec_date_key = None
            else:
                rec_date_key = "ALL"
            if rec_date_key == date_key:
                cnt += 1
        return cnt

    # --------------------
    # Main crawl
    # --------------------
    def crawl(self):
        try:
            dates_to_process = self._generate_date_list()
            if not dates_to_process:
                logger.error("[DATE] Нет дат для обработки.")
                return

            # init driver
            self._init_driver()
            logger.info(f"[START] rn={self.rn}. Даты: {dates_to_process}. CSV existed on start: {self.csv_existed_on_start}")

            for date in dates_to_process:
                # choose date_key according to user's rule:
                date_key = self._get_date_key_for_mode(date)
                prog = self._get_progress_for_date_key(date_key)
                logger.info(f"[DATE] Обрабатываем: {date or 'ALL'} -> прогресс-ключ = '{date_key}'")

                # open first page to get pagination
                try:
                    self._navigate_to_page(1, date)
                except Exception as e:
                    logger.error(f"[DATE {date}] Не удалось открыть первую страницу: {e}. Пропускаю.")
                    continue

                total_expected = self.get_total_found()
                logger.info(f"[INFO] Ожидаемое количество записей: {total_expected}")

                all_pages = self._get_numeric_pages_from_page() or [1]
                logger.info(f"[INFO] Страницы: {all_pages}")

                # decide pages to iterate:
                existing_stats = prog.get("page_stats", {}) or {}
                pages_with_incomplete = [int(p) for p, v in existing_stats.items() if int(v.get("cards_collected", 0)) < CARDS_PER_PAGE]
                pages_no_stats = [p for p in all_pages if str(p) not in existing_stats]
                pages_to_iterate = sorted(set(pages_with_incomplete + pages_no_stats))

                # if there is nothing to iterate but last_page < max -> resume forward
                if not pages_to_iterate:
                    start_page = prog.get("last_page", 0) + 1
                    pages_to_iterate = [p for p in all_pages if p >= start_page]

                if not pages_to_iterate:
                    logger.info(f"[RESUME] Нет страниц для обработки по ключу {date_key}.")
                    continue

                logger.info(f"[ITER] Страницы для обработки: {pages_to_iterate}")

                for page_num in pages_to_iterate:
                    # re-evaluate current count from memory (CSV is source of truth)
                    existing_count = int(existing_stats.get(str(page_num), {}).get("cards_collected", 0))
                    # last page (may legitimately have <CARDS_PER_PAGE)
                    last_page_num = max(all_pages) if all_pages else page_num
                    if existing_count >= CARDS_PER_PAGE and page_num != last_page_num:
                        logger.info(f"[SKIP] Страничка {page_num} уже имеет {existing_count} карточек -> пропускаю.")
                        if page_num > prog.get("last_page", 0):
                            prog["last_page"] = page_num
                            prog["collected"] = self.collected
                            self._save_progress_all()
                        continue

                    page_success = False
                    for attempt in range(1, MAX_ATTEMPTS_PER_PAGE + 1):
                        try:
                            if page_num == 1:
                                logger.info(f"--- Обрабатываю страницу {page_num} (уже открыта) ---")
                            else:
                                logger.info(f"--- Переход к странице {page_num} (попытка {attempt}/{MAX_ATTEMPTS_PER_PAGE}) ---")
                                self._navigate_to_page(page_num, date)
                                time.sleep(DELAY_BETWEEN_PAGES)

                            before_records_count = len(self.records)

                            loop_guard = 0
                            while True:
                                loop_guard += 1
                                if loop_guard > 2000:
                                    logger.warning("[LOOP] Защитный предел итераций на странице достигнут.")
                                    break

                                try:
                                    buttons = self.driver.find_elements(By.XPATH, CARD_BUTTON_XPATH)
                                except Exception:
                                    buttons = []

                                if not buttons:
                                    logger.debug("[PAGE] Кнопок не найдено — страница обработана.")
                                    break

                                btn = buttons[0]
                                try:
                                    before_cards_count = len(self.driver.find_elements(By.XPATH, OPENED_CARD_XPATH))
                                except Exception:
                                    before_cards_count = 0

                                clicked = self._click_button_with_retry(btn)
                                if not clicked:
                                    logger.warning("[CLICK] Не удалось кликнуть — пробую следующую.")
                                    time.sleep(0.5)
                                    continue

                                try:
                                    WebDriverWait(self.driver, DEFAULT_WAIT + 5).until(
                                        lambda d: len(d.find_elements(By.XPATH, OPENED_CARD_XPATH)) > before_cards_count
                                    )
                                except TimeoutException:
                                    logger.warning("[WARN] Новая карточка не появилась вовремя — пропускаю этот клик.")
                                    time.sleep(0.5)
                                    continue

                                try:
                                    cards = self.driver.find_elements(By.XPATH, OPENED_CARD_XPATH)
                                    if not cards:
                                        logger.warning("[WARN] После клика карточек нет.")
                                        time.sleep(0.5)
                                        continue
                                    card_el = cards[-1]
                                except Exception:
                                    logger.exception("[ERROR] Ошибка при получении карточки.")
                                    time.sleep(0.5)
                                    continue

                                try:
                                    WebDriverWait(self.driver, DEFAULT_WAIT).until(
                                        lambda d, el=card_el: len(el.find_elements(By.XPATH, ".//tr")) > 0
                                    )
                                except TimeoutException:
                                    logger.debug("[DEBUG] В карточке нет строк таблицы за отведённое время.")

                                rec = self._extract_from_opened_card(card_el)
                                rec["page"] = page_num
                                # if date filter used and rec lacks DATE_FIELD_NAME -> fill it, helps counting
                                if date and (DATE_FIELD_NAME not in rec or not rec.get(DATE_FIELD_NAME)):
                                    rec[DATE_FIELD_NAME] = date
                                self._write_record(rec)

                                time.sleep(DELAY_AFTER_CLICK)

                                # stop early if we collected enough for this page
                                cnt_now = self._count_records_for_date_and_page(date_key, page_num)
                                if cnt_now >= CARDS_PER_PAGE:
                                    logger.info(f"[PAGE] Достигнут порог {CARDS_PER_PAGE} карточек для страницы {page_num}.")
                                    break

                            page_success = True
                            added_now = len(self.records) - before_records_count
                            total_for_page = self._count_records_for_date_and_page(date_key, page_num)

                            # update progress
                            prog = self._get_progress_for_date_key(date_key)
                            ps = prog.setdefault("page_stats", {})
                            ps[str(page_num)] = {"cards_collected": int(total_for_page)}
                            prog["last_page"] = max(prog.get("last_page", 0), page_num)
                            prog["collected"] = self.collected
                            self._save_progress_all()
                            logger.info(f"[PROGRESS] Страница {page_num}: добавлено {added_now}, всего на странице {total_for_page}")
                            break
                        except WebDriverException as e:
                            logger.error(f"[PAGE {page_num}] WebDriverException (attempt {attempt}): {e}")
                            try:
                                self._restart_driver()
                                self._navigate_to_page(page_num, date)
                                time.sleep(DELAY_BETWEEN_PAGES)
                            except Exception as e2:
                                logger.error(f"[PAGE {page_num}] Ошибка после рестарта: {e2}")
                                continue

                    if page_success:
                        logger.info(f"[PAGE {page_num}] Успешно обработана.")
                    else:
                        logger.error(f"[PAGE {page_num}] Не удалось обработать после {MAX_ATTEMPTS_PER_PAGE} попыток.")
                        prog = self._get_progress_for_date_key(date_key)
                        prog["collected"] = self.collected
                        self._save_progress_all()

                logger.info(f"[DATE] Закончена обработка {date or 'ALL'}. Собрано всего {self.collected}")

            # итоговая проверка
            self._report_and_check()

        finally:
            self.close()

    def _report_and_check(self):
        logger.info(f"[REPORT] Всего собрано записей: {self.collected}")
        for date_key, dinfo in self.progress.get("dates", {}).items():
            page_stats = dinfo.get("page_stats", {}) or {}
            if not page_stats:
                continue
            try:
                max_page = max(int(p) for p in page_stats.keys())
            except Exception:
                max_page = None
            incomplete = []
            for p_str, info in page_stats.items():
                p = int(p_str)
                if max_page is not None and p == max_page:
                    continue
                if int(info.get("cards_collected", 0)) < CARDS_PER_PAGE:
                    incomplete.append((p, int(info.get("cards_collected", 0))))
            if incomplete:
                logger.warning(f"[CHECK] Для ключа {date_key} есть неполные страницы (кроме последней): {incomplete}")
            else:
                logger.info(f"[CHECK] Для ключа {date_key} все страницы (кроме последней) полные ({CARDS_PER_PAGE}).")

    def close(self):
        try:
            if self.driver:
                self.driver.quit()
        except Exception:
            pass
        try:
            if hasattr(self, "csvfile") and not self.csvfile.closed:
                self.csvfile.close()
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser(description="Crawler for all-pribors.ru verification-results")
    parser.add_argument("--rn", required=True, help="параметр rn, например 91851-24")
    parser.add_argument("--out", default="output.csv", help="CSV-файл для вывода")
    parser.add_argument("--headless", type=lambda x: x.lower() in ("1", "true", "yes"), default=True)
    parser.add_argument("--date", help="Одна дата в формате YYYY-MM-DD (например 2025-09-17)")
    parser.add_argument("--date-range", help="Диапазон дат в формате YYYY-MM-DD:YYYY-MM-DD (включительно)")
    args = parser.parse_args()

    crawler = AllPriborsCrawler(rn=args.rn, headless=args.headless, out=args.out,
                                date=args.date, date_range=args.date_range)
    try:
        crawler.crawl()
    except KeyboardInterrupt:
        logger.error("Остановка пользователем.")
    finally:
        crawler.close()


if __name__ == "__main__":
    main()
