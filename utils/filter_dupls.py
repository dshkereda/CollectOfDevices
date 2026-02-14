from typing import List, Dict, Any
from collections import defaultdict


def cleanup_incomplete_pages(rows: List[Dict[str, Any]], expected_per_page: int = 20) -> List[Dict[str, Any]]:
    """
    Удаляет записи страниц (< max_page), если на странице меньше expected_per_page записей.
    Самая большая page не проверяется (может быть неполной).

    :param rows: список записей CSV
    :param expected_per_page: ожидаемое количество записей на страницу
    :return: очищенный список записей
    """

    if not rows:
        return rows

    # группировка по page
    pages = defaultdict(list)
    for row in rows:
        page = row.get("page")
        if page is not None:
            try:
                page = int(page)
                pages[page].append(row)
            except ValueError:
                continue

    if not pages:
        return rows

    max_page = max(pages.keys())

    cleaned_rows = []

    for page_num, page_rows in pages.items():
        # если страница меньше max_page и неполная — пропускаем её
        if page_num < max_page and len(page_rows) < expected_per_page:
            continue

        cleaned_rows.extend(page_rows)

    return cleaned_rows
