### Что это:
- Пакет в котором реализован парсинг Jooble Url на компоненты, которые используются 
  в https://gitlab.jooble.com/data-retrieval/seo_url_warehouse/

### Возможные типы урлов, которые определяются данным пакетом:
```
1: 'serp',
2: 'region_main',
3: 'comp',
4: 'cdp',
5: 'jdp',
6: 'desc',
7: 'away',
8: 'with_param',
9: 'other'
```

### Пример использования:
```
from url_parser import UrlParser


url_list_for_test = [
    "https://jooble.org/jobs-driver",
    "https://jooble.org/jobs-driver?cmp",
    "https://jooble.org/jobs-driver/kiev",
    "https://jooble.org/jdp/32313131",
    "https://uk.jooble.org/jobs-driver",
    "https://uk.jooble.org/company/231312323",
    "https://uk.jooble.org/jobs-driver/kiev"
]

parser_obj = UrlParser(country="uk")
for url in url_list_for_test:
    res = parser_obj.parse(url)
    if res['type_id'] != 1:
        print(f"[{url}] is not serp!")
    else:
        print(f"[{url}] -> [{res['keyword']}] + [{res['region']}]")
```