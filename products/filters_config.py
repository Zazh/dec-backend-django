# products/filters_config.py

FILTER_CONFIG = {
    "moldingi": {
        "price": [
            {"label": "Все", "value": ""},
            {"label": "До 5000₸", "value": "0-5000"},
            {"label": "5000 - 10000₸", "value": "5000-10000"},
            {"label": "10000 - 15000₸", "value": "10000-15000"},
        ],
        "length": [
            {"label": "Все", "value": ""},
            {"label": "До 1 м", "value": "0-1000"},
            {"label": "1-2 м", "value": "1000-2000"},
            {"label": "От 2 м", "value": "2000-"},
        ],
        "width": [
            {"label": "Все", "value": ""},
            {"label": "до 20 мм", "value": "0-20"},
            {"label": "20-40 мм", "value": "20-40"},
            {"label": "40+ мм", "value": "40-"},
        ],
        "height": [
            {"label": "Все", "value": ""},
            {"label": "до 20 мм", "value": "0-20"},
            {"label": "20-50 мм", "value": "20-50"},
            {"label": "50+ мм", "value": "50-"},
        ],
        "attributes": []
    },
    "reiki": {
        "price": [
            {"label": "Все", "value": ""},
            {"label": "До 3000₸", "value": "0-3000"},
            {"label": "От 3000 до 7000₸", "value": "3000-7000"},
            {"label": "От 7000 до 10000₸", "value": "7000-10000"},
        ],
        "length": [
            {"label": "Все", "value": ""},
            {"label": "До 3 м", "value": "0-3000"},
            {"label": "3-7 м", "value": "3000-7000"},
            {"label": "От 7 м", "value": "7000-"},
        ],
        "width": [
            {"label": "Все", "value": ""},
            {"label": "до 20 мм", "value": "0-20"},
            {"label": "20-40 мм", "value": "20-40"},
            {"label": "40+ мм", "value": "40-"},
        ],
        "height": [
            {"label": "Все", "value": ""},
            {"label": "до 20 мм", "value": "0-20"},
            {"label": "20-50 мм", "value": "20-50"},
            {"label": "50+ мм", "value": "50-"},
        ],
        "attributes": []
    },
    "plintusy": {
        "price": [
            {"label": "Все", "value": ""},
            {"label": "до 2000₸", "value": "0-2000"},
            {"label": "2000-5000₸", "value": "2000-5000"},
            {"label": "5000+₸", "value": "5000-"},
        ],
        "length": [
            {"label": "Все", "value": ""},
            {"label": "до 2500 мм", "value": "0-2500"},
            {"label": "2500-4000 мм", "value": "2500-4000"},
            {"label": "4000+ мм", "value": "4000-"},
        ],
        "width": [
            {"label": "Все", "value": ""},
            {"label": "до 20 мм", "value": "0-20"},
            {"label": "20-40 мм", "value": "20-40"},
            {"label": "40+ мм", "value": "40-"},
        ],
        "height": [
            {"label": "Все", "value": ""},
            {"label": "до 20 мм", "value": "0-20"},
            {"label": "20-50 мм", "value": "20-50"},
            {"label": "50+ мм", "value": "50-"},
        ],
        "attributes": ["вид", "подсветка"]
    },
    "default": {
        "price": [
            {"label": "Все", "value": ""},
            {"label": "до 1000₸", "value": "0-2000"},
            {"label": "2000-5000₸", "value": "2000-5000"},
            {"label": "5000+₸", "value": "5000-"},
        ],
    }
}