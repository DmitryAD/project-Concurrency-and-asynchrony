"""
Точка входа для «python -m crawler».

Python выполняет этот файл, когда пакет запускают через -m.
Вся логика — в crawler/cli.py, здесь только вызов.
"""

import sys

from crawler.cli import main

sys.exit(main())