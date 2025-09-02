import sys
import os

scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(scripts_path)

from etl.postgre import (
    insert_into_postgres
)

