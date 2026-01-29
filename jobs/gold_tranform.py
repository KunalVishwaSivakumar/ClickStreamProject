from datetime import datetime
import sys
from pathlib import Path

project_root  = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0,str(project_root))

from pyspark.sql.functions import col, lit
from utils.spark_session import get_spark

def main():
    spark = get_spark("gold_clickstream_tranform")
    #Define Path
    silver_path = str(project_root / "data" / "silver")
    gold_path = str(project_root / "data" / "gold")

#yet to decide on the Olap strategy