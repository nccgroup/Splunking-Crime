import os


class EXPERIMENT_TYPES_LONG(object):
    # If this Enum is modified
    # modify `experiment/experiment_schema.json` accordingly
    pnf = "predict_numeric_fields"
    pcf = "predict_categorical_fields"
    dno = "detect_numeric_outliers"
    dco = "detect_categorical_outliers"
    fts = "forecast_time_series"
    cne = "cluster_numeric_events"


EXPERIMENT_TYPES_MAP = {
    EXPERIMENT_TYPES_LONG.pnf: "pnf",
    EXPERIMENT_TYPES_LONG.pcf: "pcf",
    EXPERIMENT_TYPES_LONG.dno: "dno",
    EXPERIMENT_TYPES_LONG.dco: "dco",
    EXPERIMENT_TYPES_LONG.fts: "fts",
    EXPERIMENT_TYPES_LONG.cne: "cne"
}

DEFAULT_LOOKUPS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "lookups")
CSV_FILESIZE_LIMIT = 2 ** 30
EXPERIMENT_MODEL_PREFIX = "_exp_"
EXPERMENT_DRAFT_MODEL_PREFIX = 'draft_'
MODEL_FILE_REGEX = r'.*__mlspl_(?P<model_name>[a-zA-Z_][a-zA-Z0-9_]*).csv'
