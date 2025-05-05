from enum import Enum

class PastaV9TestColumnNames(Enum):
    '''
    Based on the Pasta V9 dataset
    '''

    # Metrics
    SALES = "sales"
    ACV = "acv"
    SALES_SHARE = "sales_share"
    ACV_SHARE = "acv_share"

    # Dimensions
    BRAND = "brand"
    BASE_SIZE = "base_size"
    MANUFACTURER = "manufacturer"
    SUB_CATEGORY = "sub_category"

    # Some example dim values
    MANUFACTURER__PRIVATE_LABEL = "private label"
    SUB_CATEGORY__SEMOLINA = "semolina"

    # Time Granularities
    MONTH = "max_time_month"
    QUARTER = "max_time_quarter"
    YEAR = "max_time_year"
