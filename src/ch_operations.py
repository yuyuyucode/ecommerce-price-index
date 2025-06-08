from clickhouse_driver import Client
from configparser import ConfigParser


class ClickHouseClient:
    def __init__(self):
        config = ConfigParser()
        config.read('config/settings.ini')

        self.client = Client(
            host=config['CLICKHOUSE']['HOST'],
            port=config.getint('CLICKHOUSE', 'PORT'),
            user=config['CLICKHOUSE']['USER'],
            password=config['CLICKHOUSE']['PASSWORD'],
            database=config['CLICKHOUSE']['DATABASE'],
            secure=config.getboolean('CLICKHOUSE', 'SECURE'),
        )

    def execute_query(self, query, params=None):
        try:
            return self.client.execute(query, params)
        except Exception as e:
            print(f"查询执行失败: {str(e)}")
            raise

    def create_tables(self):
        # 创建本地表
        self.execute_query("""
                           CREATE TABLE IF NOT EXISTS local_categories
                           (
                               category
                               String,
                               category_id
                               String,
                               hierarchy
                               Int32,
                               weight
                               Float64,
                               price
                               Nullable
                           (
                               Float64
                           ),
                               parent Nullable
                           (
                               String
                           )
                               ) ENGINE = MergeTree
                           (
                           )
                               ORDER BY category_id;
                           """)

        self.execute_query("""
                           CREATE TABLE IF NOT EXISTS local_products
                           (
                               product_id
                               String,
                               category_id
                               String,
                               name
                               String,
                               weight
                               Float64,
                               price 
                               Float64,
                               change_count
                               Int32
                           ) ENGINE = MergeTree
                           (
                           )
                               ORDER BY product_id;
                           """)

        self.execute_query("""
                           CREATE TABLE IF NOT EXISTS local_daily_prices
                           (
                               product_id
                               String,
                               category_id
                               String,
                               name
                               String,
                               price
                               Float64,
                               change_date
                               Date
                           ) ENGINE = MergeTree
                           (
                           )
                               ORDER BY
                           (
                               change_date,
                               category_id,
                               product_id
                           );
                           """)
    def sync_data(self):
        # 创建外部表（使用OSS引擎新语法）
        config = ConfigParser()
        config.read('config/settings.ini')
        '''
        self.execute_query(f"""
            CREATE TABLE IF NOT EXISTS oss_categories (
                `category` String,
                `category_id` String,
                `hierarchy` Int32,
                `weight` Float64,
                `price` Nullable(Float64),
                `parent` Nullable(String)
            ) ENGINE = OSS(
                'https://{config['OSS']['BUCKET_NAME']}.{config['OSS']['ENDPOINT']}/data/categories.csv',
                '{config['OSS']['ACCESS_KEY_ID']}',
                '{config['OSS']['ACCESS_KEY_SECRET']}',
                'CSVWithNames'
            )
            SETTINGS input_format_csv_encoding = 'GBK';
            
            """)
        '''

        # 步骤1：创建临时Memory表
        self.execute_query("""
                           CREATE
                           TEMPORARY TABLE temp_gbk (
                        category String,
                        category_id String,
                        hierarchy Int32,
                        weight Float64,
                        price String,  -- 先作为String接收
                        parent String
                    ) ENGINE = Memory
                           """)

        # 步骤2：直接导入GBK数据到临时表（不指定编码）
        self.execute_query(f"""
                    INSERT INTO temp_gbk
                    SELECT * FROM oss(
                        'https://{config['OSS']['BUCKET_NAME']}.{config['OSS']['ENDPOINT']}/data/categories.csv',
                        '{config['OSS']['ACCESS_KEY_ID']}',
                        '{config['OSS']['ACCESS_KEY_SECRET']}',
                        'CSVWithNames',
                        'category String, category_id String, hierarchy Int32, weight Float64, price String, parent String'
                    )
                    SETTINGS input_format_allow_errors_num = 10
                """)

        # 步骤3：转换后插入目标表
        self.execute_query("""
                           INSERT INTO local_categories
                           SELECT assumeNotNull(category) as category,
                                  category_id,
                                  hierarchy,
                                  weight,
                                  toFloat64OrNull(price)  as price,
                                  nullIf(parent, '')      as parent
                           FROM temp_gbk
                           """)

        self.execute_query(f"""
            CREATE TABLE IF NOT EXISTS oss_products (
                `product_id` String,
                `category_id` String,
                `name` String,
                `weight` Float64,
                `price` Float64,
                `change_count` Int32
            ) ENGINE = OSS(
                'https://{config['OSS']['BUCKET_NAME']}.{config['OSS']['ENDPOINT']}/data/products.csv',
                '{config['OSS']['ACCESS_KEY_ID']}',
                '{config['OSS']['ACCESS_KEY_SECRET']}',
                'CSVWithNames'
            );
            """)

        self.execute_query(f"""
            CREATE TABLE IF NOT EXISTS oss_daily_prices (
                `product_id` String,
                `category_id` String,
                `name` String,
                `price` Float64,
                `change_date` Date
            ) ENGINE = OSS(
                'https://{config['OSS']['BUCKET_NAME']}.{config['OSS']['ENDPOINT']}/data/daily_price/daily_prices_*.csv',
                '{config['OSS']['ACCESS_KEY_ID']}',
                '{config['OSS']['ACCESS_KEY_SECRET']}',
                'CSVWithNames'
            );
            """)

        self.execute_query(f"""
        INSERT
        INTO
        local_products
        SELECT * FROM
        oss_products;
        """)

        self.execute_query(f"""
        INSERT
        INTO
        local_daily_prices
        SELECT * FROM
        oss_daily_prices;
        """)