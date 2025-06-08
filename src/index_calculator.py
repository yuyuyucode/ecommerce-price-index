from datetime import datetime
import logging
from typing import Tuple, List


class SimplePriceIndexCalculator:
    """简化版价格指数计算器"""

    def __init__(self, ch_client):
        self.client = ch_client
        self.base_date = None
        self.base_price = None
        self._setup_logging()

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )

    def set_base_period(self, base_date: str):
        """设置基期（简化版只计算总体指数）"""
        self.base_date = base_date
        query = """
                SELECT AVG(price)
                FROM local_daily_prices
                WHERE change_date = %(base_date)s
                  AND price > 0 \
                """
        result = self.client.execute_query(query, {'base_date': base_date})
        if not result or not result[0][0]:
            raise ValueError("无法计算基期价格，请检查数据")
        self.base_price = float(result[0][0])
        logging.info(f"基期({base_date})平均价格: {self.base_price:.2f}")

    def calculate_daily_index(self) -> List[Tuple]:
        """计算每日综合价格指数（简化版）"""
        if not self.base_price:
            raise ValueError("请先设置基期")

        query = """
                SELECT change_date, AVG(price) as avg_price
                FROM local_daily_prices
                WHERE price > 0
                  AND change_date BETWEEN %(start_date)s AND %(end_date)s
                GROUP BY change_date
                ORDER BY change_date \
                """

        params = {
            'start_date': self.base_date,
            'end_date': '2028-05-15'
        }

        results = self.client.execute_query(query, params)
        indices = []

        for date, avg_price in results:
            index = (avg_price / self.base_price) * 100
            indices.append((date, index))

        logging.info(f"计算了 {len(indices)} 天的价格指数")
        return indices