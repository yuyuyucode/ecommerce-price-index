from datetime import datetime
from src.ch_operations import ClickHouseClient
from src.index_calculator import SimplePriceIndexCalculator
from src.visualization import plot_simple_index
import logging


def main():
    logging.basicConfig(level=logging.INFO)

    try:
        # 1. 初始化
        logging.info("Initializing ClickHouse client...")
        client = ClickHouseClient()
        logging.info("Creating tables...")
        client.create_tables()
        logging.info("Syncing data from OSS to local tables...")
        client.sync_data()
        # 2. 计算指数
        calculator = SimplePriceIndexCalculator(client)
        calculator.set_base_period("2025-05-17")  # 固定基期

        indices = calculator.calculate_daily_index()

        # 3. 简单可视化
        output_dir = f"simple_results_{datetime.now().strftime('%Y%m%d')}"
        plot_simple_index(indices, output_dir)

    except Exception as e:
        logging.error(f"处理失败: {str(e)}")
    finally:
        logging.info("程序结束")


if __name__ == '__main__':
    main()