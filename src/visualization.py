import matplotlib.pyplot as plt
import pandas as pd
import os
from typing import List, Tuple


def plot_simple_index(index_data: List[Tuple], save_dir: str = 'simple_results'):
    """
    绘制简化版价格指数趋势图
    :param index_data: [(date, index), ...]
    :param save_dir: 输出目录
    """
    os.makedirs(save_dir, exist_ok=True)

    # 转换为DataFrame
    df = pd.DataFrame(index_data, columns=['date', 'index'])
    df['date'] = pd.to_datetime(df['date'])

    # 简单折线图
    plt.figure(figsize=(10, 5))
    plt.plot(df['date'], df['index'], 'b-', linewidth=1.5)
    plt.axhline(y=100, color='r', linestyle='--', linewidth=1)

    plt.title('Simplified Price Index Trend')
    plt.xlabel('Date')
    plt.ylabel('Index (Base=100)')
    plt.grid(True, linestyle='--', alpha=0.6)

    # 自动调整日期显示
    plt.gcf().autofmt_xdate()

    # 保存结果
    plt.savefig(f'{save_dir}/simple_index.png', dpi=150, bbox_inches='tight')
    df.to_csv(f'{save_dir}/simple_index.csv', index=False)
    plt.close()
    print(f"结果已保存到: {save_dir}")