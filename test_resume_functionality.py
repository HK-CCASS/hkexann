import asyncio
import yaml
from services.monitor.data_flow.corrected_historical_processor import CorrectedHistoricalProcessor

async def test_resume_callback():
    """测试断点续传回调功能"""

    # 加载配置
    with open('config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    # 创建模拟的进度回调函数
    processed_stocks_log = []

    async def mock_progress_callback(progress_data):
        processed_stocks = progress_data.get('processed_stocks', [])
        processed_stocks_log.append(processed_stocks.copy())
        print(f"📊 进度回调: 已处理 {len(processed_stocks)} 只股票: {processed_stocks[-5:] if processed_stocks else []}")

    # 创建处理器（使用模拟参数）
    processor = CorrectedHistoricalProcessor(
        hkex_downloader=None,  # 不会真正调用
        dual_filter=None,
        vectorizer=None,
        monitored_stocks={'00001', '00002', '00003'},
        config=config,
        progress_callback=mock_progress_callback
    )

    # 模拟处理过程
    print("🔄 模拟断点续传回调功能:")
    print("=" * 50)

    # 模拟批次处理
    batches = [['00001'], ['00002'], ['00003']]

    for i, batch in enumerate(batches, 1):
        print(f"处理批次 {i}: {batch}")

        # 模拟调用进度回调
        if processor.progress_callback:
            progress_data = {
                'processed_stocks': [stock for b in batches[:i] for stock in b],
                'stats': {'test': True}
            }
            await processor.progress_callback(progress_data)

    print(f"\n✅ 回调测试完成，共收到 {len(processed_stocks_log)} 次进度更新")
    for i, stocks in enumerate(processed_stocks_log):
        print(f"  更新 {i+1}: {len(stocks)} 只股票")

if __name__ == "__main__":
    asyncio.run(test_resume_callback())
