import requests
import pandas as pd
from datetime import datetime, timedelta
import telegram
import asyncio
import aiohttp


bot_token = ""
chat_id = ""

bot = telegram.Bot(token=bot_token)

url = "https://api.etherscan.io/api"
contract_address = "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"
address = "0x97de57eC338AB5d51557DA3434828C5DbFaDA371"
api_key = ""


async def get_data():
    async with aiohttp.ClientSession() as session:
        df = pd.DataFrame()

        for page in range(1, 7):
            params = {
                "module": "account",
                "action": "tokentx",
                "contractaddress": contract_address,
                "address": address,
                "page": page,
                "offset": 100,
                "startblock": 0,
                "endblock": 99999999,
                "sort": "desc",
                "apikey": api_key
            }

            async with session.get(url, params=params) as response:
                data = await response.json()

                if 'result' not in data or not data['result']:
                    print("API 결과 없음")
                    break

                result = data['result']

                if not result:
                    print("결과 없음")
                    break

                page_df = pd.DataFrame(result)
                df = pd.concat([df, page_df], ignore_index=True)


        df['datetime'] = pd.to_datetime(df['timeStamp'].astype(float), unit='s', origin='unix')
        df['datetime'] += pd.Timedelta(hours=9)


        if df['value'].dtype == object:
            df['value'] = df['value'].astype(float) / 10 ** 18


        columns_order = ['datetime', 'value', 'from', 'to']
        df = df.reindex(columns=columns_order)


        df['port'] = ''
        df.loc[df['to'] == '0x97de57ec338ab5d51557da3434828c5dbfada371', 'port'] = 'in'
        df.loc[df['from'] == '0x97de57ec338ab5d51557da3434828c5dbfada371', 'port'] = 'out'

        return df


async def send_telegram_message(df):

    current_time = datetime.now()
    start_time_30min = current_time - timedelta(minutes=60)
    start_time_24h = current_time - timedelta(hours=24)
    end_time = current_time


    filtered_df_30min = df[(df['datetime'] >= start_time_30min) & (df['datetime'] <= end_time)]
    filtered_df_24h = df[(df['datetime'] >= start_time_24h) & (df['datetime'] <= end_time)]


    in_value_sum_30min = filtered_df_30min.loc[filtered_df_30min['port'] == 'in', 'value'].sum()
    out_value_sum_30min = filtered_df_30min.loc[filtered_df_30min['port'] == 'out', 'value'].sum()
    in_value_sum_24h = filtered_df_24h.loc[filtered_df_24h['port'] == 'in', 'value'].sum()
    out_value_sum_24h = filtered_df_24h.loc[filtered_df_24h['port'] == 'out', 'value'].sum()


    in_value_sum_30min = round(in_value_sum_30min, 2)
    out_value_sum_30min = round(out_value_sum_30min, 2)
    in_value_sum_24h = round(in_value_sum_24h, 2)
    out_value_sum_24h = round(out_value_sum_24h, 2)


    message = "===Lybra Finance Tracker===\n"
    message += "60분 전부터 현재까지\n"
    message += "FROM: {}\n".format(start_time_30min.strftime("%Y-%m-%d %H:%M:%S"))
    message += "TO: {}\n".format(end_time.strftime("%Y-%m-%d %H:%M:%S"))
    message += "IN: {} stETH\n".format(in_value_sum_30min)
    message += "OUT: {} stETH\n".format(out_value_sum_30min)
    message += "NET: {:.2f} stETH\n\n".format(round(in_value_sum_30min - out_value_sum_30min, 2))
    message += "24시간 전부터 현재까지\n"
    message += "FROM: {}\n".format(start_time_24h.strftime("%Y-%m-%d %H:%M:%S"))
    message += "TO: {}\n".format(end_time.strftime("%Y-%m-%d %H:%M:%S"))
    message += "IN: {} stETH\n".format(in_value_sum_24h)
    message += "OUT: {} stETH\n".format(out_value_sum_24h)
    message += "NET: {:.2f} stETH\n".format(round(in_value_sum_24h - out_value_sum_24h, 2))

    await bot.send_message(chat_id=chat_id, text=message)


async def run_code():
    while True:
        try:
            current_time = datetime.now()

            if current_time.minute == 0:

                df = await get_data()
                await send_telegram_message(df)


            await asyncio.sleep(60)

        except Exception as e:

            print("Error:", str(e))



loop = asyncio.get_event_loop()
loop.run_until_complete(run_code())
