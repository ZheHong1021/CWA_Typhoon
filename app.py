import datetime as dt
import requests
import json
import pymysql
import time


def connect_db(host, user, pwd, dbname, port):
    try:
        db = pymysql.connect(
            host = host,
            user = user,
            passwd = pwd,
            database = dbname,
            port = int(port)
        )
        # print("連線成功")
        return db
    except Exception as e:
        print('連線資料庫失敗: {}'.format(str(e)))
    return None

def getTyphoon(url, params):
    response = requests.get(url, params=params)
    status_code = response.status_code
    if(status_code == 200):
        data = json.loads(response.text)
        typhoons = data["records"]["tropicalCyclones"]["tropicalCyclone"]

        db = connect_db(
            host='127.0.0.1',
            user='root',
            pwd='Ru,6e.4vu4wj/3',
            dbname="greenhouse",
            port=3306
        ) # 資料庫連線

        if( not db ):
            print("資料庫連線發生問題")
        cursor = db.cursor()

        for typhoon in typhoons:
            year = typhoon["year"]
            # 颱風編號: !!注意!!因為熱帶性低氣壓時，這個No.是沒有資料的
            no = typhoon.get("cwaTyNo", None)

            # 熱帶性低氣壓編號
            TdNo = typhoon.get("cwaTdNo", None)
            print(no)
            print(TdNo)
            # no = typhoon["cwaTyNo"] if "cwaTyNo" in typhoon else None 
            # TdNo = typhoon["cwaTdNo"] if "cwaTdNo" in typhoon else None # 熱帶性低氣壓編號

            # 颱風名稱(在熱帶性低氣壓時並不會有颱風名稱，所以用【熱帶性低氣壓編號】代替)
            name = typhoon.get("cwaTyphoonName", None)
            if not name and TdNo:
                name = "TD" + TdNo 

            # 同上
            name_en = typhoon.get("typhoonName", None)
            if not name_en and TdNo:
                name_en = "TD" + TdNo 

            analysisData = typhoon["analysisData"]["fix"] # 颱風實際數據

            for data in analysisData:
                print(f"(!實際!){year}年【No{no}.颱風】 {name}({name_en})")
                fixTime = data["fixTime"] # 定位時間
                fixTime = dt.datetime.strftime(dt.datetime.fromisoformat(fixTime), "%Y-%m-%d %H:%M:%S") # 將API中得到的日期進行轉換
                coordinate = data["coordinate"] # 座標(List)
                coordinate = coordinate.split(",") # 將字串轉成陣列 ([0]: 經度 ； [1]: 緯度)
                pressure = data["pressure"] # 中心氣壓
                maxWindSpeed = data["maxWindSpeed"] if "maxWindSpeed" in data.keys() else None # 近中心最大風速(可能無資料)
                maxGustSpeed = data["maxGustSpeed"] if "maxGustSpeed" in data.keys() else None # 近中心最大陣風(可能無資料)
                movingSpeed = data["movingSpeed"] if "movingSpeed" in data.keys() else None # 預測移速(可能無資料)
                movingDirection = data["movingDirection"]  if "movingDirection" in data.keys() else None # 預測移向(可能無資料)
                circleOf15Ms = data["circleOf15Ms"]["radius"]  if "circleOf15Ms" in data.keys() else None # 七級風暴風半徑(可能無資料)
                remark = data["movingPrediction"][0]["value"] if len(data["movingPrediction"]) > 0 else None # 備註(可能無資料)
                print(f"定位時間: {fixTime} ； 經度: {coordinate[0]} ； 緯度: {coordinate[1]} ； 中心氣壓: {pressure} ； 近中心最大風速: {maxWindSpeed}(m/s) ； 近中心最大陣風: {maxGustSpeed}(m/s) ； 預測移速: {movingSpeed}(m/s) ； 預測移向: {movingDirection} ； 暴風半徑: {circleOf15Ms}(km) ；")
                
                # 找尋資料庫有無篩選條件下的資料: 1.『颱風編號』或『熱帶性低氣壓編號』一致； 2.紀錄年份相同； 3.定位時間相同
                sql = f"""SELECT * FROM typhoon WHERE ((`no` = '{no}') OR (`td_no` = '{TdNo}')) AND `year` = {year} AND `fix_Time` = '{fixTime}'"""
                cursor.execute(sql)
                result = cursor.fetchone() # 找不到時會回傳 "None"
                if(result): # 要將先前預測的數據進行更新(因為已經有證實結果了)
                    sql = """UPDATE `typhoon` SET 
                        `name` = %s, `name_en` = %s, `no` = %s, `td_no` = %s, 
                        `latitude` = %s, `longitude` = %s, `pressure` = %s, `maxWindSpeed` = %s, 
                        `maxGustSpeed` = %s, `movingSpeed` = %s, `movingDirection` = %s, `circle_Radius` = %s, 
                        `radiusPercentProbability` = %s, `is_forecast` = %s, `remark` = %s, `fix_Time` = %s, 
                        `year` = %s
                        WHERE ((`no` = %s) OR (`td_no` = %s)) AND (`year` = %s) AND (`fix_Time` = %s);"""
                    cursor.execute(sql, (
                        name, name_en, no, TdNo, 
                        coordinate[1], coordinate[0], pressure, maxWindSpeed, 
                        maxGustSpeed, movingSpeed, movingDirection, circleOf15Ms, 
                        None, '0', remark, fixTime, 
                        year, 
                        no, TdNo, year, fixTime
                    ))
                    db.commit()
                else: # 剛產生的數據需紀錄(第一次出現的資料)
                    sql = """insert into `typhoon` 
                            (`no`, `td_no`, `name`, `name_en`, 
                            `latitude`, `longitude`, `pressure`, `maxWindSpeed`, 
                            `maxGustSpeed`, `movingSpeed`, `movingDirection`, `circle_Radius`,
                             `is_forecast`, `remark`, `fix_Time`, `year`)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                        """
                    # is_forecast = 0 (代表已為實際數據；如為1代表為預測數據)
                    cursor.execute(sql, (
                        no, TdNo, name, name_en, 
                        coordinate[1], coordinate[0], pressure, maxWindSpeed, 
                        maxGustSpeed, movingSpeed, movingDirection, circleOf15Ms, 
                        '0', remark, fixTime, year
                    ))
                    db.commit()
                print("-----------------------------")

            forecastData = typhoon["forecastData"]["fix"] # 颱風預測數據
            for data in forecastData:
                print(f"(!預測!) {year}年【No{no}.颱風】 {name}({name_en})")
                fixTime = data["initTime"] # 初始定位時間
                fixTime = dt.datetime.fromisoformat(fixTime) # 將timezone調整一下。結果會變回 datetime
                fixTime = fixTime + dt.timedelta(hours = int(data["tau"])) # 因為API是給予『定位時間』跟『往後推的小時』，要透過加總來得到預測的定位時間
                fixTime = dt.datetime.strftime(fixTime, "%Y-%m-%d %H:%M:%S")
                coordinate = data["coordinate"] # 座標
                coordinate = coordinate.split(",") # 將字串轉成陣列 ([0]: 經度 ； [1]: 緯度)
                pressure = data["pressure"] # 中心氣壓
                maxWindSpeed = data["maxWindSpeed"] if "maxWindSpeed" in data.keys() else None # 近中心最大風速
                maxGustSpeed = data["maxGustSpeed"] if "maxGustSpeed" in data.keys() else None # 近中心最大陣風
                movingSpeed = data["movingSpeed"] if "movingSpeed" in data.keys() else None # 預測移速
                movingDirection = data["movingDirection"]  if "movingDirection" in data.keys() else None # 預測移向
                circleOf15Ms = data["circleOf15Ms"]["radius"]  if "circleOf15Ms" in data.keys() else None # 七級風暴風半徑
                radiusOf70PercentProbability = data["radiusOf70PercentProbability"] if "radiusOf70PercentProbability" in data.keys() else None # 熱帶氣旋中心侵襲機率圓	
                remark = data["stateTransfers"][0]["value"] if "stateTransfers" in data.keys() else None
                print(f"初始定位時間: {fixTime} ； 經度: {coordinate[0]} ； 緯度: {coordinate[1]} ； 中心氣壓: {pressure} ； 近中心最大風速: {maxWindSpeed}(m/s) ； 近中心最大陣風: {maxGustSpeed}(m/s) ； 預測移速: {movingSpeed}(m/s) ； 預測移向: '{movingDirection}' ； 暴風半徑: {circleOf15Ms}(km) ； 熱帶氣旋中心侵襲機率圓: {radiusOf70PercentProbability}(km)")

                # 找尋資料庫有無篩選條件下的資料: 1.『颱風編號』或『熱帶性低氣壓編號』一致； 2.紀錄年份相同； 3.定位時間相同； 4.數據為『預測數據』
                sql = f"""SELECT * FROM typhoon WHERE ((`no` = '{no}') OR (`td_no` = '{TdNo}')) AND `year` = {year} AND (`fix_Time` = '{fixTime}') AND (`is_forecast` = 1)"""
                cursor.execute(sql)
                result = cursor.fetchone() # 找不到時會回傳 "None"
                if(not result):
                    sql = """insert into `typhoon` (
                        `no`, `td_no`, `name`, `name_en`, 
                        `latitude`, `longitude`, `pressure`, `maxWindSpeed`, 
                        `maxGustSpeed`, `movingSpeed`, `movingDirection`, `circle_Radius`, 
                        `radiusPercentProbability`, `is_forecast`, `remark`, `fix_Time`, 
                        `year`
                    )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                    """
                    cursor.execute(sql, (
                        no, TdNo, name, name_en, 
                        coordinate[1], coordinate[0], pressure, maxWindSpeed, 
                        maxGustSpeed, movingSpeed, movingDirection, circleOf15Ms, 
                        radiusOf70PercentProbability, '1', remark, fixTime, 
                        year
                    ))
                    db.commit()
                else:
                    sql = """UPDATE `typhoon` SET 
                            `name` = %s, `name_en` = %s, `no` = %s, `td_no` = %s, 
                            `latitude` = %s, `longitude` = %s, `pressure` = %s, `maxWindSpeed` = %s, 
                            `maxGustSpeed` = %s, `movingSpeed` = %s, `movingDirection` = %s, `circle_Radius` = %s, 
                            `radiusPercentProbability` = %s, `is_forecast` = %s, `remark` = %s, `fix_Time` = %s, 
                            `year` = %s
                        WHERE (`fix_Time` = %s) AND ((`no` = %s) OR (`td_no` = %s));"""
                    cursor.execute(sql, (
                        name, name_en, no, TdNo, 
                        coordinate[1], coordinate[0], pressure, maxWindSpeed, 
                        maxGustSpeed, movingSpeed, movingDirection, circleOf15Ms, 
                        radiusOf70PercentProbability, '1', remark, fixTime, 
                        year, 
                        fixTime, no, TdNo
                    ))
                    db.commit()
                print("-----------------------------")
        
            # 因為在熱帶性低氣壓轉為輕度颱風時，原先預測數據可能有幾筆在輕度颱風時會不見，因此我們要將那幾筆給刪除。
            sql = f"DELETE FROM `typhoon` WHERE `td_no` = '{TdNo}' AND `name` != '{name}';"
            cursor.execute(sql)
            db.commit()
        db.close()


if __name__ == '__main__':
    Api_Code = "W-C0034-005" # 更新頻率: 6hr
    url = f"https://opendata.cwa.gov.tw/api/v1/rest/datastore/{Api_Code}"
    token = "CWB-3B4F6F2F-1E94-4A08-9FD7-EC2742EA45C9", # 會員 TOKEN

    params = { # Request時使用
        "Authorization": token,
    }

    try:
        getTyphoon(url, params)
    except Exception as e:
        print(f"發生不明錯誤: {e}")
    finally:
        print(f"程式執行結束，3秒後關閉...")
        time.sleep(3)
    

    